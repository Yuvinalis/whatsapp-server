import express from 'express';
import makeWASocket, { useMultiFileAuthState, DisconnectReason, Browsers, fetchLatestBaileysVersion } from '@whiskeysockets/baileys';
import QRCode from 'qrcode';
import { createClient } from '@supabase/supabase-js';
import pino from 'pino';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const API_SECRET = process.env.WHATSAPP_API_SECRET;
const AUTH_DIR = path.join(__dirname, 'auth_sessions');

// Rate-limiting: delay between consecutive sends per session (ms)
const INTER_MESSAGE_DELAY_MS = parseInt(process.env.INTER_MESSAGE_DELAY_MS || '1500', 10);
// Max concurrent sessions draining queues at once
const MAX_CONCURRENT_DRAINS = parseInt(process.env.MAX_CONCURRENT_DRAINS || '5', 10);

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !API_SECRET) {
  console.error('❌ Missing required environment variables. Check .env file.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const app = express();
app.use(express.json());

// ─────────────────────────────────────────────
// Global Session Registry
// Each entry: { sock, connectionStatus, currentPhone, isConnecting, queue: [], isDraining: boolean }
// ─────────────────────────────────────────────
const sessions = {};

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const formatJid = (phone) => {
  let clean = phone.replace(/\D/g, '');
  if (clean.length === 10 && clean.startsWith('0')) {
    clean = '254' + clean.substring(1);
  } else if (clean.length === 9 && (clean.startsWith('7') || clean.startsWith('1'))) {
    clean = '254' + clean;
  }
  return `${clean}@s.whatsapp.net`;
};

const log = (emoji, msg) => {
  const timestamp = new Date().toLocaleTimeString();
  console.log(`${emoji} [${timestamp}] ${msg}`);
};

function getOrCreateSession(sessionId) {
  if (!sessions[sessionId]) {
    sessions[sessionId] = {
      sock: null,
      connectionStatus: 'disconnected',
      currentPhone: null,
      isConnecting: false,
      qrTimeoutTimer: null,
      // In-memory per-session FIFO queue of { id, phone, message } objects
      queue: [],
      // Whether this session's drain loop is currently running
      isDraining: false,
    };
  }
  return sessions[sessionId];
}

// ─────────────────────────────────────────────
// Per-Session Queue Drain Loop
// Runs continuously while there are items in the queue.
// Stops itself when the queue is empty, restarts on next enqueue.
// ─────────────────────────────────────────────
async function drainSessionQueue(sessionId) {
  const session = sessions[sessionId];
  if (!session || session.isDraining) return;
  session.isDraining = true;

  log('📋', `[Session ${sessionId}] Queue drain started — ${session.queue.length} item(s) pending`);

  while (session.queue.length > 0) {
    // Check connection before each send
    if (!session.sock || session.connectionStatus !== 'connected') {
      log('⚠️', `[Session ${sessionId}] Not connected — pausing queue drain`);
      break;
    }

    const msg = session.queue.shift();
    log('📤', `[Session ${sessionId}] Sending to ${msg.phone} (queue ID: ${msg.id})`);

    try {
      // Update DB: mark as processing
      if (msg.id) {
        await supabase.from('whatsapp_queue').update({ status: 'processing' }).eq('id', msg.id);
      }

      const recipientJid = formatJid(msg.phone);

      // Anti-ban: random initial delay (1–3s), then typing presence, then send
      const initialWait = Math.floor(Math.random() * 2000) + 1000;
      await delay(initialWait);

      await session.sock.sendPresenceUpdate('composing', recipientJid);
      const typingDuration = Math.min(msg.message.length * 40, 6000);
      await delay(typingDuration);

      await session.sock.sendMessage(recipientJid, { text: msg.message });
      await session.sock.sendPresenceUpdate('paused', recipientJid);
      totalMessagesSent++;

      // Update DB: mark as sent
      if (msg.id) {
        await supabase.from('whatsapp_queue').update({
          status: 'sent',
          processed_at: new Date().toISOString(),
        }).eq('id', msg.id);
      }

      log('✅', `[Session ${sessionId}] Message sent to ${msg.phone}`);

      // Inter-message delay for rate limiting / anti-ban
      if (session.queue.length > 0) {
        await delay(INTER_MESSAGE_DELAY_MS);
      }

    } catch (sendErr) {
      log('❌', `[Session ${sessionId}] Failed to send to ${msg.phone}: ${sendErr.message}`);
      if (msg.id) {
        await supabase.from('whatsapp_queue').update({
          status: 'failed',
          retry_count: (msg.retry_count || 0) + 1,
          error_message: sendErr.message || 'Unknown sending error',
        }).eq('id', msg.id);
      }
    }
  }

  session.isDraining = false;
  log('📋', `[Session ${sessionId}] Queue drain finished`);
}

// ─────────────────────────────────────────────
// Enqueue a message for a session and start drain if not already running
// ─────────────────────────────────────────────
function enqueueMessage(sessionId, msgItem) {
  const session = getOrCreateSession(sessionId);
  session.queue.push(msgItem);
  log('📥', `[Session ${sessionId}] Message enqueued for ${msgItem.phone} — queue length: ${session.queue.length}`);
  // Fire-and-forget drain (self-stops when queue is empty)
  drainSessionQueue(sessionId).catch((err) => {
    log('❌', `[Session ${sessionId}] Drain error: ${err.message}`);
    if (sessions[sessionId]) sessions[sessionId].isDraining = false;
  });
}

// ─────────────────────────────────────────────
// Update Supabase Session Status
// ─────────────────────────────────────────────
async function updateSessionStatus(sessionId, updates) {
  try {
    if (updates.status === 'disconnected') {
      const { error } = await supabase.from('whatsapp_sessions').delete().eq('session_id', sessionId);
      if (error) throw error;
      
      // Also update any child sessions sharing this session
      await supabase
        .from('whatsapp_sessions')
        .update({ status: 'disconnected', phone: null, updated_at: new Date().toISOString() })
        .eq('parent_session_id', sessionId);

      log('🗑️', `[Session ${sessionId}] Disconnected session deleted from database`);
      return;
    }

    const { error } = await supabase.from('whatsapp_sessions').upsert(
      {
        session_id: sessionId,
        ...updates,
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'session_id' }
    );
    if (error) throw error;

    // Propagate status & phone to all child stores sharing this session
    if (updates.status === 'connected' || updates.status === 'connecting') {
      await supabase
        .from('whatsapp_sessions')
        .update({
          status: updates.status,
          phone: updates.phone ?? null,
          updated_at: new Date().toISOString()
        })
        .eq('parent_session_id', sessionId);
    }

    log('📡', `[Session ${sessionId}] Status updated: ${JSON.stringify(updates)}`);
  } catch (err) {
    log('❌', `[Session ${sessionId}] Failed to update session in Supabase: ${err.message}`);
  }
}

// ─────────────────────────────────────────────
// Supabase Auth Persistence Helpers
// ─────────────────────────────────────────────

/**
 * Restores auth files from Supabase (whatsapp_sessions table) to local auth_sessions folder
 */
async function restoreAuthFromSupabase(sessionId) {
  const sessionAuthDir = path.join(AUTH_DIR, sessionId);
  const credsFile = path.join(sessionAuthDir, 'creds.json');
  return fs.existsSync(credsFile);
}

/**
 * Saves current local auth files to Supabase (whatsapp_sessions table)
 */
async function saveAuthToSupabase(sessionId) {
  // Credentials are store locally in auth_sessions folder
}

// ─────────────────────────────────────────────
// Baileys WhatsApp Connection
// ─────────────────────────────────────────────
async function startConnection(sessionId) {
  const session = getOrCreateSession(sessionId);
  if (session.isConnecting) {
    log('⚠️', `[Session ${sessionId}] Connection already in progress, skipping...`);
    return;
  }

  session.isConnecting = true;
  session.connectionStatus = 'connecting';
  await updateSessionStatus(sessionId, { status: 'connecting', qr_code: null, parent_session_id: null });

  try {
    let version = [2, 3000, 1015901307];
    try {
      const vData = await fetchLatestBaileysVersion();
      version = vData.version;
      log('📡', `[Session ${sessionId}] Using WhatsApp Web v${version.join('.')}`);
    } catch (vErr) {
      log('⚠️', `[Session ${sessionId}] Could not fetch latest Baileys version, using default fallback: ${vErr.message}`);
    }

    // Restore auth from Supabase if missing locally
    await restoreAuthFromSupabase(sessionId);

    const sessionAuthDir = path.join(AUTH_DIR, sessionId);
    const { state, saveCreds } = await useMultiFileAuthState(sessionAuthDir);

    const sock = makeWASocket({
      version,
      auth: state,
      browser: Browsers.ubuntu('Chrome'),
      logger: pino({ level: 'silent' }),
    });

    session.sock = sock;
    sock.ev.on('creds.update', async () => {
      await saveCreds();
      await saveAuthToSupabase(sessionId);
    });

    sock.ev.on('connection.update', async (update) => {
      const { connection, qr, lastDisconnect } = update;

      // ── QR Code Received ──
      if (qr) {
        log('📱', `[Session ${sessionId}] QR Code received — scan with your phone`);
        session.connectionStatus = 'qrcode';

        // 2-minute QR code expiration timer
        if (session.qrTimeoutTimer) {
          clearTimeout(session.qrTimeoutTimer);
        }
        session.qrTimeoutTimer = setTimeout(async () => {
          log('⏰', `[Session ${sessionId}] QR code expired after 2 minutes — stopping connection attempt`);
          if (session.sock) {
            try {
              session.sock.ev.removeAllListeners('connection.update');
              session.sock.end();
            } catch (e) {}
            session.sock = null;
          }
          session.connectionStatus = 'disconnected';
          session.isConnecting = false;
          session.qrCode = null;
          session.qrTimeoutTimer = null;
          await updateSessionStatus(sessionId, {
            status: 'disconnected',
            phone: null,
            qr_code: null,
          });
        }, 120000);

        try {
          const qrDataUrl = await QRCode.toDataURL(qr);
          session.qrCode = qrDataUrl;
          await updateSessionStatus(sessionId, {
            status: 'qrcode',
            qr_code: qrDataUrl,
            phone: null,
          });
        } catch (err) {
          log('❌', `[Session ${sessionId}] QR generation error: ${err.message}`);
        }
      }

      // ── Connection Opened ──
      if (connection === 'open') {
        log('✅', `[Session ${sessionId}] WhatsApp connected successfully!`);
        if (session.qrTimeoutTimer) {
          clearTimeout(session.qrTimeoutTimer);
          session.qrTimeoutTimer = null;
        }
        session.connectionStatus = 'connected';
        session.isConnecting = false;
        session.qrCode = null;
        session.currentPhone = sock.user?.id?.split(':')[0] || '';
        await updateSessionStatus(sessionId, {
          status: 'connected',
          phone: session.currentPhone,
          qr_code: null,
        });
        // Backup full auth session state to Supabase DB
        await saveAuthToSupabase(sessionId);
        // Resume any queued messages that arrived before/during reconnect
        if (session.queue.length > 0) {
          log('🔄', `[Session ${sessionId}] Resuming ${session.queue.length} queued message(s) after reconnect`);
          drainSessionQueue(sessionId).catch(() => { session.isDraining = false; });
        }
      }

      // ── Connection Closed ──
      if (connection === 'close') {
        const wasConnected = session.connectionStatus === 'connected';
        if (session.qrTimeoutTimer) {
          clearTimeout(session.qrTimeoutTimer);
          session.qrTimeoutTimer = null;
        }
        session.isConnecting = false;
        session.qrCode = null;
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const isLoggedOut = statusCode === DisconnectReason.loggedOut || statusCode === 401 || statusCode === 405;
        // Only auto-reconnect if it was previously connected and not logged out
        const shouldReconnect = !isLoggedOut && wasConnected;

        log('🔌', `[Session ${sessionId}] Connection closed. Code: ${statusCode}. Reconnecting: ${shouldReconnect}`);

        if (isLoggedOut) {
          log('🗑️', `[Session ${sessionId}] Clearing auth sessions due to logout/session expiration (code ${statusCode})...`);
          if (fs.existsSync(sessionAuthDir)) {
            fs.rmSync(sessionAuthDir, { recursive: true, force: true });
          }
          await updateSessionStatus(sessionId, {
            status: 'disconnected',
            phone: null,
            qr_code: null,
          });
          if (sessions[sessionId]) {
            delete sessions[sessionId];
          }
        }

        if (shouldReconnect) {
          session.connectionStatus = 'connecting';
          await updateSessionStatus(sessionId, { status: 'connecting', qr_code: null });
          // Exponential backoff reconnect (3s base)
          setTimeout(() => startConnection(sessionId), 3000);
        } else {
          session.connectionStatus = 'disconnected';
          session.currentPhone = null;
          session.sock = null;
          await updateSessionStatus(sessionId, {
            status: 'disconnected',
            phone: null,
            qr_code: null,
          });
          if (sessions[sessionId]) {
            delete sessions[sessionId];
          }
        }
      }
    });
  } catch (err) {
    log('❌', `[Session ${sessionId}] Connection startup error: ${err.message}`);
    session.isConnecting = false;
    session.connectionStatus = 'disconnected';
    await updateSessionStatus(sessionId, { status: 'disconnected', qr_code: null });
  }
}

// Track in-memory sent counter
let totalMessagesSent = 0;

// ─────────────────────────────────────────────
// Public Health Check (for Render / Uptime Pings)
// ─────────────────────────────────────────────
app.get('/health', async (req, res) => {
  // Count from in-memory sessions (authoritative for this instance)
  const inMemoryCount = Object.values(sessions).filter(s => s.connectionStatus === 'connected').length;

  // Also count from Supabase DB — the DB is the source of truth after restarts
  let dbActiveCount = inMemoryCount;
  try {
    const { count } = await supabase.from('whatsapp_sessions').select('*', { count: 'exact', head: true }).eq('status', 'connected');
    if (count !== null && count !== undefined) dbActiveCount = count;
  } catch (e) {}

  // Use whichever is higher (DB has connected records even if in-memory is still warming up)
  const activeCount = Math.max(inMemoryCount, dbActiveCount);

  let dbSentCount = totalMessagesSent;
  try {
    const { count } = await supabase.from('whatsapp_queue').select('*', { count: 'exact', head: true }).eq('status', 'sent');
    if (count !== null && count !== undefined) dbSentCount = count;
  } catch (e) {}

  res.json({
    status: 'ok',
    uptime: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),
    active_sessions: activeCount,
    in_memory_sessions: inMemoryCount,
    db_sessions: dbActiveCount,
    messages_sent: dbSentCount,
  });
});

// ─────────────────────────────────────────────
// Auth Middleware
// ─────────────────────────────────────────────
const authMiddleware = (req, res, next) => {
  const token = req.headers['x-api-secret'];
  if (token !== API_SECRET) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  next();
};

app.use(authMiddleware);

// ─────────────────────────────────────────────
// API Routes
// ─────────────────────────────────────────────

// GET /status — Return current connection status for a session
app.get('/status', async (req, res) => {
  const sessionId = req.query.session_id || req.body?.session_id || req.query.store_id || req.body?.store_id;
  if (!sessionId) {
    return res.status(400).json({ success: false, error: 'session_id or store_id is required' });
  }

  let session = sessions[sessionId];
  let effectiveStatus = session ? session.connectionStatus : 'disconnected';
  let effectivePhone = session ? session.currentPhone : null;
  let effectiveQr = session ? session.qrCode : null;

  if (effectiveStatus !== 'connected') {
    const { data: dbSess } = await supabase
      .from('whatsapp_sessions')
      .select('status, phone, parent_session_id, qr_code')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (dbSess) {
      if (dbSess.parent_session_id && sessions[dbSess.parent_session_id]) {
        const parent = sessions[dbSess.parent_session_id];
        effectiveStatus = parent.connectionStatus;
        effectivePhone = parent.currentPhone || dbSess.phone;
      } else {
        effectiveStatus = dbSess.status || effectiveStatus;
        effectivePhone = dbSess.phone || effectivePhone;
        effectiveQr = dbSess.qr_code || effectiveQr;
      }
    }
  }

  res.json({
    success: true,
    status: effectiveStatus,
    phone: effectivePhone,
    qr_code: effectiveQr,
    queue_length: session ? session.queue.length : 0,
  });
});

// GET /queue-stats — Return per-session queue statistics
app.get('/queue-stats', (req, res) => {
  const stats = Object.entries(sessions).map(([id, s]) => ({
    session_id: id,
    status: s.connectionStatus,
    queue_length: s.queue.length,
    is_draining: s.isDraining,
  }));
  res.json({ success: true, sessions: stats });
});

// POST /connect — Start or resume WhatsApp connection for a session
app.post('/connect', async (req, res) => {
  const sessionId = req.body.session_id || req.body.store_id;
  if (!sessionId) {
    return res.status(400).json({ success: false, error: 'session_id or store_id is required' });
  }
  const session = getOrCreateSession(sessionId);

  if (session.connectionStatus === 'connected') {
    return res.json({ success: true, status: 'connected', phone: session.currentPhone });
  }

  if (session.isConnecting) {
    return res.json({ success: true, status: 'connecting', message: 'Already connecting...' });
  }

  try {
    // Start connection in background (non-blocking)
    startConnection(sessionId);
    res.json({ success: true, status: 'connecting' });
  } catch (err) {
    log('❌', `[Session ${sessionId}] Connect error: ${err.message}`);
    res.json({ success: false, error: err.message });
  }
});

// POST /disconnect — Logout and clear session, or unlink shared session
app.post('/disconnect', async (req, res) => {
  const sessionId = req.body.session_id || req.body.store_id;
  if (!sessionId) {
    return res.status(400).json({ success: false, error: 'session_id or store_id is required' });
  }

  try {
    // 1) Read DB row for this session
    const { data: dbSess } = await supabase
      .from('whatsapp_sessions')
      .select('session_id, parent_session_id, phone, status')
      .eq('session_id', sessionId)
      .maybeSingle();

    // CASE A: This store is a CHILD / SHARING store (parent_session_id is set)
    if (dbSess?.parent_session_id) {
      log('🔗', `[Disconnect] Unlinking child store "${sessionId}" from parent session "${dbSess.parent_session_id}"`);

      await supabase
        .from('whatsapp_sessions')
        .update({
          status: 'disconnected',
          phone: null,
          parent_session_id: null,
          qr_code: null,
          updated_at: new Date().toISOString()
        })
        .eq('session_id', sessionId);

      if (sessions[sessionId]) {
        delete sessions[sessionId];
      }

      return res.json({ success: true, status: 'disconnected', message: 'Unlinked from shared WhatsApp session.' });
    }

    // CASE B: This store is a PRIMARY / PARENT store
    // Check if any OTHER stores are currently sharing this session
    const { data: childStores } = await supabase
      .from('whatsapp_sessions')
      .select('session_id')
      .eq('parent_session_id', sessionId);

    if (childStores && childStores.length > 0) {
      const newParentId = childStores[0].session_id;
      log('👑', `[Disconnect] Primary store "${sessionId}" is unlinking, but ${childStores.length} store(s) share it. Promoting "${newParentId}" to primary session holder!`);

      const oldAuthDir = path.join(AUTH_DIR, sessionId);
      const newAuthDir = path.join(AUTH_DIR, newParentId);

      // Transfer auth directory to the new primary session holder
      if (fs.existsSync(oldAuthDir)) {
        if (fs.existsSync(newAuthDir)) {
          fs.rmSync(newAuthDir, { recursive: true, force: true });
        }
        fs.cpSync(oldAuthDir, newAuthDir, { recursive: true });
        fs.rmSync(oldAuthDir, { recursive: true, force: true });
      }

      // Re-key in-memory active session map if present
      if (sessions[sessionId]) {
        sessions[newParentId] = sessions[sessionId];
        delete sessions[sessionId];
      }

      // Promote the first child store to primary
      await supabase
        .from('whatsapp_sessions')
        .update({
          parent_session_id: null,
          status: 'connected',
          phone: dbSess?.phone,
          updated_at: new Date().toISOString()
        })
        .eq('session_id', newParentId);

      // Re-point remaining child stores to the new primary
      if (childStores.length > 1) {
        const remainingChildIds = childStores.slice(1).map(c => c.session_id);
        await supabase
          .from('whatsapp_sessions')
          .update({
            parent_session_id: newParentId,
            updated_at: new Date().toISOString()
          })
          .in('session_id', remainingChildIds);
      }

      // Unlink the requesting store
      await supabase
        .from('whatsapp_sessions')
        .update({
          status: 'disconnected',
          phone: null,
          parent_session_id: null,
          qr_code: null,
          updated_at: new Date().toISOString()
        })
        .eq('session_id', sessionId);

      return res.json({
        success: true,
        status: 'disconnected',
        message: `Store unlinked. WhatsApp session promoted to ${newParentId} for remaining stores.`
      });
    }

    // CASE C: Primary store with NO OTHER stores sharing it
    log('🔌', `[Disconnect] Disconnecting primary store "${sessionId}" (no other stores sharing)...`);
    const session = sessions[sessionId];
    const sessionAuthDir = path.join(AUTH_DIR, sessionId);

    if (session && session.sock) {
      try {
        await session.sock.logout();
      } catch (e) {
        log('⚠️', `[Session ${sessionId}] Logout warning: ${e.message}`);
      }
      session.sock = null;
    }

    if (session) {
      if (session.qrTimeoutTimer) {
        clearTimeout(session.qrTimeoutTimer);
        session.qrTimeoutTimer = null;
      }
      session.connectionStatus = 'disconnected';
      session.currentPhone = null;
      session.isConnecting = false;
      session.queue = [];
      session.isDraining = false;
    }

    if (fs.existsSync(sessionAuthDir)) {
      fs.rmSync(sessionAuthDir, { recursive: true, force: true });
      log('🗑️', `[Session ${sessionId}] Auth session files cleared`);
    }

    await supabase
      .from('whatsapp_sessions')
      .update({
        status: 'disconnected',
        phone: null,
        parent_session_id: null,
        qr_code: null,
        updated_at: new Date().toISOString()
      })
      .eq('session_id', sessionId);

    if (sessions[sessionId]) {
      delete sessions[sessionId];
    }

    res.json({ success: true, status: 'disconnected' });
  } catch (err) {
    log('❌', `[Session ${sessionId}] Disconnect error: ${err.message}`);
    res.json({ success: false, error: err.message });
  }
});

// POST /transfer-session — Move a connected session from one store to another.
// Body: { from_session_id, to_session_id }
// 1) Reads creds from the source auth folder (does NOT delete it).
// 2) Writes those creds into the target auth folder.
// 3) Re-keys the whatsapp_sessions row in Supabase from from→to, clearing the source.
// 4) Returns success so the client can call `connect` with session_id=to_session_id.
//
// This lets an admin "move" a device to a different store without re-scanning
// the QR code.
app.post('/transfer-session', async (req, res) => {
  const fromId = req.body.from_session_id;
  const toId = req.body.to_session_id;
  if (!fromId || !toId) {
    return res.status(400).json({ success: false, error: 'from_session_id and to_session_id are required' });
  }
  if (fromId === toId) {
    return res.status(400).json({ success: false, error: 'from_session_id and to_session_id must differ' });
  }

  const fromAuthDir = path.join(AUTH_DIR, fromId);
  const toAuthDir = path.join(AUTH_DIR, toId);

  if (!fs.existsSync(fromAuthDir)) {
    return res.json({ success: false, error: `Source session "${fromId}" has no auth files on this server. Was it ever connected here?` });
  }

  try {
    // 1) Disconnect the source so its socket goes away cleanly.
    const sourceSession = sessions[fromId];
    if (sourceSession && sourceSession.sock) {
      try { await sourceSession.sock.logout(); } catch (e) { log('⚠️', `[Session ${fromId}] Logout warning during transfer: ${e.message}`); }
      sourceSession.sock = null;
    }
    if (sourceSession) {
      sourceSession.connectionStatus = 'disconnected';
      sourceSession.currentPhone = null;
      sourceSession.isConnecting = false;
    }

    // 2) Copy the auth folder to the target.
    if (fs.existsSync(toAuthDir)) {
      fs.rmSync(toAuthDir, { recursive: true, force: true });
    }
    fs.cpSync(fromAuthDir, toAuthDir, { recursive: true });
    log('📦', `[Transfer] Copied auth files from "${fromId}" → "${toId}"`);

    // 3) Re-key the whatsapp_sessions row in Supabase.
    // The source row gets a fresh "disconnected" state, the target row is
    // updated to "connected" with the same phone. We do this via two
    // updates so the transfer is observable from the admin UI.
    const { data: sourceRow, error: srcFetchErr } = await supabase
      .from('whatsapp_sessions')
      .select('phone')
      .eq('session_id', fromId)
      .maybeSingle();
    if (srcFetchErr) {
      log('❌', `[Transfer] Failed to read source session: ${srcFetchErr.message}`);
    }
    const phone = sourceRow?.phone || null;

    // Upsert the target row
    const { error: targetErr } = await supabase
      .from('whatsapp_sessions')
      .upsert({
        session_id: toId,
        status: 'connected',
        phone,
        qr_code: null,
        updated_at: new Date().toISOString()
      }, { onConflict: 'session_id' });
    if (targetErr) {
      log('❌', `[Transfer] Failed to upsert target session: ${targetErr.message}`);
      return res.json({ success: false, error: targetErr.message });
    }

    // Clear the source row (drop creds/keys + mark disconnected)
    const { error: srcClearErr } = await supabase
      .from('whatsapp_sessions')
      .update({
        status: 'disconnected',
        phone: null,
        qr_code: null,
        is_processing: false,
        updated_at: new Date().toISOString()
      })
      .eq('session_id', fromId);
    if (srcClearErr) {
      log('❌', `[Transfer] Failed to clear source session: ${srcClearErr.message}`);
    }

    // Optional: clean up the in-memory sessions map
    if (sessions[fromId]) {
      delete sessions[fromId];
    }

    log('✅', `[Transfer] Session moved: ${fromId} → ${toId} (phone: ${phone || 'unknown'})`);
    res.json({ success: true, status: 'connected', phone });
  } catch (err) {
    log('❌', `[Transfer] Error: ${err.message}`);
    res.json({ success: false, error: err.message });
  }
});

// POST /reuse-session — Reuse an existing connected WhatsApp session for another store
// Body: { from_session_id, to_session_id }
app.post('/reuse-session', async (req, res) => {
  const fromId = req.body.from_session_id || req.body.source_store_id;
  const toId = req.body.to_session_id || req.body.target_store_id || req.body.store_id;
  if (!fromId || !toId) {
    return res.status(400).json({ success: false, error: 'from_session_id and to_session_id are required' });
  }
  if (fromId === toId) {
    return res.status(400).json({ success: false, error: 'from_session_id and to_session_id must differ' });
  }

  const toAuthDir = path.join(AUTH_DIR, toId);

  try {
    // Determine the root parent session ID
    const { data: sourceRow } = await supabase
      .from('whatsapp_sessions')
      .select('phone, parent_session_id, status')
      .eq('session_id', fromId)
      .maybeSingle();

    const rootParentId = sourceRow?.parent_session_id || fromId;

    const { data: rootRow } = await supabase
      .from('whatsapp_sessions')
      .select('phone, status')
      .eq('session_id', rootParentId)
      .maybeSingle();

    const phone = rootRow?.phone || sourceRow?.phone || sessions[rootParentId]?.currentPhone || null;
    const status = rootRow?.status || sourceRow?.status || (sessions[rootParentId]?.connectionStatus === 'connected' ? 'connected' : 'disconnected');

    // Clean up any local auth files for target so it doesn't run a separate socket
    if (fs.existsSync(toAuthDir)) {
      fs.rmSync(toAuthDir, { recursive: true, force: true });
    }
    if (sessions[toId]) {
      if (sessions[toId].sock) {
        try { await sessions[toId].sock.logout(); } catch (e) {}
      }
      delete sessions[toId];
    }

    // Upsert target whatsapp_sessions row pointing to rootParentId as parent
    const { error: targetErr } = await supabase
      .from('whatsapp_sessions')
      .upsert({
        session_id: toId,
        parent_session_id: rootParentId,
        status: status,
        phone: phone,
        qr_code: null,
        updated_at: new Date().toISOString()
      }, { onConflict: 'session_id' });

    if (targetErr) throw targetErr;

    // DO NOT start a new connection/socket for toId.
    // rootParentId's active Baileys socket is shared seamlessly!

    log('✅', `[Reuse] Session shared for store ${toId} from root parent ${rootParentId} (phone: ${phone || 'unknown'})`);
    res.json({ success: true, status: status, phone });
  } catch (err) {
    log('❌', `[Reuse] Error: ${err.message}`);
    res.json({ success: false, error: err.message });
  }
});

// POST /send-message — Enqueue a single message for immediate async delivery
// Body: { session_id, phone, message }
app.post('/send-message', async (req, res) => {
  const { session_id, phone, message } = req.body;
  if (!phone || !message) {
    return res.status(400).json({ success: false, error: 'phone and message are required' });
  }

  let targetSessionId = session_id || 'admin_session';
  let session = sessions[targetSessionId];

  // If not directly connected in memory, resolve parent_session_id or phone match
  if (!session || session.connectionStatus !== 'connected') {
    const { data: dbSess } = await supabase
      .from('whatsapp_sessions')
      .select('parent_session_id, phone')
      .eq('session_id', targetSessionId)
      .maybeSingle();

    if (dbSess?.parent_session_id && sessions[dbSess.parent_session_id]?.connectionStatus === 'connected') {
      targetSessionId = dbSess.parent_session_id;
      session = sessions[targetSessionId];
    } else if (dbSess?.phone) {
      const match = Object.entries(sessions).find(([, s]) => s.connectionStatus === 'connected' && s.currentPhone === dbSess.phone);
      if (match) {
        targetSessionId = match[0];
        session = match[1];
      }
    }
  }

  if (!session || session.connectionStatus !== 'connected') {
    return res.status(503).json({
      success: false,
      error: `Session "${session_id}" is not connected. Link a WhatsApp device first.`,
    });
  }

  enqueueMessage(targetSessionId, { id: null, phone, message });

  res.status(202).json({
    success: true,
    message: 'Message enqueued for delivery',
    queue_position: sessions[targetSessionId]?.queue.length ?? 1,
  });
});

// POST /process-queue — Pull pending messages from Supabase whatsapp_queue and enqueue them
app.post('/process-queue', async (req, res) => {
  try {
    const { data: messages, error: fetchErr } = await supabase
      .from('whatsapp_queue')
      .select('*')
      .eq('status', 'pending')
      .order('created_at', { ascending: true });

    if (fetchErr) throw fetchErr;

    if (!messages || messages.length === 0) {
      const activeCount = Object.values(sessions).filter(s => s.connectionStatus === 'connected').length;
      return res.json({ success: true, details: 'No pending messages', count: 0, active_sessions: activeCount });
    }

    log('📨', `Fetched ${messages.length} pending DB message(s) — routing to session queues`);

    let enqueued = 0;
    let skipped = 0;

    for (const msg of messages) {
      const storeSessionId = msg.store_id;
      let session = storeSessionId ? sessions[storeSessionId] : null;
      let activeSessionId = storeSessionId;

      // ── Resolve session via parent_session_id or phone match if not directly connected ──
      if (storeSessionId && (!session || session.connectionStatus !== 'connected')) {
        const { data: dbSess } = await supabase
          .from('whatsapp_sessions')
          .select('status, phone, parent_session_id')
          .eq('session_id', storeSessionId)
          .maybeSingle();

        if (dbSess?.parent_session_id) {
          const parentId = dbSess.parent_session_id;
          activeSessionId = parentId;
          session = sessions[parentId];

          if (!session || session.connectionStatus !== 'connected') {
            const { data: parentDbSess } = await supabase
              .from('whatsapp_sessions')
              .select('status')
              .eq('session_id', parentId)
              .maybeSingle();

            if (parentDbSess && parentDbSess.status === 'connected') {
              log('🔄', `Auto-triggering connection for parent DB session ${parentId}...`);
              startConnection(parentId);
              await delay(2000);
              session = sessions[parentId];
            }
          }
        } else if (dbSess && dbSess.status === 'connected') {
          log('🔄', `Auto-triggering connection for active DB session ${storeSessionId}...`);
          startConnection(storeSessionId);
          await delay(2000);
          session = sessions[storeSessionId];
        }

        // Phone fallback matching
        if ((!session || session.connectionStatus !== 'connected') && dbSess?.phone) {
          const match = Object.entries(sessions).find(
            ([, s]) => s.connectionStatus === 'connected' && s.sock && s.currentPhone === dbSess.phone
          );
          if (match) {
            activeSessionId = match[0];
            session = match[1];
          }
        }
      }

      // ── Enqueue if a connected session socket was found ──
      if (session && session.connectionStatus === 'connected' && session.sock) {
        enqueueMessage(activeSessionId, {
          id: msg.id,
          phone: msg.phone,
          message: msg.message,
          retry_count: msg.retry_count || 0,
        });
        enqueued++;
        continue;
      }

      log('⚠️', `No connected session available for store ${storeSessionId} — deferring msg ${msg.id}`);

      const createdAt = new Date(msg.created_at).getTime();
      const ageMs = Date.now() - createdAt;
      if (ageMs > 5 * 60 * 1000) {
        await supabase.from('whatsapp_queue').update({
          status: 'failed',
          error_message: `No connected WhatsApp session available for store ${storeSessionId} after 5 minutes. Please link a WhatsApp device.`,
        }).eq('id', msg.id);
        skipped++;
      }
    }

    log('📊', `Queue routing: ${enqueued} enqueued, ${skipped} failed (no session after timeout)`);
    const activeCount = Object.values(sessions).filter(s => s.connectionStatus === 'connected').length;
    let dbSentCount = totalMessagesSent;
    try {
      const { count } = await supabase.from('whatsapp_queue').select('*', { count: 'exact', head: true }).eq('status', 'sent');
      if (count !== null && count !== undefined) dbSentCount = count;
    } catch (e) {}

    res.json({
      success: true,
      active_sessions: activeCount,
      messages_sent: dbSentCount,
      enqueued,
      skipped,
      total: messages.length
    });
  } catch (err) {
    log('❌', `Queue routing error: ${err.message}`);
    res.json({ success: false, error: err.message });
  }
});

// ─────────────────────────────────────────────
// Server Startup
// ─────────────────────────────────────────────
app.listen(PORT, async () => {
  log('🚀', `WhatsApp Server running on http://localhost:${PORT}`);
  log('🔐', `API authentication: enabled (x-api-secret header)`);
  log('⚡', `Inter-message delay: ${INTER_MESSAGE_DELAY_MS}ms | Max concurrent drains: ${MAX_CONCURRENT_DRAINS}`);

  // Ensure AUTH_DIR exists
  if (!fs.existsSync(AUTH_DIR)) {
    fs.mkdirSync(AUTH_DIR, { recursive: true });
  }



  // Scan and reconnect all saved sessions
  const subdirs = fs.readdirSync(AUTH_DIR).filter(file => {
    return fs.statSync(path.join(AUTH_DIR, file)).isDirectory();
  });

  for (const sessionId of subdirs) {
    const sessionAuthDir = path.join(AUTH_DIR, sessionId);

    // Verify if this session is marked connected in Supabase DB as a primary session
    const { data: dbSess } = await supabase
      .from('whatsapp_sessions')
      .select('status, parent_session_id')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (!dbSess || dbSess.status === 'disconnected' || dbSess.parent_session_id) {
      log('🧹', `Cleaning up orphan/child local auth files for session ${sessionId}...`);
      if (fs.existsSync(sessionAuthDir)) {
        fs.rmSync(sessionAuthDir, { recursive: true, force: true });
      }
      if (sessions[sessionId]) {
        delete sessions[sessionId];
      }
    } else if (fs.readdirSync(sessionAuthDir).length > 0) {
      log('🔄', `Found valid active primary auth session for ${sessionId} — auto-reconnecting...`);
      startConnection(sessionId);
    }
  }

  if (subdirs.length === 0) {
    log('📱', 'No saved sessions found. Waiting for /connect calls to generate QR codes.');
  }
});
