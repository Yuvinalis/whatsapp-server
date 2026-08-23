import express from 'express';
import makeWASocket, { useMultiFileAuthState, DisconnectReason, Browsers, fetchLatestBaileysVersion, downloadMediaMessage } from '@whiskeysockets/baileys';
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

const extractPhoneFromMsg = (msg) => {
  const candidates = [
    msg.key?.remoteJidAlt,
    msg.key?.remoteJid,
    msg.key?.participantAlt,
    msg.key?.participant,
    msg.participant,
  ];
  for (const cand of candidates) {
    if (cand && typeof cand === 'string' && cand.includes('@s.whatsapp.net')) {
      const phone = cand.split('@')[0].split(':')[0].replace(/\D/g, '');
      if (phone) return phone;
    }
  }
  for (const cand of candidates) {
    if (cand && typeof cand === 'string' && !cand.endsWith('@lid')) {
      const phone = cand.split('@')[0].split(':')[0].replace(/\D/g, '');
      if (phone) return phone;
    }
  }
  return null;
};

const matchPhone = (storedPhone, incomingPhone) => {
  if (!storedPhone || !incomingPhone) return false;
  const cleanStored = String(storedPhone).replace(/\D/g, '');
  const cleanIncoming = String(incomingPhone).replace(/\D/g, '');
  if (!cleanStored || !cleanIncoming) return false;
  if (cleanStored === cleanIncoming) return true;
  const s9 = cleanStored.length >= 9 ? cleanStored.slice(-9) : cleanStored;
  const i9 = cleanIncoming.length >= 9 ? cleanIncoming.slice(-9) : cleanIncoming;
  return s9 === i9;
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
    if (updates.status === 'connected' || updates.status === 'connecting' || updates.status === 'disconnected') {
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
  try {
    const sessionAuthDir = path.join(AUTH_DIR, sessionId === 'default' ? '' : sessionId);
    const credsFile = path.join(sessionAuthDir, 'creds.json');

    // If local creds file already exists and is valid, no need to overwrite
    if (fs.existsSync(credsFile) && fs.statSync(credsFile).size > 10) {
      return true;
    }

    log('📥', `[Session ${sessionId}] Attempting auth restoration from Supabase DB...`);

    const { data: dbSess, error } = await supabase
      .from('whatsapp_sessions')
      .select('auth_data, parent_session_id')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (error || !dbSess) {
      log('ℹ️', `[Session ${sessionId}] No DB record found for auth restoration.`);
      return false;
    }

    let authData = dbSess.auth_data;

    // If this is a child session sharing a parent session, fetch parent's auth_data
    if (!authData && dbSess.parent_session_id) {
      const { data: parentDb } = await supabase
        .from('whatsapp_sessions')
        .select('auth_data')
        .eq('session_id', dbSess.parent_session_id)
        .maybeSingle();
      if (parentDb) authData = parentDb.auth_data;
    }

    if (!authData || typeof authData !== 'object' || Object.keys(authData).length === 0) {
      log('ℹ️', `[Session ${sessionId}] DB record has no auth_data payload.`);
      return false;
    }

    // Ensure target folder exists
    if (!fs.existsSync(sessionAuthDir)) {
      fs.mkdirSync(sessionAuthDir, { recursive: true });
    }

    let restoredCount = 0;
    for (const [fileName, fileContent] of Object.entries(authData)) {
      if (!fileName || !fileContent) continue;
      const filePath = path.join(sessionAuthDir, fileName);
      const strContent = typeof fileContent === 'string' ? fileContent : JSON.stringify(fileContent);
      fs.writeFileSync(filePath, strContent, 'utf-8');
      restoredCount++;
    }

    log('✅', `[Session ${sessionId}] Successfully restored ${restoredCount} auth file(s) from Supabase DB to ${sessionAuthDir}`);
    return true;
  } catch (err) {
    log('❌', `[Session ${sessionId}] Failed to restore auth from Supabase DB: ${err.message}`);
    return false;
  }
}

/**
 * Saves current local auth files to Supabase (whatsapp_sessions table)
 */
async function saveAuthToSupabase(sessionId) {
  try {
    const sessionAuthDir = path.join(AUTH_DIR, sessionId === 'default' ? '' : sessionId);
    const credsFile = path.join(sessionAuthDir, 'creds.json');

    if (!fs.existsSync(credsFile) || fs.statSync(credsFile).size <= 10) {
      return;
    }

    const files = fs.readdirSync(sessionAuthDir).filter(f => f.endsWith('.json'));
    if (!files.length) return;

    const fileMap = {};
    for (const fileName of files) {
      const filePath = path.join(sessionAuthDir, fileName);
      try {
        const content = fs.readFileSync(filePath, 'utf-8');
        fileMap[fileName] = JSON.parse(content);
      } catch (pErr) {
        fileMap[fileName] = fs.readFileSync(filePath, 'utf-8');
      }
    }

    const { error } = await supabase
      .from('whatsapp_sessions')
      .update({
        auth_data: fileMap,
        updated_at: new Date().toISOString()
      })
      .eq('session_id', sessionId);

    if (error) throw error;
    log('💾', `[Session ${sessionId}] Successfully backed up ${Object.keys(fileMap).length} auth file(s) to Supabase DB.`);
  } catch (err) {
    log('❌', `[Session ${sessionId}] Failed to backup auth to Supabase DB: ${err.message}`);
  }
}

// Helper to process incoming, outgoing, or historic WhatsApp messages
async function processIncomingOrHistoricMessage(sessionId, sock, msg) {
  if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast') return;

  const senderPhoneRaw = extractPhoneFromMsg(msg);
  if (!senderPhoneRaw) return;

  const isFromMe = !!msg.key.fromMe;
  const senderType = isFromMe ? 'agent' : 'lead';

  let text = msg.message.conversation ||
    msg.message.extendedTextMessage?.text ||
    msg.message.imageMessage?.caption ||
    msg.message.videoMessage?.caption ||
    msg.message.documentMessage?.caption ||
    msg.message.buttonsResponseMessage?.selectedDisplayText ||
    msg.message.listResponseMessage?.title ||
    msg.message.templateButtonReplyMessage?.selectedDisplayText ||
    '';

  if (!text.trim()) {
    if (msg.message.imageMessage) text = '📷 [Image Attachment]';
    else if (msg.message.videoMessage) text = '🎥 [Video Attachment]';
    else if (msg.message.audioMessage) text = '🎵 [Audio Message]';
    else if (msg.message.documentMessage) text = '📄 [Document Attachment]';
    else if (msg.message.stickerMessage) text = '🎨 [Sticker]';
    else if (msg.message.contactMessage || msg.message.contactsArrayMessage) text = '🎴 [Contact Card]';
    else if (msg.message.locationMessage || msg.message.liveLocationMessage) text = '📍 [Location Pin]';
    else text = '💬 [Message]';
  }

  const { data: matchedLeads, error: leadErr } = await supabase
    .from('marketing_leads')
    .select('id, business_name, phone_number, whatsapp_number, sela_ai_enabled, agent_id, created_by');

  if (leadErr || !matchedLeads) return;

  const targetLead = matchedLeads.find((l) => {
    return matchPhone(l.phone_number, senderPhoneRaw) || matchPhone(l.whatsapp_number, senderPhoneRaw);
  });

  if (!targetLead) return;

  // Send Delivery Receipt (2 Gray Ticks on lead's WhatsApp) for incoming lead messages
  if (!isFromMe && msg.key.remoteJid && msg.key.id && sock) {
    try {
      await sock.sendReceipt(msg.key.remoteJid, msg.key.participant, [msg.key.id], 'delivery');
      log('✔️✔️', `[Session ${sessionId}] Delivery receipt (2 ticks) sent to ${msg.key.remoteJid} for msg ${msg.key.id}`);
    } catch (e) {
      log('⚠️', `[Session ${sessionId}] Delivery receipt error: ${e.message}`);
    }
  }

  // Deduplication check: if wa_message_id already exists in lead_chat_messages for this lead, skip
  if (msg.key.id) {
    const { data: existing } = await supabase
      .from('lead_chat_messages')
      .select('id')
      .eq('lead_id', targetLead.id)
      .eq('wa_message_id', msg.key.id)
      .maybeSingle();

    if (existing) return;
  }

  // Optimistic message matching for outgoing agent/admin messages within a 60-second window
  let pendingMatchId = null;
  if (isFromMe) {
    const windowStart = new Date(Date.now() - 60000).toISOString();
    const { data: pendingCandidates } = await supabase
      .from('lead_chat_messages')
      .select('id, message_text')
      .eq('lead_id', targetLead.id)
      .in('sender_type', ['agent', 'admin'])
      .is('wa_message_id', null)
      .gte('created_at', windowStart)
      .order('created_at', { ascending: false });

    if (pendingCandidates && pendingCandidates.length > 0) {
      const matchTextClean = text.trim();
      const matched = pendingCandidates.find(
        (c) => c.message_text.trim() === matchTextClean || (matchTextClean && matchTextClean.includes(c.message_text.trim()))
      );
      pendingMatchId = matched ? matched.id : pendingCandidates[0].id;
    }
  }

  // Process & Download Media / Attachments (Images, Audio, Video, Documents, Stickers)
  let mediaUrl = null;
  const isMediaMsg = !!(
    msg.message?.imageMessage ||
    msg.message?.videoMessage ||
    msg.message?.audioMessage ||
    msg.message?.documentMessage ||
    msg.message?.stickerMessage
  );

  if (isMediaMsg && typeof downloadMediaMessage === 'function') {
    try {
      const buffer = await downloadMediaMessage(
        msg,
        'buffer',
        {},
        { logger: pino({ level: 'silent' }), reuploadRequest: sock?.updateMediaMessage }
      );

      if (buffer && buffer.length > 0) {
        let ext = 'bin';
        let mimeType = 'application/octet-stream';
        if (msg.message.imageMessage) {
          ext = 'jpg';
          mimeType = msg.message.imageMessage.mimetype || 'image/jpeg';
        } else if (msg.message.audioMessage) {
          ext = 'mp3';
          mimeType = msg.message.audioMessage.mimetype || 'audio/ogg';
        } else if (msg.message.videoMessage) {
          ext = 'mp4';
          mimeType = msg.message.videoMessage.mimetype || 'video/mp4';
        } else if (msg.message.stickerMessage) {
          ext = 'webp';
          mimeType = 'image/webp';
        } else if (msg.message.documentMessage) {
          mimeType = msg.message.documentMessage.mimetype || 'application/pdf';
          const docName = msg.message.documentMessage.fileName || 'document.pdf';
          ext = docName.includes('.') ? docName.split('.').pop() : 'pdf';
        }

        const safeKeyId = msg.key.id ? msg.key.id.replace(/[^a-zA-Z0-9_-]/g, '') : Math.random().toString(36).substring(2);
        const storagePath = `whatsapp-media/${targetLead.id}/${Date.now()}_${safeKeyId}.${ext}`;
        
        const { error: uploadErr } = await supabase.storage
          .from('media')
          .upload(storagePath, buffer, { contentType: mimeType, upsert: true });

        if (!uploadErr) {
          const { data: publicData } = supabase.storage.from('media').getPublicUrl(storagePath);
          mediaUrl = publicData?.publicUrl || null;
          log('📎', `[Session ${sessionId}] Downloaded media and stored at ${mediaUrl}`);
        } else {
          log('⚠️', `[Session ${sessionId}] Supabase media upload error: ${uploadErr.message}`);
        }
      }
    } catch (mediaErr) {
      log('⚠️', `[Session ${sessionId}] Baileys downloadMediaMessage error: ${mediaErr.message}`);
    }
  }

  log('📩', `[Session ${sessionId}] Ingesting WhatsApp ${senderType} msg with ${senderPhoneRaw}: "${text.substring(0, 50)}"`);

  const msgTimestamp = msg.messageTimestamp 
    ? new Date(Number(msg.messageTimestamp) * 1000).toISOString()
    : new Date().toISOString();

  if (pendingMatchId) {
    const updatePayload = {
      wa_message_id: msg.key.id || null,
      message_status: 'sent',
      created_at: msgTimestamp
    };
    if (mediaUrl) updatePayload.media_url = mediaUrl;

    await supabase
      .from('lead_chat_messages')
      .update(updatePayload)
      .eq('id', pendingMatchId);
    log('✅', `[Session ${sessionId}] Linked optimistic message ${pendingMatchId} with wa_message_id ${msg.key.id}`);
  } else {
    const { error: insErr } = await supabase.from('lead_chat_messages').insert({
      lead_id: targetLead.id,
      sender_type: senderType,
      session_id: sessionId,
      message_text: text,
      media_url: mediaUrl || null,
      message_status: isFromMe ? 'sent' : 'delivered',
      wa_message_id: msg.key.id || null,
      created_at: msgTimestamp
    });

    if (insErr) {
      log('❌', `Failed to insert lead chat message: ${insErr.message}`);
    }
  }

  await supabase.from('marketing_leads').update({
    last_message_at: msgTimestamp,
    whatsapp_session_id: sessionId
  }).eq('id', targetLead.id);

  // Push Notification for incoming lead messages
  if (!isFromMe) {
    try {
      await fetch(`${SUPABASE_URL}/functions/v1/send-fcm-notification`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`
        },
        body: JSON.stringify({
          type: 'lead_reply_notification',
          leadId: targetLead.id,
          businessName: targetLead.business_name,
          messageText: text,
          agentId: targetLead.agent_id || targetLead.created_by || null
        })
      });
    } catch (pushErr) {}
  }

  // Sela AI Auto Reply
  if (!isFromMe && targetLead.sela_ai_enabled) {
    log('🤖', `[Session ${sessionId}] Sela AI auto-reply triggered for "${targetLead.business_name}"`);
    const aiReplyText = `Hello! Thanks for reaching out to ${targetLead.business_name}. Sela AI Assistant here: We are updating your store layout and catalog! Send us any questions or product details anytime.`;

    await supabase.from('lead_chat_messages').insert({
      lead_id: targetLead.id,
      sender_type: 'system',
      session_id: sessionId,
      message_text: aiReplyText,
      message_status: 'sent',
    });

    enqueueMessage(sessionId, {
      phone: senderPhoneRaw,
      message: aiReplyText
    });
  }
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

    let sessionAuthDir = path.join(AUTH_DIR, sessionId);
    if (!fs.existsSync(path.join(sessionAuthDir, 'creds.json')) && fs.existsSync(path.join(AUTH_DIR, 'creds.json'))) {
      sessionAuthDir = AUTH_DIR;
    }
    const { state, saveCreds } = await useMultiFileAuthState(sessionAuthDir);

    const sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      browser: Browsers.ubuntu('Chrome'),
      logger: pino({ level: 'silent' }),
      syncFullHistory: false, // Prevents historic chat sync from blocking Node event loop & timing out connection
      generateHighQualityLinkPreview: false,
      markOnlineOnConnect: true,
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 60000,
      keepAliveIntervalMs: 25000,
    });

    session.sock = sock;
    sock.ev.on('creds.update', async () => {
      // Clear QR expiration timer as soon as credentials update (QR scanned)
      if (session.qrTimeoutTimer) {
        clearTimeout(session.qrTimeoutTimer);
        session.qrTimeoutTimer = null;
      }
      await saveCreds();
      await saveAuthToSupabase(sessionId);
    });

    sock.ev.on('connection.update', async (update) => {
      const { connection, qr, lastDisconnect } = update;

      // ── Connecting / Linking State (e.g. after QR scan) ──
      if (connection === 'connecting') {
        if (session.connectionStatus === 'qrcode') {
          log('⏳', `[Session ${sessionId}] QR code scanned! Linking device...`);
          session.connectionStatus = 'connecting';
          if (session.qrTimeoutTimer) {
            clearTimeout(session.qrTimeoutTimer);
            session.qrTimeoutTimer = null;
          }
          await updateSessionStatus(sessionId, { status: 'connecting', qr_code: null });
        }
      }

      // ── QR Code Received ──
      if (qr) {
        log('📱', `[Session ${sessionId}] QR Code received — scan with your phone`);
        session.connectionStatus = 'qrcode';

        // 2-minute QR code expiration timer
        if (session.qrTimeoutTimer) {
          clearTimeout(session.qrTimeoutTimer);
        }
        session.qrTimeoutTimer = setTimeout(async () => {
          if (session.connectionStatus === 'connected') return;

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
        session.currentPhone = (sock.user?.id || sock.user?.phone || '').split('@')[0].split(':')[0].replace(/\D/g, '');
        
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

        const error = lastDisconnect?.error;
        const statusCode = error?.output?.statusCode || error?.statusCode;
        const errorMessage = error?.message || error?.toString() || '';

        // Comprehensive check for device logout on phone / session expiration
        const isLoggedOut =
          statusCode === DisconnectReason.loggedOut ||
          statusCode === 401 ||
          statusCode === 403 ||
          statusCode === 405 ||
          errorMessage.toLowerCase().includes('logged out') ||
          errorMessage.toLowerCase().includes('bad session') ||
          errorMessage.toLowerCase().includes('unauthorized');

        log('🔌', `[Session ${sessionId}] Connection closed. Code: ${statusCode || 'none'}. Error: "${errorMessage}". IsLoggedOut: ${isLoggedOut}`);

        if (isLoggedOut) {
          log('🗑️', `[Session ${sessionId}] Session unlinked/logged out on phone (code ${statusCode || 'logout'}). Cleaning up auth state...`);

          if (fs.existsSync(sessionAuthDir) && sessionAuthDir !== AUTH_DIR) {
            fs.rmSync(sessionAuthDir, { recursive: true, force: true });
          }

          // If child stores were sharing this session, update them to disconnected gracefully
          const { data: childStores } = await supabase
            .from('whatsapp_sessions')
            .select('session_id')
            .eq('parent_session_id', sessionId);

          if (childStores && childStores.length > 0) {
            log('⚠️', `[Session ${sessionId}] Primary session logged out on phone. Marking ${childStores.length} child store(s) as disconnected.`);
            await supabase
              .from('whatsapp_sessions')
              .update({
                status: 'disconnected',
                phone: null,
                parent_session_id: null,
                qr_code: null,
                updated_at: new Date().toISOString()
              })
              .eq('parent_session_id', sessionId);
          }

          await updateSessionStatus(sessionId, {
            status: 'disconnected',
            phone: null,
            qr_code: null,
          });

          if (sessions[sessionId]) {
            delete sessions[sessionId];
          }
          return;
        }

        const credsFile = path.join(sessionAuthDir, 'creds.json');
        const hasCreds = fs.existsSync(credsFile) && fs.statSync(credsFile).size > 10;
        const isRestartRequired = statusCode === DisconnectReason.restartRequired || statusCode === 515;
        const isTimedOut = statusCode === DisconnectReason.timedOut || statusCode === 408;

        const shouldReconnect = !isLoggedOut && (
          wasConnected ||
          hasCreds ||
          isRestartRequired ||
          isTimedOut ||
          session.connectionStatus === 'connecting'
        );

        if (shouldReconnect) {
          log('🔄', `[Session ${sessionId}] Connection dropped (${statusCode || 'reconnect'}). Reconnecting automatically...`);
          session.connectionStatus = 'connecting';
          await updateSessionStatus(sessionId, { status: 'connecting', qr_code: null });
          const delayMs = isRestartRequired ? 1000 : 3000;
          setTimeout(() => startConnection(sessionId), delayMs);
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

    // ── WhatsApp Real-time & Historic Messages Ingestion ──
    sock.ev.on('messages.upsert', async (mUpsert) => {
      try {
        if (!mUpsert || !mUpsert.messages || !mUpsert.messages.length) return;
        for (const msg of mUpsert.messages) {
          await processIncomingOrHistoricMessage(sessionId, sock, msg);
        }
      } catch (upsertErr) {
        log('❌', `[Session ${sessionId}] Error in messages.upsert: ${upsertErr.message}`);
      }
    });

    sock.ev.on('messaging-history.set', async ({ messages: histMessages }) => {
      try {
        if (!histMessages || !histMessages.length) return;
        log('📜', `[Session ${sessionId}] Received ${histMessages.length} historic chat messages from WhatsApp history sync`);
        for (const msg of histMessages) {
          await processIncomingOrHistoricMessage(sessionId, sock, msg);
        }
      } catch (hErr) {
        log('❌', `[Session ${sessionId}] Error in messaging-history.set: ${hErr.message}`);
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
// Session Health Validator & Periodic Auto-Cleanup Loop
// Safe check: Only deletes sessions that are genuinely disconnected/unlinked on phone or missing credentials.
// Preserves all active sockets and auto-reconnects idle credentialed sessions.
// ─────────────────────────────────────────────
async function validateAllDatabaseSessions() {
  try {
    const { data: dbSessions } = await supabase
      .from('whatsapp_sessions')
      .select('session_id, status, phone, parent_session_id');

    if (!dbSessions || dbSessions.length === 0) return;

    for (const dbSess of dbSessions) {
      const id = dbSess.session_id;

      if (dbSess.parent_session_id) {
        const parentInMem = sessions[dbSess.parent_session_id];
        if (parentInMem && parentInMem.connectionStatus === 'connected') {
          continue;
        }
        const { data: parentDb } = await supabase
          .from('whatsapp_sessions')
          .select('status')
          .eq('session_id', dbSess.parent_session_id)
          .maybeSingle();

        if (!parentDb || parentDb.status === 'disconnected') {
          log('🧹', `Cleaning up child session ${id} with dead parent ${dbSess.parent_session_id}`);
          await updateSessionStatus(id, { status: 'disconnected', phone: null });
        }
        continue;
      }

      const memSess = sessions[id];

      if (memSess && memSess.connectionStatus === 'connected' && memSess.sock) {
        continue;
      }

      if (memSess && (memSess.connectionStatus === 'connecting' || memSess.connectionStatus === 'qrcode')) {
        continue;
      }

      const hasAuthCreds = await restoreAuthFromSupabase(id);

      if (hasAuthCreds) {
        if (!memSess?.isConnecting) {
          log('🔄', `[Session Validator] Auth restored for session ${id} — auto-reconnecting...`);
          startConnection(id);
        }
      } else {
        log('⚠️', `[Session Validator] Marking unlinked session ${id} as disconnected in database`);
        await updateSessionStatus(id, { status: 'disconnected', phone: null });
        if (sessions[id]) delete sessions[id];
      }
    }
  } catch (err) {
    log('⚠️', `[Session Validator] Error validating DB sessions: ${err.message}`);
  }
}

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

  if (session && session.connectionStatus === 'connected' && session.sock) {
    return res.json({
      success: true,
      status: 'connected',
      phone: session.currentPhone,
      qr_code: null,
      queue_length: session.queue.length,
    });
  }

  if (session && (session.connectionStatus === 'connecting' || session.connectionStatus === 'qrcode')) {
    return res.json({
      success: true,
      status: session.connectionStatus,
      phone: session.currentPhone,
      qr_code: session.qrCode,
      queue_length: session.queue.length,
    });
  }

  const { data: dbSess } = await supabase
    .from('whatsapp_sessions')
    .select('status, phone, parent_session_id, qr_code')
    .eq('session_id', sessionId)
    .maybeSingle();

  if (!dbSess) {
    return res.json({
      success: true,
      status: 'disconnected',
      phone: null,
      qr_code: null,
      queue_length: 0,
    });
  }

  if (dbSess.parent_session_id && sessions[dbSess.parent_session_id]?.connectionStatus === 'connected') {
    const parent = sessions[dbSess.parent_session_id];
    return res.json({
      success: true,
      status: 'connected',
      phone: parent.currentPhone || dbSess.phone,
      qr_code: null,
      queue_length: parent.queue.length,
    });
  }

  const sessionAuthDir = path.join(AUTH_DIR, sessionId);
  const credsFile = path.join(sessionAuthDir, 'creds.json');
  const hasAuthCreds = fs.existsSync(credsFile) && fs.statSync(credsFile).size > 10;

  if (hasAuthCreds) {
    startConnection(sessionId);
    await delay(1500);
    const recheckedSession = sessions[sessionId];
    if (recheckedSession && recheckedSession.connectionStatus === 'connected') {
      return res.json({
        success: true,
        status: 'connected',
        phone: recheckedSession.currentPhone,
        qr_code: null,
        queue_length: recheckedSession.queue.length,
      });
    }
  }

  await updateSessionStatus(sessionId, { status: 'disconnected', phone: null });

  res.json({
    success: true,
    status: 'disconnected',
    phone: null,
    qr_code: null,
    queue_length: 0,
  });
});

// GET /verify-sessions — Validate all sessions in DB and clean up dead ones
app.get('/verify-sessions', async (req, res) => {
  await validateAllDatabaseSessions();

  const { data: activeDbSessions } = await supabase
    .from('whatsapp_sessions')
    .select('session_id, phone, status')
    .eq('status', 'connected');

  res.json({
    success: true,
    active_sessions: activeDbSessions || []
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

    if (fs.existsSync(sessionAuthDir) && sessionAuthDir !== AUTH_DIR) {
      fs.rmSync(sessionAuthDir, { recursive: true, force: true });
      log('🗑️', `[Session ${sessionId}] Auth session files cleared`);
    } else if (sessionAuthDir === AUTH_DIR && fs.existsSync(AUTH_DIR)) {
      const files = fs.readdirSync(AUTH_DIR);
      for (const f of files) {
        const p = path.join(AUTH_DIR, f);
        if (fs.statSync(p).isFile()) fs.unlinkSync(p);
      }
      log('🗑️', `[Session ${sessionId}] Root auth session files cleared`);
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

// POST /sync-lead-chat — Pull / sync recent WhatsApp messages for a lead
app.post('/sync-lead-chat', async (req, res) => {
  const { session_id, lead_id, phone } = req.body;
  if (!lead_id && !phone) {
    return res.status(400).json({ success: false, error: 'lead_id or phone is required' });
  }

  try {
    let targetLead = null;
    if (lead_id) {
      const { data } = await supabase
        .from('marketing_leads')
        .select('id, business_name, phone_number, whatsapp_number')
        .eq('id', lead_id)
        .maybeSingle();
      targetLead = data;
    }

    if (!targetLead && phone) {
      const { data: leads } = await supabase
        .from('marketing_leads')
        .select('id, business_name, phone_number, whatsapp_number');
      if (leads) {
        targetLead = leads.find(l => matchPhone(l.phone_number, phone) || matchPhone(l.whatsapp_number, phone));
      }
    }

    if (!targetLead) {
      return res.status(404).json({ success: false, error: 'Lead not found for phone/id' });
    }

    // Send Read Receipts (Blue Ticks) to WhatsApp for this lead
    let targetSessId = session_id;
    let sess = targetSessId ? sessions[targetSessId] : null;
    if (!sess || sess.connectionStatus !== 'connected') {
      const active = Object.entries(sessions).find(([, s]) => s.connectionStatus === 'connected' && s.sock);
      if (active) sess = active[1];
    }

    if (sess && sess.sock && targetLead) {
      try {
        const leadPhone = targetLead.whatsapp_number || targetLead.phone_number;
        const jid = formatJid(leadPhone);

        // On-demand history sync: request chat history from WhatsApp via Baileys fetchMessageHistory if supported
        if (typeof sess.sock.fetchMessageHistory === 'function') {
          try {
            log('📜', `[Sync] Requesting chat history fetch for ${jid}...`);
            await sess.sock.fetchMessageHistory(100, undefined, undefined);
          } catch (fErr) {
            log('⚠️', `[Sync] History fetch error for ${jid}: ${fErr.message}`);
          }
        }

        const { data: leadMsgs } = await supabase
          .from('lead_chat_messages')
          .select('wa_message_id')
          .eq('lead_id', targetLead.id)
          .eq('sender_type', 'lead')
          .not('wa_message_id', 'is', null);

        if (leadMsgs && leadMsgs.length > 0) {
          const keysToRead = leadMsgs.map(m => ({ remoteJid: jid, id: m.wa_message_id, fromMe: false }));
          await sess.sock.readMessages(keysToRead);
          for (const m of leadMsgs) {
            await sess.sock.sendReceipt(jid, undefined, [m.wa_message_id], 'read');
          }
          log('🔵🔵', `[Sync] Sent Read Receipt (Blue Ticks) for ${leadMsgs.length} message(s) to ${jid}`);
        }
      } catch (rErr) {
        log('⚠️', `Read receipt sync warning: ${rErr.message}`);
      }
    }

    const { count } = await supabase
      .from('lead_chat_messages')
      .select('*', { count: 'exact', head: true })
      .eq('lead_id', targetLead.id);

    res.json({
      success: true,
      message: 'Lead chat synced successfully',
      lead_id: targetLead.id,
      business_name: targetLead.business_name,
      total_messages: count || 0
    });
  } catch (err) {
    log('❌', `Sync lead chat error: ${err.message}`);
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
      const storeSessionId = msg.session_id || msg.store_id;
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

      // ── Active session fallback matching if no exact session match was found ──
      if (!session || session.connectionStatus !== 'connected' || !session.sock) {
        const activeFallback = Object.entries(sessions).find(
          ([, s]) => s.connectionStatus === 'connected' && s.sock
        );
        if (activeFallback) {
          activeSessionId = activeFallback[0];
          session = activeFallback[1];
          log('🔀', `Fallback: routing msg ${msg.id} to active connected session ${activeSessionId}`);
        } else {
          const { data: anyConnSess } = await supabase
            .from('whatsapp_sessions')
            .select('session_id')
            .eq('status', 'connected')
            .limit(1)
            .maybeSingle();

          if (anyConnSess?.session_id) {
            log('🔄', `Auto-triggering connection for active fallback session ${anyConnSess.session_id}...`);
            startConnection(anyConnSess.session_id);
            await delay(2000);
            if (sessions[anyConnSess.session_id]?.connectionStatus === 'connected') {
              activeSessionId = anyConnSess.session_id;
              session = sessions[anyConnSess.session_id];
            }
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



  // Restore all saved auth sessions from Supabase DB on boot
  try {
    const { data: dbSessions } = await supabase
      .from('whatsapp_sessions')
      .select('session_id, status, auth_data')
      .not('auth_data', 'is', null);

    if (dbSessions && dbSessions.length > 0) {
      log('🌐', `[Boot] Found ${dbSessions.length} session(s) in Supabase DB with auth backups — restoring...`);
      for (const s of dbSessions) {
        const restored = await restoreAuthFromSupabase(s.session_id);
        if (restored) {
          log('🔄', `[Boot] Restored and auto-connecting session ${s.session_id}...`);
          startConnection(s.session_id);
        }
      }
    }
  } catch (bootErr) {
    log('⚠️', `[Boot] Error restoring sessions from DB: ${bootErr.message}`);
  }

  // Periodic automatic validation loop to clean up dead/unlinked sessions from database safely
  setInterval(validateAllDatabaseSessions, 45000);
  setTimeout(validateAllDatabaseSessions, 5000);
});
