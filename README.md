# OpenClaw WhatsApp Server

A local Node.js server that runs Baileys (WhatsApp Web wrapper) on your PC and exposes it via Cloudflare Tunnel. This allows your Supabase Edge Function to send WhatsApp messages through your personal WhatsApp number.

## Architecture

```
Admin Dashboard → Supabase Edge Function → Cloudflare Tunnel → This Server (Your PC) → WhatsApp
```

## Quick Setup

### 1. Install Dependencies

```bash
cd whatsapp-server
npm install
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and fill in:
- `SUPABASE_SERVICE_ROLE_KEY` — Get from Supabase Dashboard → Settings → API
- `WHATSAPP_API_SECRET` — Any random string (must match the Edge Function secret)

### 3. Start the Server

```bash
npm start
```

The server will start on `http://localhost:3001`.

### 4. Expose via Cloudflare Tunnel

#### Quick Test (Temporary URL)

```bash
cloudflared tunnel --url http://localhost:3001
```

This gives you a temporary `https://xxxx.trycloudflare.com` URL. Copy it.

#### Production (Permanent URL)

```bash
# One-time setup
cloudflared tunnel login
cloudflared tunnel create openclaw-whatsapp
cloudflared tunnel route dns openclaw-whatsapp whatsapp.yourdomain.com

# Create config file at ~/.cloudflared/config.yml:
# tunnel: <TUNNEL_ID>
# credentials-file: ~/.cloudflared/<TUNNEL_ID>.json
# ingress:
#   - hostname: whatsapp.yourdomain.com
#     service: http://localhost:3001
#   - service: http_status:404

# Run
cloudflared tunnel run openclaw-whatsapp
```

### 5. Set Edge Function Secrets

```bash
# Set the tunnel URL and shared secret in Supabase
npx supabase secrets set WHATSAPP_SERVER_URL=https://your-tunnel-url.trycloudflare.com
npx supabase secrets set WHATSAPP_API_SECRET=your-secret-string-from-env
```

### 6. Link WhatsApp

Go to Admin Dashboard → WhatsApp → Click "Link WhatsApp Device" → Scan the QR code with your phone.

## API Endpoints

All endpoints require `x-api-secret` header.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/status` | Get current connection status |
| POST | `/connect` | Start WhatsApp connection (generates QR) |
| POST | `/disconnect` | Logout and clear session |
| POST | `/process-queue` | Send pending messages from queue |

## Anti-Ban Protections

The server implements several measures to prevent WhatsApp from flagging your number:

1. **Random initial delay** — 2-5 seconds before each message
2. **Typing presence** — Shows "typing..." to the recipient
3. **Human-like typing speed** — 50ms per character (max 7 seconds)
4. **Pause after sending** — Stops typing indicator after message sent
5. **Sequential processing** — Messages sent one at a time, never in burst

## Auto-Reconnect

If the server restarts (PC reboot, etc.), it automatically reconnects using saved session files in `auth_sessions/`. No need to re-scan the QR code.

## Troubleshooting

- **QR code doesn't appear**: Make sure the server is running and Cloudflare Tunnel is active
- **Connection keeps dropping**: Check your internet connection; Baileys will auto-reconnect
- **Messages not sending**: Verify WhatsApp is connected (check `/status` endpoint)
- **"Logged out" error**: Your WhatsApp session was revoked. Click "Link Device" again to re-scan
