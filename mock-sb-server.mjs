// Throwaway mock of Streamer.bot — lets the whole ticker run with NO Streamer.bot
// install, for local styling and the verify suites. It faithfully mimics the two
// SB surfaces Zipper depends on:
//
//   • HTTP Server on :7474 — SB Path->Folder file serving. Maps:
//       /zipper-overlay/*     -> overlay/       (ticker.html + css + js)
//       /zipper-shared/*      -> zipper-shared/ (panel-core.js + control.html)
//       /mock/push  (POST)    -> body = a full ticker:update payload; stored and
//                                broadcast (what the sidecar's DoAction becomes)
//       /mock/fixture         -> loads platforms/fixtures/sample-update.json,
//                                stores + broadcasts it (quick styling data)
//       /mock/stats           -> { pushCount, commandCount, clients } for verify
//       /                     -> a landing index for quick manual testing
//   • WebSocket Server on :8080 — speaks SB's envelope. On `Subscribe` it acks
//     (enforcing the lowercase `general` key exactly like real SB); on
//     `DoAction "Ticker Push"` WITH a tickerPayload arg it persists + broadcasts
//     (the C# action's sidecar path); WITHOUT one it replays the persisted
//     payload to that client (the sync-on-connect path). `DoAction "Ticker
//     Command"` re-broadcasts { type:'ticker:command', command, value } to every
//     subscribed client (the C# relay the sidecar + control page ride).
//
// This is a REFERENCE/TEST harness. The real thing uses Streamer.bot itself (see
// README.md) with the same wire shapes.

import { WebSocketServer } from 'ws';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { resolve, dirname, extname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OVERLAY_DIR = resolve(__dirname, 'overlay');
const SHARED_DIR = resolve(__dirname, 'zipper-shared');
const SAMPLE_FIXTURE = resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json');

const HTTP_PORT = Number(process.env.SB_HTTP_PORT) || 7474; // SB HTTP Server default
const WS_PORT = Number(process.env.SB_WS_PORT) || 8080; // SB WebSocket Server default

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml', '.woff': 'font/woff', '.woff2': 'font/woff2', '.ttf': 'font/ttf',
};

// The C# action's persisted global, mirrored: the last tickerPayload string.
let storedPayload = null;
const stats = { pushCount: 0, commandCount: 0, announceCount: 0 };

const EMPTY_UPDATE = JSON.stringify({
  type: 'ticker:update', tournament: null, matches: [], standings: [], caps: { text: '', logo: '' },
});

// SB's General.Custom envelope for a WebsocketBroadcastJson payload. Real SB
// delivers `data` as the parsed object; panel-core also handles strings.
function customEvent(jsonString) {
  let data;
  try { data = JSON.parse(jsonString); } catch { data = jsonString; }
  return JSON.stringify({
    timeStamp: '1970-01-01T00:00:00.0000000',
    event: { source: 'General', type: 'Custom' },
    data,
  });
}

function safeResolve(base, rel) {
  const file = resolve(base, '.' + rel);
  return file === base || file.startsWith(base + '\\') || file.startsWith(base + '/') ? file : null;
}

async function serveFile(res, file) {
  try {
    const body = await readFile(file);
    res.writeHead(200, { 'content-type': MIME[extname(file).toLowerCase()] || 'application/octet-stream' });
    res.end(body);
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  }
}

function landingHtml() {
  return `<!doctype html><meta charset="utf-8"><title>Zipper — mock Streamer.bot</title>
<style>
  body{font:15px/1.5 system-ui,sans-serif;max-width:760px;margin:2rem auto;padding:0 1rem;color:#e6e6e6;background:#141414}
  h1{font-size:1.3rem} a{color:#9ad} .dim{color:#888;font-size:.85em} ul{margin:.2rem 0;padding-left:1.2rem}
  code{background:#222;padding:.1em .4em;border-radius:4px}
</style>
<h1>Zipper &mdash; mock Streamer.bot</h1>
<p>Open a page below as an OBS Browser Source (or in a tab), then hit
<a href="/mock/fixture">/mock/fixture</a> to broadcast the sample tournament payload.
Feed live data by running the sidecar against this mock:
<code>set SB_WS_PORT=${WS_PORT}&amp;&amp; npm run sidecar</code></p>
<ul>
  <li><a href="/zipper-overlay/ticker.html?w=1920&sbport=${WS_PORT}" target="_blank">ticker.html · 1920px</a></li>
  <li><a href="/zipper-overlay/ticker.html?w=640&sbport=${WS_PORT}" target="_blank">ticker.html · 640px</a></li>
  <li><a href="/zipper-shared/control.html?sbport=${WS_PORT}" target="_blank">control.html</a></li>
</ul>
<p class="dim">Stored payload: ${storedPayload ? storedPayload.length + ' bytes' : '(none — sync replays an empty update)'}</p>`;
}

// ── HTTP server (SB HTTP Server mimic) ───────────────────────────────────────
const http = createServer(async (req, res) => {
  const url = req.url || '/';
  const path = decodeURIComponent(url.split('?')[0]);

  if (path === '/mock/push' && req.method === 'POST') {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      try { JSON.parse(body); } catch {
        res.writeHead(400, { 'content-type': 'application/json' });
        res.end('{"ok":false,"error":"body is not JSON"}');
        return;
      }
      storedPayload = body;
      stats.pushCount++;
      broadcast(customEvent(storedPayload));
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{"ok":true}');
    });
    return;
  }
  if (path === '/mock/fixture') {
    try {
      storedPayload = await readFile(SAMPLE_FIXTURE, 'utf-8');
      stats.pushCount++;
      broadcast(customEvent(storedPayload));
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{"ok":true}');
    } catch (err) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    }
    return;
  }
  if (path === '/mock/announce') {
    // Dev helper: /mock/announce?text=...&kind=sub&duration=5
    const query = new URLSearchParams((req.url || '').split('?')[1] || '');
    const text = String(query.get('text') || '').trim();
    if (!text) {
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end('{"ok":false,"error":"text required"}');
      return;
    }
    const kind = query.get('kind') || 'custom';
    const duration = Math.min(30, Math.max(2, parseInt(query.get('duration'), 10) || 8));
    stats.announceCount++;
    broadcast(customEvent(JSON.stringify({ type: 'ticker:announce', kind, text, duration })));
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{"ok":true}');
    return;
  }
  if (path === '/mock/stats') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ...stats, clients: clients.size, hasPayload: !!storedPayload }));
    return;
  }
  if (path === '/' || path === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(landingHtml());
    return;
  }
  // Two SB HTTP maps, identical to the real-SB config: `zipper-overlay` →
  // overlay/ and `zipper-shared` → zipper-shared/. The overlay loads
  // "/zipper-shared/panel-core.js" by absolute path.
  let file = null;
  if (path.startsWith('/zipper-overlay/')) file = safeResolve(OVERLAY_DIR, path.slice('/zipper-overlay'.length));
  else if (path.startsWith('/zipper-shared/')) file = safeResolve(SHARED_DIR, path.slice('/zipper-shared'.length));
  if (!file) {
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('Map /zipper-overlay/ (overlay/) or /zipper-shared/ (zipper-shared/).');
    return;
  }
  return serveFile(res, file);
});

http.on('error', (err) => { console.error('[mock] HTTP error:', err.message); process.exit(1); });
http.listen(HTTP_PORT, '127.0.0.1', () => {
  console.log(`[mock] HTTP  http://127.0.0.1:${HTTP_PORT}/  (landing page + overlay + control)`);
});

// ── WebSocket server (SB WebSocket Server mimic) ─────────────────────────────
const wss = new WebSocketServer({ host: '127.0.0.1', port: WS_PORT });
const clients = new Set();

wss.on('error', (err) => { console.error('[mock] WS error:', err.message); process.exit(1); });
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.subscribedCustom = false; // only a correctly-subscribed client receives broadcasts
  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (m.request === 'Subscribe') {
      // Real Streamer.bot uses a LOWERCASE source key ('general') in Subscribe,
      // while delivered events carry a capitalized source ('General'). Enforce
      // that here so a wrong-case subscribe gets nothing — exactly like real SB.
      const gen = m.events && m.events.general;
      ws.subscribedCustom = Array.isArray(gen) && gen.includes('Custom');
      console.log('[mock] Subscribe', ws.subscribedCustom ? 'OK (general.Custom)' : 'IGNORED (expected lowercase events.general:["Custom"])');
      ws.send(JSON.stringify({ id: m.id, status: 'ok', result: { events: m.events } }));
    } else if (m.request === 'DoAction') {
      ws.send(JSON.stringify({ id: m.id, status: 'ok' }));
      const name = m.action && m.action.name;
      if (name === 'Ticker Push') {
        const payload = m.args && m.args.tickerPayload;
        if (typeof payload === 'string' && payload.trim().startsWith('{')) {
          // Sidecar path: persist + broadcast (mirrors ticker-push.cs).
          storedPayload = payload;
          stats.pushCount++;
          broadcast(customEvent(storedPayload));
          console.log(`[mock] Ticker Push (sidecar) — ${payload.length} bytes`);
        } else if (ws.subscribedCustom) {
          // Sync-on-connect path: replay to the requesting client only.
          ws.send(customEvent(storedPayload || EMPTY_UPDATE));
          console.log('[mock] Ticker Push (sync replay)');
        }
      } else if (name === 'Ticker Announce') {
        // Mirrors ticker-announce.cs: ephemeral broadcast, never stored.
        const text = String(m.args?.text ?? '').trim();
        if (text) {
          const kind = String(m.args?.kind ?? 'custom') || 'custom';
          const duration = Math.min(30, Math.max(2, parseInt(m.args?.duration, 10) || 8));
          stats.announceCount++;
          broadcast(customEvent(JSON.stringify({ type: 'ticker:announce', kind, text, duration })));
          console.log(`[mock] Ticker Announce (${kind}) — ${text}`);
        }
      } else if (name === 'Ticker Command') {
        let command = String(m.args?.command ?? '');
        let value = m.args?.value == null ? '' : String(m.args.value);
        // Chat Command triggers set their own `command` ("!ticker") + commandId;
        // mirror ticker-command.cs's mapping so verify covers the chat path.
        if (m.args?.commandId) {
          const raw = String(m.args?.rawInput ?? '').trim();
          const chatCmd = command.trim().replace(/^!/, '').toLowerCase();
          if (chatCmd === 'ticker') {
            const sp = raw.indexOf(' ');
            command = sp < 0 ? raw : raw.slice(0, sp);
            value = sp < 0 ? '' : raw.slice(sp + 1).trim();
          } else {
            command = chatCmd;
            value = raw;
          }
        }
        if (command) {
          stats.commandCount++;
          broadcast(customEvent(JSON.stringify({ type: 'ticker:command', command, value })));
          console.log(`[mock] Ticker Command → ${command}`);
        }
      }
    }
  });
  ws.on('close', () => clients.delete(ws));
});
wss.on('listening', () => console.log(`[mock] WS    ws://127.0.0.1:${WS_PORT}  (Streamer.bot mock)`));

function broadcast(msg) {
  for (const ws of clients) { if (ws.subscribedCustom) { try { ws.send(msg); } catch {} } }
}
