// Mock of Streamer.bot — lets the whole ticker run with NO Streamer.bot install, for
// local styling and the verify suites. It copies the two SB surfaces Zipper depends
// on, as they were measured on Streamer.bot 1.0.7:
//
//   • HTTP Server on :7474 — SB Path->Folder file serving. Maps:
//       /zipper-overlay/*     -> overlay/       (ticker.html + css + js)
//       /zipper-shared/*      -> zipper-shared/ (panel-core.js + control.html)
//       /                     -> a landing index for quick manual testing
//     Like SB 1.0.7, it gives .svg, .woff and .woff2 no type of their own: they are
//     served as application/octet-stream.
//   • WebSocket Server on :8080 — speaks SB's envelope. On `Subscribe` it acks
//     (enforcing the lowercase `general` key exactly like real SB). On `DoAction` it
//     replies the way SB does: at once, when the action is QUEUED (not when it has
//     run), with the request's own args plus actionId, actionName, queueName,
//     runningActionId and actionQueuedAt; and with an error when no action has that
//     name. Then it runs a copy of the C# action, line for line:
//       Ticker Push      actions/ticker-push.cs      (store + broadcast, or replay)
//       Ticker Command   actions/ticker-command.cs   (the relay, chat parsing included)
//       Ticker Announce  actions/ticker-announce.cs  (compose, clamp, broadcast)
//     Every broadcast goes to EVERY subscribed client, as CPH.WebsocketBroadcastJson
//     does — the connect-sync replay of Ticker Push too, not only to the client that
//     asked. tools/cs/ticker-cases.json is the table both are run against: verify.mjs
//     runs it here, verify-cs.mjs through the compiled C#.
//
// Test and dev controls, on the HTTP port (each answers with JSON):
//   /mock/push  (POST)    body = a full ticker:update payload; stored and broadcast
//                         (what the sidecar's DoAction becomes)
//   /mock/fixture         stores + broadcasts platforms/fixtures/sample-update.json
//   /mock/announce?text=…&kind=sub&duration=5   broadcasts a ticker:announce (dev helper)
//   /mock/reset           forgets the stored payload: an install with nothing saved
//   /mock/bleed?on=1|0    overlap mode. Off (the default) is a BLOCKING action queue:
//                         one run at a time, each with its own arguments. On is SB's
//                         Default queue as measured: two DoActions for one action within
//                         about 10 ms both run with the later one's arguments, and the
//                         replies say queueName 'Default'. MOCK_ARG_BLEED=1 turns it on
//                         at start. A test that turns it on turns it off again.
//   /mock/missing?name=…&on=1|0   that action answers "was not found"
//   /mock/silent?name=…&on=1|0    that action is queued (the reply is "ok") and its run
//                                 does nothing: an action that never answers
//   /mock/throw?name=…&on=1|0[&message=…]   that action's run sends ticker:error, as its
//                                 catch block does when it throws (it stands for any throw)
//   /mock/queue?name=…&queue=…    the replies for that action carry this queueName,
//                                 whatever bleed mode says; an empty queue= clears it
//   /mock/kick            closes every open WebSocket connection
//   /mock/stats           counters, switches and the stored payload, for the suites
// A switch's on= takes 1 or 0 only; any other value gets a 400 and changes nothing.
//
// Where it differs from Streamer.bot, on purpose: it refuses WebSocket connections from
// web pages of other sites (an Origin that is not this machine or a local file), and
// messages over 1 MiB (a WebSocket message, or a /mock/push body), because it is a
// development tool on a developer's machine. SB with authentication off does neither.
//
// Not measured, so not claimed: what SB sends when the text handed to
// WebsocketBroadcastJson is not JSON (Ticker Push's check lets `{not json}` through).
// The mock then puts the text in `data` as a string; no page can use it.
//
// This is a REFERENCE/TEST harness. The real thing uses Streamer.bot itself (see
// README.md) with the same wire shapes.

import { WebSocketServer } from 'ws';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { resolve, dirname, extname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OVERLAY_DIR = resolve(__dirname, 'overlay');
const SHARED_DIR = resolve(__dirname, 'zipper-shared');
const SAMPLE_FIXTURE = resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json');

const HTTP_PORT = Number(process.env.SB_HTTP_PORT) || 7474; // SB HTTP Server default
const WS_PORT = Number(process.env.SB_WS_PORT) || 8080; // SB WebSocket Server default
const MAX_PAYLOAD = 1024 * 1024; // bytes in one WebSocket message or /mock/push body

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.ttf': 'font/ttf',
  // Measured on Streamer.bot 1.0.7: .svg, .woff and .woff2 are NOT given their own
  // type; they fall through to application/octet-stream. The pages have to work with that.
};

// The C# action's persisted global, mirrored: the last tickerPayload string.
let storedPayload = null;
const stats = { pushCount: 0, commandCount: 0, announceCount: 0 };

// EMPTY_UPDATE in actions/ticker-push.cs, the same text.
const EMPTY_UPDATE = '{"type":"ticker:update","tournament":null,"matches":[],"standings":[],"caps":{"text":"","logo":""}}';

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
<p class="dim">Stored payload: ${storedPayload ? storedPayload.length + ' chars' : '(none — sync replays an empty update)'}</p>`;
}

// ── Test controls ────────────────────────────────────────────────────────────
const ACTIONS = ['Ticker Push', 'Ticker Command', 'Ticker Announce'];
const ACTION_IDS = Object.fromEntries(ACTIONS.map((name) => [name, randomUUID()]));
const BLEED_WINDOW_MS = 10;          // requests for one action this close together overlap
const DEFAULT_THROW_MESSAGE = 'mock: the action threw (the throw switch is on)';
let argBleed = process.env.MOCK_ARG_BLEED === '1';
const missing = new Set();           // action names that answer "was not found"
const silent = new Set();            // action names whose runs do nothing
const throwing = new Map();          // action name → the message its ticker:error carries
const queueFor = new Map();          // action name → the queueName its replies carry
const pendingByAction = new Map();   // bleed mode: name → [{ args }] waiting out the window
let totalRuns = 0;

function json(res, body, status = 200) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(body));
}

// A switch's on= value: true for '1', false for '0', undefined when it is not given,
// and null for anything else ('true', 'yes', 'on'), which the switch refuses with a 400.
function onParam(query) {
  if (!query.has('on')) return undefined;
  const v = query.get('on');
  return v === '1' ? true : v === '0' ? false : null;
}
const BAD_ON = { ok: false, error: 'on must be 0 or 1' };

// Reads a request body up to MAX_PAYLOAD bytes; null when it is longer. The rest is
// still read (and dropped), so the client gets the answer rather than a reset.
async function readBody(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size <= MAX_PAYLOAD) chunks.push(chunk);
  }
  return size > MAX_PAYLOAD ? null : Buffer.concat(chunks).toString('utf8');
}

// ── HTTP server (SB HTTP Server mimic) ───────────────────────────────────────
// The handler is async, so anything it threw would be an unhandled rejection and end
// the process. Everything is inside one try: a bad request gets an answer.
const http = createServer(async (req, res) => {
  try {
    await handleHttp(req, res);
  } catch (err) {
    console.error('[mock] request error:', err && err.message);
    try {
      if (!res.headersSent) res.writeHead(500, { 'content-type': 'text/plain' });
      res.end('error');
    } catch {}
  }
});

async function handleHttp(req, res) {
  const url = req.url || '/';
  let path;
  try { path = decodeURIComponent(url.split('?')[0]); }
  catch { res.writeHead(400, { 'content-type': 'text/plain' }); res.end('bad path'); return; }
  const query = new URLSearchParams(url.split('?')[1] || '');

  if (path === '/mock/push' && req.method === 'POST') {
    const body = await readBody(req);
    if (body === null) return json(res, { ok: false, error: `body is over ${MAX_PAYLOAD} bytes` }, 413);
    try { JSON.parse(body); } catch { return json(res, { ok: false, error: 'body is not JSON' }, 400); }
    storedPayload = body;
    stats.pushCount++;
    broadcast(customEvent(storedPayload));
    return json(res, { ok: true });
  }
  if (path === '/mock/fixture') {
    try {
      storedPayload = await readFile(SAMPLE_FIXTURE, 'utf-8');
      stats.pushCount++;
      broadcast(customEvent(storedPayload));
      return json(res, { ok: true });
    } catch (err) {
      return json(res, { ok: false, error: err.message }, 500);
    }
  }
  if (path === '/mock/announce') {
    // Dev helper: /mock/announce?text=...&kind=sub&duration=5
    const text = String(query.get('text') || '').trim();
    if (!text) return json(res, { ok: false, error: 'text required' }, 400);
    const kind = query.get('kind') || 'custom';
    const duration = Math.min(30, Math.max(2, parseInt(query.get('duration'), 10) || 8));
    stats.announceCount++;
    broadcast(customEvent(JSON.stringify({ type: 'ticker:announce', kind, text, duration })));
    return json(res, { ok: true });
  }
  if (path === '/mock/reset') {
    storedPayload = null;
    return json(res, { storedPayload });
  }
  if (path === '/mock/bleed') {
    const on = onParam(query);
    if (on === null) return json(res, BAD_ON, 400);
    if (on !== undefined) argBleed = on;
    return json(res, { argBleed });
  }
  if (path === '/mock/missing' || path === '/mock/silent' || path === '/mock/throw' || path === '/mock/queue') {
    const name = query.get('name');
    if (!ACTIONS.includes(name)) return json(res, { ok: false, error: `name must be one of: ${ACTIONS.join(', ')}` }, 400);
    if (path === '/mock/queue') {
      if (query.has('queue')) { const q = query.get('queue'); if (q) queueFor.set(name, q); else queueFor.delete(name); }
      return json(res, { queues: Object.fromEntries(queueFor) });
    }
    const on = onParam(query);
    if (on === null) return json(res, BAD_ON, 400);
    if (path === '/mock/throw') {
      if (on === true) throwing.set(name, query.get('message') || DEFAULT_THROW_MESSAGE);
      else if (on === false) throwing.delete(name);
      return json(res, { throwing: Object.fromEntries(throwing) });
    }
    const set = path === '/mock/missing' ? missing : silent;
    if (on === true) set.add(name);
    else if (on === false) set.delete(name);
    return json(res, { [path.slice(6)]: [...set] });
  }
  if (path === '/mock/kick') {
    const open = [...clients];
    for (const ws of open) { try { ws.close(); } catch {} }
    return json(res, { kicked: open.length });
  }
  if (path === '/mock/stats') {
    return json(res, {
      ...stats, totalRuns, clients: clients.size, hasPayload: !!storedPayload, storedPayload,
      argBleed, missing: [...missing], silent: [...silent], throwing: Object.fromEntries(throwing), queues: Object.fromEntries(queueFor),
    });
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
  await serveFile(res, file);
}

http.on('error', (err) => { console.error('[mock] HTTP error:', err.message); process.exit(1); });
http.listen(HTTP_PORT, '127.0.0.1', () => {
  console.log(`[mock] HTTP  http://127.0.0.1:${HTTP_PORT}/  (landing page + overlay + control)`);
});

// ── WebSocket server (SB WebSocket Server mimic) ─────────────────────────────
// Who may connect: a client that sends no Origin (Node's ws: the verify suites and the
// sidecar), a page this machine serves (127.0.0.1, localhost or [::1], any port), and
// a local file (an OBS source added as a file sends "file://" or "null"). A page of any
// other site is refused, so a web page open in a browser cannot drive the mock.
function originAllowed(origin) {
  if (!origin || origin === 'null' || origin === 'file://') return true;
  return /^https?:\/\/(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$/.test(origin);
}

const wss = new WebSocketServer({
  host: '127.0.0.1', port: WS_PORT, maxPayload: MAX_PAYLOAD,
  verifyClient: ({ origin }, done) => (originAllowed(origin) ? done(true) : done(false, 403, 'Forbidden')),
});
const clients = new Set();

wss.on('error', (err) => { console.error('[mock] WS error:', err.message); process.exit(1); });
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.subscribedCustom = false; // only a correctly-subscribed client receives broadcasts
  // Without a listener, a protocol error on one connection (a message over the limit,
  // bad UTF-8) would end the process.
  ws.on('error', (err) => console.log('[mock] client error:', err.message));
  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return; // `null`, a number, a string: not a request
    try { handleRequest(ws, m); }
    catch (err) { console.error('[mock] request failed:', err && err.message); }
  });
  ws.on('close', () => clients.delete(ws));
});
wss.on('listening', () => console.log(`[mock] WS    ws://127.0.0.1:${WS_PORT}  (Streamer.bot mock)`));

function handleRequest(ws, m) {
  if (m.request === 'Subscribe') {
    // Real Streamer.bot uses a LOWERCASE source key ('general') in Subscribe,
    // while delivered events carry a capitalized source ('General'). Enforce
    // that here so a wrong-case subscribe gets nothing — exactly like real SB.
    const gen = m.events && m.events.general;
    ws.subscribedCustom = Array.isArray(gen) && gen.includes('Custom');
    console.log('[mock] Subscribe', ws.subscribedCustom ? 'OK (general.Custom)' : 'IGNORED (expected lowercase events.general:["Custom"])');
    ws.send(JSON.stringify({ id: m.id, status: 'ok', result: { events: m.events } }));
  } else if (m.request === 'DoAction') {
    const name = m.action && m.action.name;
    // Measured on Streamer.bot 1.0.7: an action that does not exist gets an error
    // reply; one that exists gets "ok" AT ONCE, when it is queued (not when it has
    // run), echoing the request's own args plus the queue it was put in. So the
    // reply says nothing about what the run did, or whether it has happened yet.
    if (!ACTIONS.includes(name) || missing.has(name)) {
      ws.send(JSON.stringify({ id: m.id, status: 'error', error: `The action '${name}' was not found` }));
      return;
    }
    const args = (m.args && typeof m.args === 'object' && !Array.isArray(m.args)) ? m.args : {};
    const queueName = queueFor.has(name) ? queueFor.get(name) : (argBleed ? 'Default' : 'Mock Blocking');
    ws.send(JSON.stringify({
      id: m.id, status: 'ok',
      args: {
        ...args,
        actionId: ACTION_IDS[name], actionName: name, queueName,
        runningActionId: randomUUID(), actionQueuedAt: new Date().toISOString(),
      },
    }));
    queueAction(name, args);
  }
}

function broadcast(msg) {
  for (const ws of clients) { if (ws.subscribedCustom) { try { ws.send(msg); } catch {} } }
}

// ── Action runs ──────────────────────────────────────────────────────────────
// Bleed mode off behaves like actions in a BLOCKING queue: one run at a time, in
// arrival order, each with its own args.
//
// Bleed mode on behaves like Streamer.bot's DEFAULT queue as measured on 1.0.7: runs of
// one action that overlap all see ONE request's arguments (the later one's, in 25 of 25
// same-socket tries; it crosses sockets too). The mock holds each request for
// BLEED_WINDOW_MS and then runs every request for that action that arrived in the
// window with the LAST one's args.
function queueAction(name, args) {
  if (!argBleed) { runAction(name, args, false); return; }
  let pending = pendingByAction.get(name);
  if (!pending) {
    pending = [];
    pendingByAction.set(name, pending);
    setTimeout(() => {
      pendingByAction.delete(name);
      const last = pending[pending.length - 1].args;
      for (let i = 0; i < pending.length; i++) runAction(name, last, pending.length > 1);
    }, BLEED_WINDOW_MS);
  }
  pending.push({ args });
}

// A run that throws (an argument whose text cannot be taken, say) is logged and goes no
// further. It must not reach the caller: in bleed mode the runs happen later, in a timer,
// where a throw would end the process and skip the other runs of that window.
function runAction(name, args, bled) {
  totalRuns++;
  try {
    if (silent.has(name)) { console.log(`[mock] ${name} did nothing (the silent switch is on)`); return; }
    if (throwing.has(name)) { actionThrew(name, throwing.get(name)); return; }
    if (bled) console.log(`[mock] ${name} ran with another request's arguments (bleed mode)`);
    if (name === 'Ticker Push') runTickerPush(args);
    else if (name === 'Ticker Command') runTickerCommand(args);
    else runTickerAnnounce(args);
  } catch (err) {
    console.log(`[mock] ${name} run failed: ${err && err.message}`);
  }
}

// ── Copies of the C# actions ─────────────────────────────────────────────────
// Everything from here to the end of runTickerAnnounce follows the C# line for line.
// Change them together: tools/cs/ticker-cases.json is the table both are run against
// (verify.mjs here, verify-cs.mjs for the compiled C#), and verify fails when they differ.

// CPH.TryGetArg<string>: no value when the argument is absent or null; any other value
// as its text. The pages and the sidecar send strings only.
function tryGetArg(args, key) {
  if (!Object.prototype.hasOwnProperty.call(args, key)) return null;
  const v = args[key];
  return v == null ? null : String(v);
}

// C#'s Char.IsWhiteSpace, which String.Trim() and string.IsNullOrWhiteSpace use: the
// Unicode space separators, U+2028, U+2029, tab to carriage return, and U+0085. Unlike
// JavaScript's trim(), it does not take U+FEFF (the byte-order mark).
const CS_SPACE = /[\t\n\v\f\r \u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]/;
function csTrim(s) {
  let a = 0, b = s.length;
  while (a < b && CS_SPACE.test(s[a])) a++;
  while (b > a && CS_SPACE.test(s[b - 1])) b--;
  return s.substring(a, b);
}
function isNullOrWhiteSpace(s) {
  return s == null || csTrim(s).length === 0;
}
// string.StartsWith("{") / EndsWith("}") in the C# compare by culture, which passes over
// a byte-order mark (U+FEFF) at that end (measured with verify:cs). Other characters such
// a comparison may pass over are not copied.
function csStartsWith(s, c) { let i = 0; while (i < s.length && s[i] === '\ufeff') i++; return s[i] === c; }
function csEndsWith(s, c) { let i = s.length - 1; while (i >= 0 && s[i] === '\ufeff') i--; return i >= 0 && s[i] === c; }

// JsonStr in the actions: quotes and backslashes escaped, \b \f \n \r \t by name, every
// other character below a space as \u00xx, the rest as it is.
function jsonStr(s) {
  if (s == null) return '""';
  let out = '"';
  for (const c of s) {
    switch (c) {
      case '"': out += '\\"'; break;
      case '\\': out += '\\\\'; break;
      case '\b': out += '\\b'; break;
      case '\f': out += '\\f'; break;
      case '\n': out += '\\n'; break;
      case '\r': out += '\\r'; break;
      case '\t': out += '\\t'; break;
      default: out += c < ' ' ? '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0') : c;
    }
  }
  return out + '"';
}

// The catch block every action has: a warning in the log, and ticker:error to every
// subscriber with the exception's message.
function actionThrew(name, message) {
  console.log(`[mock] ${name} ERROR: ${message}`);
  broadcast(customEvent('{"type":"ticker:error","message":' + jsonStr(message) + '}'));
}

// actions/ticker-push.cs
function runTickerPush(args) {
  let payload = tryGetArg(args, 'tickerPayload');
  if (payload !== null && !isNullOrWhiteSpace(payload)) {
    // Sidecar push: cheap sanity check only — the sidecar owns the shape.
    payload = csTrim(payload);
    if (!csStartsWith(payload, '{') || !csEndsWith(payload, '}')) { actionThrew('Ticker Push', 'tickerPayload is not a JSON object'); return; }
    storedPayload = payload; // CPH.SetGlobalVar("ticker.payload", payload, true)
    stats.pushCount++;
    console.log(`[mock] Ticker Push (sidecar) — broadcasting ${payload.length} chars`);
  } else {
    // Overlay connect-sync: replay the persisted payload, to every subscriber.
    payload = storedPayload;
    if (isNullOrWhiteSpace(payload)) payload = EMPTY_UPDATE;
    console.log(`[mock] Ticker Push (sync replay) — broadcasting ${payload.length} chars`);
  }
  broadcast(customEvent(payload));
}

// actions/ticker-command.cs
function runTickerCommand(args) {
  let command = '';
  let value = '';
  let raw = tryGetArg(args, 'rawInput');
  raw = raw == null ? '' : csTrim(raw);
  const cid = tryGetArg(args, 'commandId');
  // A chat Command trigger sets ITS OWN `command` argument (the matched chat command,
  // e.g. "!ticker") plus a `commandId`, which a control-page DoAction never carries.
  if (cid !== null && !isNullOrWhiteSpace(cid)) {
    const c = tryGetArg(args, 'command');
    // .Trim().TrimStart('!').ToLowerInvariant(). toLowerCase() stands in for
    // ToLowerInvariant(); the table compares the two on ASCII letters only.
    const chatCmd = csTrim(c ?? '').replace(/^!+/, '').toLowerCase();
    if (chatCmd === 'ticker') {
      const sp = raw.indexOf(' ');
      command = sp < 0 ? raw : raw.substring(0, sp);
      value = sp < 0 ? '' : csTrim(raw.substring(sp + 1));
    } else {
      command = chatCmd;
      value = raw;
    }
  } else {
    const c = tryGetArg(args, 'command');
    if (c !== null && !isNullOrWhiteSpace(c)) {
      // Control page / Stream Deck: explicit { command, value } args.
      command = csTrim(c);
      const v = tryGetArg(args, 'value');
      if (v !== null) value = v;
    } else if (raw.length > 0) {
      // Bare rawInput fallback (manual Set Argument setups).
      const sp = raw.indexOf(' ');
      command = sp < 0 ? raw : raw.substring(0, sp);
      value = sp < 0 ? '' : csTrim(raw.substring(sp + 1));
    }
  }
  if (isNullOrWhiteSpace(command)) {
    console.log('[mock] Ticker Command: no command argument — nothing to relay');
    return;
  }
  stats.commandCount++;
  broadcast(customEvent('{"type":"ticker:command","command":' + jsonStr(command) + ',"value":' + jsonStr(value) + '}'));
  console.log(`[mock] Ticker Command → ${command}`);
}

// int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture): white space (tab,
// line feed, vertical tab, form feed, carriage return, space) round it, one leading + or
// -, then the digits 0 to 9; NUL characters may follow (measured with verify:cs).
// Anything else, or a value outside Int32, fails (null).
function intTryParse(s) {
  const m = /^[\t\n\v\f\r ]*([+-]?)([0-9]+)[\t\n\v\f\r ]*\0*$/.exec(s);
  if (!m) return null;
  let n = BigInt(m[2]);
  if (m[1] === '-') n = -n;
  if (n < -2147483648n || n > 2147483647n) return null;
  return Number(n);
}

// actions/ticker-announce.cs
function runTickerAnnounce(args) {
  const arg = (key) => { const v = tryGetArg(args, key); return v != null ? csTrim(v) : ''; };
  const firstArg = (...keys) => { for (const k of keys) { const v = arg(k); if (v.length > 0) return v; } return ''; };
  let kind = arg('kind');
  let text = arg('text');
  const user = firstArg('user', 'userName', 'displayName');
  // Auto-compose from common Twitch trigger arguments when no explicit text was set.
  if (text.length === 0) {
    const viewers = firstArg('viewers', 'viewerCount', 'raiders');
    if (viewers.length > 0) {
      if (kind.length === 0) kind = 'raid';
      text = (user.length > 0 ? user : 'Someone') + ' is raiding with ' + viewers + ' viewers!';
    } else if (user.length > 0) {
      if (kind.length === 0) kind = 'sub';
      text = user + ' just subscribed!';
    }
  }
  if (kind.length === 0) kind = 'custom';
  if (text.length === 0) {
    console.log('[mock] Ticker Announce: no text and nothing to compose from — skipped');
    return;
  }
  let duration = intTryParse(arg('duration'));
  if (duration === null) duration = 8;
  duration = Math.min(30, Math.max(2, duration));
  stats.announceCount++;
  broadcast(customEvent('{"type":"ticker:announce","kind":' + jsonStr(kind) + ',"text":' + jsonStr(text) + ',"duration":' + duration + '}'));
  console.log(`[mock] Ticker Announce (${kind}) — ${text}`);
}

// A verify suite starts the mock with a pipe on stdin and MOCK_EXIT_WITH_PARENT=1.
// When the suite goes away, however it goes, the pipe closes and the mock exits, so
// it is not left holding the ports.
if (process.env.MOCK_EXIT_WITH_PARENT === '1') {
  process.stdin.on('end', () => process.exit(0));
  process.stdin.on('close', () => process.exit(0));
  process.stdin.on('error', () => process.exit(0));
  process.stdin.resume();
}
