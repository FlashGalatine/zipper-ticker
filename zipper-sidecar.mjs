// Zipper sidecar — the one Node process. Polls the configured tournament
// platform, normalizes results into the ticker:update payload, and pushes it
// through Streamer.bot's WebSocket:
//
//   outbound  on change (or first poll) → DoAction "Ticker Push" with the
//             pre-serialized `tickerPayload` (docs/PROTOCOL.md); the C# action
//             persists it to the `ticker.payload` global and broadcasts it.
//             After every poll → a ticker:status snapshot for the control page.
//             Status is ephemeral and must not clobber the persisted update
//             payload, so it rides the "Ticker Command" relay as the reserved
//             command "__status" (value = status JSON); the control page
//             unwraps it and the sidecar ignores its own echo. No third SB
//             action needed.
//   inbound   subscribes to General.Custom and consumes
//             { type:'ticker:command', command, value } broadcasts emitted by
//             the control page via the "Ticker Command" action.
//
// Commands: setUrl, start, stop, pollNow, setInterval, setCapsText,
//           setCapsLogo, setMaxItems, setMessage, setMessageText,
//           setMessageEnabled, status.
//
// Config lives in ./config.json (gitignored; seeded from config.example.json on
// first run). API keys may also come from env: CHALLONGE_API_KEY, STARTGG_API_KEY.
//
// Test hook: a tournament URL of the form "stub:<path-to-json>" loads a
// normalized platform result from that file instead of the network — used by
// verify.mjs and handy for offline overlay styling.
//
// Run:  start-zipper.bat  ·  npm run sidecar
// Env:  SB_WS_URL (default ws://127.0.0.1:8080/) · ZIPPER_GUARD_PORT (7496)
//       ZIPPER_CONFIG (config file path override)

import { readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';
import { detectPlatform } from './platforms/index.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = process.env.ZIPPER_CONFIG || resolve(__dirname, 'config.json');
const EXAMPLE_PATH = resolve(__dirname, 'config.example.json');
const SB_WS_URL = process.env.SB_WS_URL || `ws://127.0.0.1:${process.env.SB_WS_PORT || 8080}/`;
// Single-instance guard: a second launch (double-click + SB action) exits
// cleanly instead of double-polling. Greenroom holds 7495; Zipper takes 7496.
const GUARD_PORT = Number(process.env.ZIPPER_GUARD_PORT) || 7496;

const MIN_INTERVAL_MS = 15000;
const MAX_BACKOFF_MS = 300000; // 5 min cap when the platform keeps erroring
const MESSAGE_MAX_CHARS = 1000;
const MESSAGE_MAX_LINES = 20;

let config = {
  challongeApiKey: null,
  startggApiKey: null,
  tournamentUrl: null,
  pollIntervalMs: 30000,
  maxItems: 20,
  topN: 0, // >0 = only show matches involving the top-N placed participants
  caps: { text: '', logo: '' },
  // Persistent message mode — while enabled (and the text isn't blank) the
  // overlay crawls these lines INSTEAD of tournament results. One non-blank
  // line per ticker item. Independent of polling: a message shows with no
  // tournament configured at all, so message changes push immediately rather
  // than riding the next poll.
  message: { enabled: false, text: '' },
  // Milestone announcements — StreamElements session counters polled with the
  // JWT (streamelements.com → account → Channel settings → Show secrets). The
  // token stays in this file; it NEVER travels the Streamer.bot bus. Each
  // milestones entry is a step size: e.g. followers: 100 announces at every
  // 100 followers. 0 = off.
  streamElements: { jwtToken: null, pollSeconds: 60 },
  milestones: { followers: 0, subs: 0, tips: 0, cheers: 0 },
  milestoneState: {}, // last announced threshold per metric (persisted)
  autoStart: false,
};

let polling = false;
let pollTimer = null;
let pollInFlight = false;
let consecutiveErrors = 0;
let lastError = '';
let lastPollAt = null;
let lastHash = null;
let lastPayload = null; // last payload built, so config-only changes (message,
                        // caps) can re-push without waiting for a poll
let platformName = null;

let sbWs = null;
let sbReconnectTimer = null;
let sbMsgId = 0;

// ── Config ────────────────────────────────────────────────────────────────────

async function loadConfig() {
  let raw = null;
  try {
    raw = await readFile(CONFIG_PATH, 'utf-8');
  } catch (err) {
    if (err.code !== 'ENOENT') log(`Could not read config: ${err.message}`);
  }
  if (raw == null) {
    try {
      raw = await readFile(EXAMPLE_PATH, 'utf-8');
      await writeFile(CONFIG_PATH, raw, 'utf-8');
      log('Seeded config.json from config.example.json');
    } catch {
      log('No config.example.json — using built-in defaults');
      return;
    }
  }
  try { adoptConfig(JSON.parse(raw)); } catch (err) { log(`Bad config JSON: ${err.message}`); }
}

function adoptConfig(parsed) {
  if (!parsed || typeof parsed !== 'object') return;
  config = {
    challongeApiKey: strOrNull(parsed.challongeApiKey) || strOrNull(process.env.CHALLONGE_API_KEY),
    startggApiKey: strOrNull(parsed.startggApiKey) || strOrNull(process.env.STARTGG_API_KEY),
    tournamentUrl: strOrNull(parsed.tournamentUrl),
    pollIntervalMs: clampInt(parsed.pollIntervalMs, MIN_INTERVAL_MS, 3600000, 30000),
    maxItems: clampInt(parsed.maxItems, 1, 100, 20),
    topN: clampInt(parsed.topN, 0, 100, 0),
    caps: {
      text: String(parsed.caps?.text ?? '').slice(0, 120),
      logo: String(parsed.caps?.logo ?? '').slice(0, 500),
    },
    message: {
      enabled: !!parsed.message?.enabled,
      text: String(parsed.message?.text ?? '').slice(0, MESSAGE_MAX_CHARS),
    },
    streamElements: {
      jwtToken: strOrNull(parsed.streamElements?.jwtToken),
      pollSeconds: clampInt(parsed.streamElements?.pollSeconds, 5, 3600, 60),
    },
    milestones: {
      followers: clampInt(parsed.milestones?.followers, 0, 1000000, 0),
      subs: clampInt(parsed.milestones?.subs, 0, 1000000, 0),
      tips: clampInt(parsed.milestones?.tips, 0, 1000000, 0),
      cheers: clampInt(parsed.milestones?.cheers, 0, 1000000, 0),
    },
    milestoneState: (parsed.milestoneState && typeof parsed.milestoneState === 'object') ? { ...parsed.milestoneState } : {},
    autoStart: !!parsed.autoStart,
  };
}

async function persist() {
  // autoStart persists the CURRENT polling intent so a sidecar restart resumes.
  const out = { ...config, autoStart: polling };
  try { await writeFile(CONFIG_PATH, JSON.stringify(out, null, 2), 'utf-8'); }
  catch (err) { log(`Could not persist config: ${err.message}`); }
}

function strOrNull(v) {
  const s = v == null ? '' : String(v).trim();
  return s || null;
}

function clampInt(v, min, max, fallback) {
  const n = Math.round(Number(v));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

// Parse a JSON-string command value (batched setCaps / setOptions); null on junk.
function parseJson(v) {
  try { return JSON.parse(String(v ?? '')); } catch { return null; }
}

// Booleans arrive as JSON true/false from the control page, but as words from
// chat and Stream Deck ("!ticker setMessageEnabled on").
function truthy(v) {
  if (typeof v === 'boolean') return v;
  const s = String(v ?? '').trim().toLowerCase();
  return s === 'true' || s === 'on' || s === '1' || s === 'yes';
}

// ── Poll loop ─────────────────────────────────────────────────────────────────

function startPolling() {
  if (!config.tournamentUrl) { lastError = 'No tournament URL set'; pushStatus(); return; }
  polling = true;
  persist();
  schedulePoll(0);
}

function stopPolling() {
  polling = false;
  if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
  persist();
  pushStatus();
}

function schedulePoll(delayMs) {
  if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
  if (!polling) return;
  pollTimer = setTimeout(() => { pollTimer = null; pollOnce(); }, delayMs);
}

function nextDelay() {
  if (!consecutiveErrors) return config.pollIntervalMs;
  // Exponential backoff on consecutive failures, capped at 5 minutes.
  return Math.min(config.pollIntervalMs * 2 ** Math.min(consecutiveErrors, 5), MAX_BACKOFF_MS);
}

async function pollOnce() {
  // Emit a status snapshot the moment a poll is requested. The control page's
  // liveness heartbeat expects a reply per command, and a slow fetch would
  // otherwise delay it past the window; this also reflects "polling on" the
  // instant Start is clicked instead of only after the first fetch returns.
  pushStatus();
  if (pollInFlight) return;
  pollInFlight = true;
  try {
    const result = await fetchCurrent();
    consecutiveErrors = 0;
    lastError = '';
    lastPollAt = new Date().toISOString();
    const payload = buildPayload(result);
    lastPayload = payload;
    const hash = hashPayload(payload);
    if (hash !== lastHash) {
      lastHash = hash;
      pushUpdate(payload);
      log(`pushed update (${payload.matches.length} matches, ${payload.standings.length} standings)`);
    }
  } catch (err) {
    consecutiveErrors++;
    lastError = err?.message || String(err);
    lastPollAt = new Date().toISOString();
    log(`poll failed (${consecutiveErrors}x): ${lastError}`);
  } finally {
    pollInFlight = false;
    pushStatus();
    schedulePoll(nextDelay());
  }
}

async function fetchCurrent() {
  const url = config.tournamentUrl;
  // Test hook / offline styling: stub:<path> loads a normalized result file.
  if (url.startsWith('stub:')) {
    platformName = 'stub';
    const raw = await readFile(resolve(__dirname, url.slice(5)), 'utf-8');
    return JSON.parse(raw);
  }
  const platform = detectPlatform(url);
  if (!platform) throw new Error(`Unrecognized tournament URL: ${url}`);
  platformName = platform.name;
  const id = platform.parseId(url);
  if (!id) throw new Error(`${platform.displayName}: could not parse a tournament id from that URL`);
  return platform.fetchTicker(id, config, url);
}

// Normalize + sort + cap into the ticker:update wire payload (docs/PROTOCOL.md).
function buildPayload(result) {
  const rank = { completed: 0, in_progress: 1, upcoming: 2 };
  const standings = (Array.isArray(result.standings) ? result.standings : [])
    .slice()
    .sort((a, b) => (a.placement ?? 999) - (b.placement ?? 999));

  // topN filter: only matches involving the N best-placed participants. Needs
  // standings from the platform (start.gg supplies them; live tournaments use
  // current placements, completed ones the final results). No standings → no
  // filter, everything shows.
  let topNames = null;
  if (config.topN > 0 && standings.length) {
    topNames = new Set(standings.slice(0, config.topN).map((s) => s.name));
  }

  const matches = (Array.isArray(result.matches) ? result.matches : [])
    // Drop TBD-vs-TBD upcoming matches — "TBD vs TBD" tells the viewer nothing.
    .filter((m) => m && !(m.state === 'upcoming' && m.p1?.name === 'TBD' && m.p2?.name === 'TBD'))
    .filter((m) => !topNames || topNames.has(m.p1?.name) || topNames.has(m.p2?.name))
    .slice()
    .sort((a, b) => {
      const r = (rank[a.state] ?? 3) - (rank[b.state] ?? 3);
      if (r) return r;
      // Completed: newest first. In-progress/upcoming: play order.
      return a.state === 'completed'
        ? (b.order ?? 0) - (a.order ?? 0)
        : (a.order ?? 0) - (b.order ?? 0);
    })
    .slice(0, config.maxItems);

  const payload = {
    type: 'ticker:update',
    tournament: {
      name: result.tournamentName || '',
      platform: platformName,
      url: config.tournamentUrl,
      state: result.state || 'in_progress',
    },
    generatedAt: new Date().toISOString(),
    matches,
    standings: standings.slice(0, config.topN > 0 ? Math.min(config.topN, config.maxItems) : config.maxItems),
    caps: { ...config.caps },
  };
  const msg = messageBlock();
  if (msg) payload.message = msg;
  return payload;
}

// The message-mode wire block, or null when it's off or blank — the overlay
// falls back to results whenever the key is absent. Results keep flowing in the
// same payload underneath, so toggling the message off restores them instantly.
function messageBlock() {
  if (!config.message.enabled) return null;
  const lines = config.message.text
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean)
    .slice(0, MESSAGE_MAX_LINES);
  return lines.length ? { lines } : null;
}

// A results-free payload, for pushing a message when no poll has ever run (the
// message-only use case: no tournament configured at all).
function blankPayload() {
  return {
    type: 'ticker:update',
    tournament: { name: '', platform: platformName, url: config.tournamentUrl, state: 'in_progress' },
    generatedAt: new Date().toISOString(),
    matches: [],
    standings: [],
    caps: { ...config.caps },
  };
}

// Re-push the current strip state NOW, restamped with the live message + caps.
// Config-only changes have to reach the overlay even when polling is off, so
// they can't wait for the next poll to carry them.
function pushCurrent() {
  const payload = {
    ...(lastPayload || blankPayload()),
    generatedAt: new Date().toISOString(),
    caps: { ...config.caps },
  };
  const msg = messageBlock();
  if (msg) payload.message = msg;
  else delete payload.message;
  lastPayload = payload;
  lastHash = hashPayload(payload); // pushUpdate resets this to null if SB is down
  pushUpdate(payload);
}

// Change detection: only what the strip renders — ids, states, scores, caps,
// message.
function hashPayload(p) {
  return JSON.stringify([
    p.tournament?.name ?? '',
    p.caps,
    p.message ?? null,
    p.matches.map((m) => [m.id, m.state, m.p1?.score, m.p2?.score, m.p1?.name, m.p2?.name]),
    p.standings,
  ]);
}

// ── StreamElements milestone poller ───────────────────────────────────────────
// Same recipe as the live-verified Midnight Velvet goal meter: poll
// kappa/v2/sessions/<channel> (channel decoded from the JWT), read the session
// counters, and fire a "Ticker Announce" (kind milestone) whenever a counter
// crosses its configured step. The first successful poll only RECORDS the
// current thresholds — no announcement blast when you first configure it.

const SE_BASE = process.env.ZIPPER_SE_BASE || 'https://api.streamelements.com';

const SE_METRICS = {
  followers: { keys: ['follower-total'], text: (n) => `${n} followers — thank you!` },
  subs: { keys: ['subscriber-total', 'subscriber-count'], text: (n) => `${n} subs — thank you!` },
  tips: { keys: ['tip-total'], text: (n) => `$${n} in tips — thank you!` },
  cheers: { keys: ['cheer-total'], text: (n) => `${n} bits — thank you!` },
};

let seTimer = null;
let seLastPollAt = null;
let seLastError = '';

function seChannelFromJwt(jwt) {
  try {
    let s = String(jwt).split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    while (s.length % 4) s += '=';
    return JSON.parse(Buffer.from(s, 'base64').toString('utf-8')).channel || null;
  } catch { return null; }
}

function seConfigured() {
  return !!config.streamElements.jwtToken
    && Object.values(config.milestones).some((step) => step > 0);
}

function startSePolling() {
  if (seTimer) { clearInterval(seTimer); seTimer = null; }
  if (!seConfigured()) return;
  const channel = seChannelFromJwt(config.streamElements.jwtToken);
  if (!channel) { seLastError = 'Could not read the channel id from the StreamElements JWT'; log(seLastError); return; }
  const run = () => pollSe(channel).catch((e) => { seLastError = e?.message || String(e); log(`SE poll failed: ${seLastError}`); });
  run();
  seTimer = setInterval(run, config.streamElements.pollSeconds * 1000);
  log(`StreamElements milestone polling every ${config.streamElements.pollSeconds}s`);
}

// Session counters come as numbers or { count | amount | value } objects.
function seCounterValue(entry) {
  if (entry == null) return null;
  if (typeof entry === 'number') return entry;
  const v = entry.count ?? entry.amount ?? entry.value;
  return v != null && Number.isFinite(Number(v)) ? Number(v) : null;
}

async function pollSe(channel) {
  const res = await fetch(`${SE_BASE}/kappa/v2/sessions/${channel}`, {
    headers: { Authorization: `Bearer ${config.streamElements.jwtToken}`, Accept: 'application/json' },
  });
  if (!res.ok) {
    throw new Error(`StreamElements ${res.status}${res.status === 401 ? ' — JWT invalid or expired' : ''}`);
  }
  const j = await res.json();
  const data = j?.data || j || {};
  seLastPollAt = new Date().toISOString();
  seLastError = '';

  let stateChanged = false;
  for (const [metric, def] of Object.entries(SE_METRICS)) {
    const step = config.milestones[metric];
    if (!step) continue;
    let value = null;
    for (const key of def.keys) {
      value = seCounterValue(data[key]);
      if (value != null) break;
    }
    if (value == null) continue;
    const threshold = Math.floor(value / step) * step;
    const prev = config.milestoneState[metric];
    if (prev === undefined) {
      // First sighting: record silently so configuring Zipper mid-stream
      // doesn't announce every historical milestone at once.
      config.milestoneState[metric] = threshold;
      stateChanged = true;
    } else if (threshold > prev) {
      config.milestoneState[metric] = threshold;
      stateChanged = true;
      const text = def.text(threshold);
      log(`milestone: ${metric} crossed ${threshold} → announcing`);
      doAction('Ticker Announce', { kind: 'milestone', text, duration: '10' });
    }
  }
  if (stateChanged) await persist();
}

// ── Streamer.bot WS client ────────────────────────────────────────────────────

function connectSb() {
  if (sbWs && sbWs.readyState === WebSocket.OPEN) return;
  try {
    sbWs = new WebSocket(SB_WS_URL);
    sbWs.on('open', () => {
      if (sbReconnectTimer) { clearTimeout(sbReconnectTimer); sbReconnectTimer = null; }
      // Lowercase `general` — the SB Subscribe case gotcha (delivered events carry
      // capitalized 'General'; a capitalized Subscribe silently receives nothing).
      sbWs.send(JSON.stringify({ request: 'Subscribe', id: String(++sbMsgId), events: { general: ['Custom'] } }));
      log(`Connected to Streamer.bot at ${SB_WS_URL}`);
      pushStatus();
      // A message-only setup never polls, so no other code path would ever put
      // the configured message on screen after a restart.
      if (!polling && messageBlock()) pushCurrent();
    });
    sbWs.on('close', () => { sbWs = null; scheduleSbReconnect(); });
    sbWs.on('error', (e) => { log(`SB WS error: ${e.message} — is SB's WebSocket Server on ${SB_WS_URL} with auth OFF?`); });
    sbWs.on('message', (raw) => {
      let m;
      try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m && m.id && !m.event) return; // request acks
      if (!(m && m.event && m.event.source === 'General' && m.event.type === 'Custom')) return;
      let d = m.data;
      if (typeof d === 'string') { try { d = JSON.parse(d); } catch { return; } }
      // Echo-loop guard: this client also hears its own ticker:update /
      // ticker:status broadcasts. Consume ONLY the command channel.
      if (!d || typeof d !== 'object' || d.type !== 'ticker:command') return;
      dispatch(String(d.command ?? ''), String(d.value ?? ''));
    });
  } catch {
    scheduleSbReconnect();
  }
}

function scheduleSbReconnect() {
  if (sbReconnectTimer) return;
  sbReconnectTimer = setTimeout(() => { sbReconnectTimer = null; connectSb(); }, 3000);
}

function doAction(name, args) {
  if (!sbWs || sbWs.readyState !== WebSocket.OPEN) return false;
  try {
    sbWs.send(JSON.stringify({ request: 'DoAction', id: String(++sbMsgId), action: { name }, args }));
    return true;
  } catch { return false; }
}

// ticker:update rides the "Ticker Push" action so the C# side persists it for
// the sync-on-connect replay.
function pushUpdate(payload) {
  if (!doAction('Ticker Push', { tickerPayload: JSON.stringify(payload) })) {
    // SB is down — force a re-push on the next successful poll after reconnect.
    lastHash = null;
  }
}

// ticker:status is ephemeral (never persisted) — it rides the "Ticker Command"
// relay with the reserved command "__status"; the C# action broadcasts
// { type:'ticker:command', command:'__status', value:'<status JSON>' } and the
// control page unwraps it. dispatch() below ignores it (echo guard).
function pushStatus() {
  const status = {
    type: 'ticker:status',
    polling,
    url: config.tournamentUrl,
    platform: platformName,
    lastPollAt,
    lastError,
    intervalMs: config.pollIntervalMs,
    maxItems: config.maxItems,
    topN: config.topN,
    caps: { ...config.caps },
    message: { ...config.message },
    // StreamElements milestone status — never the token itself.
    se: { configured: seConfigured(), lastPollAt: seLastPollAt, lastError: seLastError },
  };
  doAction('Ticker Command', { command: '__status', value: JSON.stringify(status) });
}

// ── Command dispatch (inbound half of the bus) ────────────────────────────────

// Message edits push straight to the overlay: polling may be off, or there may
// be no tournament at all, so "re-push on the next poll" would never arrive.
function applyMessageChange() {
  persist();
  pushCurrent();
  pushStatus();
}

function dispatch(command, value) {
  if (command === '__status') return; // our own status relay echoing back
  log(`command: ${command}${value ? ' ' + value.slice(0, 120) : ''}`);
  switch (command) {
    case 'setUrl':
      config.tournamentUrl = strOrNull(value);
      lastHash = null;
      platformName = config.tournamentUrl ? (detectPlatform(config.tournamentUrl)?.name
        ?? (config.tournamentUrl.startsWith('stub:') ? 'stub' : null)) : null;
      lastError = '';
      persist();
      if (polling && config.tournamentUrl) schedulePoll(0);
      pushStatus();
      return;
    case 'start': startPolling(); return;
    case 'stop': stopPolling(); return;
    case 'pollNow':
      if (!config.tournamentUrl) { lastError = 'No tournament URL set'; pushStatus(); return; }
      if (!polling) { polling = true; persist(); }
      schedulePoll(0);
      return;
    case 'setInterval':
      config.pollIntervalMs = clampInt(value, MIN_INTERVAL_MS, 3600000, config.pollIntervalMs);
      persist();
      if (polling) schedulePoll(config.pollIntervalMs);
      pushStatus();
      return;
    case 'setCapsText':
      config.caps.text = String(value ?? '').slice(0, 120);
      lastHash = null; // caps travel in the update payload — force a re-push
      persist();
      // Stopped (or message-only, with no tournament) still needs the new caps
      // on screen, so push rather than wait for a poll that may never come.
      if (polling) schedulePoll(0); else { pushCurrent(); pushStatus(); }
      return;
    case 'setCapsLogo':
      config.caps.logo = String(value ?? '').slice(0, 500);
      lastHash = null;
      persist();
      if (polling) schedulePoll(0); else { pushCurrent(); pushStatus(); }
      return;
    case 'setTopN':
      config.topN = clampInt(value, 0, 100, config.topN);
      lastHash = null;
      persist();
      if (polling) schedulePoll(0); else pushStatus();
      return;
    case 'setMaxItems':
      config.maxItems = clampInt(value, 1, 100, config.maxItems);
      lastHash = null;
      persist();
      if (polling) schedulePoll(0); else pushStatus();
      return;
    // Batched setters — the control page sends ONE of these per button so it
    // never fires a burst of DoActions (Streamer.bot bleeds arguments between
    // action invocations that arrive within a few ms, which corrupted the caps
    // text). Value is a JSON object. The per-field commands above stay for chat
    // / Stream Deck, where one action fires at a time.
    case 'setCaps': {
      const o = parseJson(value);
      if (o && typeof o === 'object') {
        if (typeof o.text === 'string') config.caps.text = o.text.slice(0, 120);
        if (typeof o.logo === 'string') config.caps.logo = o.logo.slice(0, 500);
      }
      lastHash = null; // caps travel in the update payload — force a re-push
      persist();
      if (polling) schedulePoll(0); else { pushCurrent(); pushStatus(); }
      return;
    }
    // Persistent message mode — batched (text + enabled in one command) for the
    // control page; the single-field variants below are for chat / Stream Deck.
    case 'setMessage': {
      const o = parseJson(value);
      if (o && typeof o === 'object') {
        if (typeof o.text === 'string') config.message.text = o.text.slice(0, MESSAGE_MAX_CHARS);
        if (o.enabled !== undefined) config.message.enabled = truthy(o.enabled);
      }
      applyMessageChange();
      return;
    }
    case 'setMessageText':
      config.message.text = String(value ?? '').slice(0, MESSAGE_MAX_CHARS);
      applyMessageChange();
      return;
    case 'setMessageEnabled':
      config.message.enabled = truthy(value);
      applyMessageChange();
      return;
    case 'setOptions': {
      const o = parseJson(value);
      if (o && typeof o === 'object') {
        if (o.intervalMs !== undefined) config.pollIntervalMs = clampInt(o.intervalMs, MIN_INTERVAL_MS, 3600000, config.pollIntervalMs);
        if (o.maxItems !== undefined) config.maxItems = clampInt(o.maxItems, 1, 100, config.maxItems);
        if (o.topN !== undefined) config.topN = clampInt(o.topN, 0, 100, config.topN);
      }
      lastHash = null;
      persist();
      if (polling) schedulePoll(0); else pushStatus();
      return;
    }
    case 'status': pushStatus(); return;
    default:
      log(`unknown command "${command}" — ignored`);
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────

function log(msg) {
  console.log(`[${new Date().toISOString()}] [zipper] ${msg}`);
}

function acquireGuard() {
  return new Promise((res) => {
    const guard = createServer();
    guard.once('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        log(`Another Zipper sidecar is already running (guard port :${GUARD_PORT}) — exiting.`);
        process.exit(0);
      }
      log(`guard port error: ${err.message} — continuing without the single-instance guard`);
      res();
    });
    guard.listen(GUARD_PORT, '127.0.0.1', () => res());
  });
}

async function main() {
  log(`Zipper sidecar starting (config: ${CONFIG_PATH})`);
  await acquireGuard();
  await loadConfig();
  connectSb();
  startSePolling();
  if (config.autoStart && config.tournamentUrl) startPolling();
  const shutdown = () => {
    log('shutting down');
    if (pollTimer) clearTimeout(pollTimer);
    if (seTimer) clearInterval(seTimer);
    try { sbWs?.close(); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main();
