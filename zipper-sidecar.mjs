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
// Commands: setUrl, start, stop, pollNow, setInterval, setCaps, setCapsText,
//           setCapsLogo, setOptions, setMaxItems, setTopN, setMessage,
//           setMessageText, setMessageEnabled, setAutoSchedule, status.
//           Names match without regard to case. An unknown or refused command is
//           reported in the status's commandNotice (docs/PROTOCOL.md).
//
// Auto-scheduling (config.autoSchedule, default ON): API calls are only spent
// while the event is actually running. A poll that comes back `completed`
// pushes the final payload and then stops polling; one that comes back
// `upcoming` sleeps until the platform's advertised start time (slow re-check
// when it has none) and resumes normal polling once the event goes live.
//
// Config lives in ./config.json (gitignored; seeded from config.example.json on
// first run). API keys may also come from env: CHALLONGE_API_KEY, STARTGG_API_KEY.
//
// Test hook: a tournament URL of the form "stub:<path-to-json>" loads a
// normalized platform result from that file instead of the network — used by
// verify.mjs and handy for offline overlay styling. It works only when the
// sidecar is started with ZIPPER_ALLOW_STUB=1 (the verify suite sets it), and only
// for a file inside this folder: the path is checked as text before any file is
// opened (see stubRefusal). Otherwise setUrl refuses it and a poll of one saved in
// config.json fails, each with a fixed message that quotes no file. With the
// variable, such a file can also hold its poll open: see stubHold.
//
// Run:  start-zipper.bat  ·  npm run sidecar
// Env:  SB_WS_URL (default ws://127.0.0.1:8080/) · ZIPPER_GUARD_PORT (7496)
//       ZIPPER_CONFIG (config file path override)
//       ZIPPER_ALLOW_STUB=1 (tests only: turns on stub: URLs and the stub hold)

import { readFile, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { createServer } from 'node:net';
import { resolve, relative, isAbsolute, dirname, sep } from 'node:path';
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
// Set by the verify suite. Without it a stub: URL is refused and nothing is read.
const STUB_ALLOWED = process.env.ZIPPER_ALLOW_STUB === '1';

// Fixed messages: none quotes a file's content, a path or a value sent, because the
// status that carries them reaches every page.
const NO_URL_MESSAGE = 'No tournament URL set';
const STUB_OFF_MESSAGE = 'stub: URLs are for the tests and work only with ZIPPER_ALLOW_STUB=1';
const STUB_OUTSIDE_MESSAGE = 'stub: URLs may name only a file inside the Zipper folder';
const STUB_UNREADABLE_MESSAGE = 'stub: the test file could not be read';
const STUB_NOT_OBJECT_MESSAGE = 'stub: the test file is not a JSON object';
const LOGO_REFUSED_MESSAGE = 'Logo not changed: use an http(s) URL or a path on the overlay\'s server, such as /zipper-overlay/assets/logo.png';
// An unknown command is reported by its name only, cut to this many characters (the
// longest real name, setMessageEnabled, has 17). The name can come from chat or any
// local program, so nothing else limits its length.
const COMMAND_NAME_MAX = 40;
const LOGO_MAX = 500;
// What a relative logo is resolved against to check it: the overlay's address on
// Streamer.bot's HTTP server. Only its host matters (a relative logo must stay on it).
const OVERLAY_BASE = 'http://127.0.0.1:7474/zipper-overlay/';

const MIN_INTERVAL_MS = 15000;
const MAX_BACKOFF_MS = 300000; // 5 min cap when the platform keeps erroring
const MESSAGE_MAX_CHARS = 1000;
const MESSAGE_MAX_LINES = 20;
const WAITING_POLL_MS = 300000; // 5 min re-check while upcoming with no usable start time
const MAX_WAIT_MS = 6 * 3600000; // longest single sleep while waiting on a start time

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
  // Auto-scheduling: stop polling when the event completes; while it hasn't
  // started, sleep until the platform's start time instead of burning API calls.
  autoSchedule: true,
  autoStart: false,
};

let polling = false;
let pollTimer = null;
let pollInFlight = false;
let consecutiveErrors = 0;
let lastError = ''; // poll failures only
// An unknown or refused command (docs/PROTOCOL.md). The next accepted command clears
// it; `status`, which the control page sends every 10 seconds, does not.
let commandNotice = '';
let lastPollAt = null;
let lastHash = null;
let lastPayload = null; // last payload built, so config-only changes (message,
                        // caps) can re-push without waiting for a poll
let platformName = null;
let eventState = null; // last fetched tournament state (upcoming/in_progress/completed)
let eventStartAt = null; // platform's advertised start time (ISO), when it has one
let waitingUntil = null; // set while auto-scheduling sleeps for the start time
let autoStopped = null; // 'completed' after an automatic stop, for the status line

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
  // A saved logo that fails the rule is dropped here, and written out on the next save.
  let logo = checkLogo(parsed.caps?.logo);
  if (logo === null) {
    logo = '';
    log('config.json: the saved logo was dropped (it must be an http(s) URL or a path on the overlay\'s server)');
  }
  config = {
    challongeApiKey: strOrNull(parsed.challongeApiKey) || strOrNull(process.env.CHALLONGE_API_KEY),
    startggApiKey: strOrNull(parsed.startggApiKey) || strOrNull(process.env.STARTGG_API_KEY),
    tournamentUrl: strOrNull(parsed.tournamentUrl),
    pollIntervalMs: clampInt(parsed.pollIntervalMs, MIN_INTERVAL_MS, 3600000, 30000),
    maxItems: clampInt(parsed.maxItems, 1, 100, 20),
    topN: clampInt(parsed.topN, 0, 100, 0),
    caps: {
      text: String(parsed.caps?.text ?? '').slice(0, 120),
      logo,
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
    autoSchedule: parsed.autoSchedule === undefined ? true : !!parsed.autoSchedule,
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

// The logo (ZIP-01), on every way in: setCapsLogo, setCaps and config.json. Returns the
// trimmed value to keep ('' clears the cap), or null when it is refused. Accepted: an
// explicit http: or https: URL (any host), or a value that, resolved against the
// overlay's address, stays http: on the overlay's own host: a path on its server.
// Everything else is refused: data:, file:, javascript:, blob:, and //host, \\host or
// /\host, which are not paths on that server (under a file:// overlay they would name
// a network share). The URL parser drops tabs and newlines, so their variants resolve
// the same way and are refused too.
function checkLogo(v) {
  const s = String(v ?? '').trim().slice(0, LOGO_MAX);
  if (!s) return '';
  try {
    if (/^https?:/i.test(s)) {
      const u = new URL(s);
      return u.protocol === 'http:' || u.protocol === 'https:' ? s : null;
    }
    const u = new URL(s, OVERLAY_BASE);
    return u.protocol === 'http:' && u.host === new URL(OVERLAY_BASE).host ? s : null;
  } catch {
    return null;
  }
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
  if (!config.tournamentUrl) { refuse(NO_URL_MESSAGE); return; }
  polling = true;
  autoStopped = null; // manual (re)start always gets one fresh poll
  persist();
  schedulePoll(0);
}

function stopPolling() {
  polling = false;
  waitingUntil = null;
  autoStopped = null;
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

// Auto-scheduling: spend API calls only while the event is actually running.
// completed → stop polling outright (the final payload just pushed stays on
// screen); upcoming → sleep until the platform's start time (slow re-check when
// it has none), so polling resumes by itself once the event goes live. Errors
// keep the normal backoff — a failed fetch says nothing about the event state.
function nextDelayAuto() {
  waitingUntil = null;
  if (!config.autoSchedule || consecutiveErrors) return nextDelay();
  if (eventState === 'completed') {
    polling = false;
    autoStopped = 'completed';
    persist();
    log('event completed — polling auto-stopped');
    return 0; // unused: schedulePoll is a no-op once polling is false
  }
  if (eventState === 'upcoming') {
    const untilStart = eventStartAt ? Date.parse(eventStartAt) - Date.now() : NaN;
    const hasStart = Number.isFinite(untilStart) && untilStart > 0;
    // MAX_WAIT_MS chunks a far-off start into cheap once-per-6h re-checks (also
    // catches organisers moving the date). No/passed start time → slow re-check.
    const wait = hasStart ? Math.min(untilStart, MAX_WAIT_MS) : Math.max(WAITING_POLL_MS, config.pollIntervalMs);
    waitingUntil = new Date(Date.now() + wait).toISOString();
    log(`event not started — ${hasStart ? `sleeping until ${waitingUntil} (start ${eventStartAt})` : `re-checking in ${Math.round(wait / 60000)} min (no start time from the platform)`}`);
    return wait;
  }
  return nextDelay();
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
    eventState = result.state || 'in_progress';
    eventStartAt = strOrNull(result.startAt);
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
    const delay = nextDelayAuto(); // may auto-stop; must run before pushStatus
    pushStatus();
    schedulePoll(delay);
  }
}

async function fetchCurrent() {
  const url = config.tournamentUrl;
  if (!url) throw new Error(NO_URL_MESSAGE); // ZIP-12: a plain message, never a TypeError
  // Test hook / offline styling: stub:<path> loads a normalized result file. The gate
  // runs first, so a URL from config.json that fails it reads nothing.
  if (url.startsWith('stub:')) {
    const refusal = stubRefusal(url);
    if (refusal) throw new Error(refusal);
    platformName = 'stub';
    let raw;
    try { raw = await readFile(resolve(__dirname, url.slice(5)), 'utf-8'); }
    catch { throw new Error(STUB_UNREADABLE_MESSAGE); }
    // The parse error's own text quotes the file, so it is never passed on.
    let result = null;
    try { result = JSON.parse(raw); } catch {}
    if (!result || typeof result !== 'object' || Array.isArray(result)) throw new Error(STUB_NOT_OBJECT_MESSAGE);
    await stubHold(result); // no abort signal yet; the poll's deadline is passed here later
    return result;
  }
  const platform = detectPlatform(url);
  if (!platform) throw new Error(`Unrecognized tournament URL: ${url}`);
  platformName = platform.name;
  const id = platform.parseId(url);
  if (!id) throw new Error(`${platform.displayName}: could not parse a tournament id from that URL`);
  return platform.fetchTicker(id, config, url);
}

// The stub: gate (decision 2). Returns null when the URL may be read, or the fixed
// message that refuses it. Without ZIPPER_ALLOW_STUB=1 nothing is allowed. With it,
// only a path inside this folder, and that is checked as TEXT: path.resolve and
// path.relative touch no file, so nothing is opened, stat'ed or looked up first. (An
// open, stat or realpath of \\host\share would itself be the outbound file-sharing
// connection.)
// The resolved path must begin with this folder's path exactly, letter case included,
// followed by a separator. path.relative alone is not enough on Windows: it drops the
// leading \\ of a network path and folds case with JavaScript's toLowerCase, so it
// calls \\C:\<this folder>\x.json (a share on a server named "C:"), its // form, or a
// sibling folder spelled with a Kelvin sign (U+212A, which NTFS keeps apart from "k")
// a path inside this folder. The separator keeps out a sibling folder whose name
// starts with this one's. An absolute path inside this folder typed in another letter
// case is refused too; for a test hook that costs nothing.
function stubRefusal(url) {
  if (!STUB_ALLOWED) return STUB_OFF_MESSAGE;
  const full = resolve(__dirname, url.slice(5));
  if (!full.startsWith(__dirname + sep)) return STUB_OUTSIDE_MESSAGE;
  const rel = relative(__dirname, full);
  if (!rel || rel === '..' || rel.startsWith('..' + sep) || isAbsolute(rel)) return STUB_OUTSIDE_MESSAGE;
  return null;
}

// ── stub hold: BEGIN (verify.mjs runs this block on its own, in-process) ──
// A slow source for the tests. A stub fixture may name a hold file ("stubHoldFile",
// relative to the repo folder). With ZIPPER_ALLOW_STUB=1 the poll then waits while
// that file exists, and goes on as soon as the test deletes it: the wait comes from
// the fixture file only, never from the URL, and has no fixed length. Without the
// variable this does nothing. An AbortSignal, when one is given, ends the wait at
// once with the signal's reason, and the timer is cleared.
const STUB_HOLD_CHECK_MS = 50;
function stubHold(result, signal) {
  const name = result && typeof result.stubHoldFile === 'string' ? result.stubHoldFile : '';
  if (!STUB_ALLOWED || !name) return Promise.resolve();
  const file = resolve(__dirname, name);
  if (!file.startsWith(__dirname + sep)) return Promise.resolve(); // only a hold file inside the repo folder counts
  return new Promise((done, fail) => {
    let timer = null;
    let logged = false;
    const finish = (err) => {
      if (timer !== null) { clearTimeout(timer); timer = null; }
      if (signal) signal.removeEventListener('abort', onAbort);
      if (err) fail(err); else done();
    };
    const onAbort = () => finish(signal.reason ?? new Error('stub hold aborted'));
    if (signal) {
      if (signal.aborted) { onAbort(); return; }
      signal.addEventListener('abort', onAbort, { once: true });
    }
    const look = () => {
      timer = null;
      if (!existsSync(file)) { finish(); return; }
      if (!logged) { logged = true; log(`stub: holding the poll while ${name} exists`); }
      timer = setTimeout(look, STUB_HOLD_CHECK_MS);
    };
    look();
  });
}
// ── stub hold: END ──

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
    commandNotice,
    intervalMs: config.pollIntervalMs,
    maxItems: config.maxItems,
    topN: config.topN,
    caps: { ...config.caps },
    message: { ...config.message },
    // Auto-scheduling snapshot: what the last poll said about the event and
    // what the scheduler is doing about it (docs/PROTOCOL.md).
    auto: {
      enabled: config.autoSchedule,
      eventState,
      startAt: eventStartAt,
      waitingUntil,
      stopped: autoStopped, // 'completed' | null
    },
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

// The commands dispatch() runs, keyed by their names in lower case. A name matches
// without regard to case (ZIP-08): chat lower-cases a dedicated command, so "!pollnow"
// arrives as "pollnow". The cases below keep the camelCase names docs/PROTOCOL.md uses.
const COMMANDS = new Map([
  'setUrl', 'start', 'stop', 'pollNow', 'setInterval', 'setCapsText', 'setCapsLogo',
  'setTopN', 'setMaxItems', 'setCaps', 'setMessage', 'setMessageText',
  'setMessageEnabled', 'setAutoSchedule', 'setOptions', 'status',
].map((name) => [name.toLowerCase(), name]));

// A command name as the log and commandNotice show it: cut to COMMAND_NAME_MAX
// characters (whole characters, so an emoji is never split), never its value.
function shownName(name) {
  const chars = Array.from(name);
  return chars.length > COMMAND_NAME_MAX ? chars.slice(0, COMMAND_NAME_MAX).join('') + '…' : name;
}

// Records why a command was not (fully) carried out. The caller pushes the status.
function notice(text) {
  commandNotice = text;
  log(text);
}
// A command refused outright: nothing changes but the notice, which the status carries.
function refuse(text) {
  notice(text);
  pushStatus();
}

function dispatch(command, value) {
  // Folded first, so our own status relay echoing back is ignored in any casing. Were
  // "__STATUS" reported as unknown, that report would push a status, which echoes.
  const folded = command.toLowerCase();
  if (folded === '__status') return;
  const shown = shownName(command);
  log(`command: ${shown}${value ? ' ' + value.slice(0, 120) : ''}`);
  const name = COMMANDS.get(folded);
  if (!name) { refuse(`unknown command "${shown}" — ignored`); return; }
  if (name !== 'status') commandNotice = ''; // an accepted command clears the last notice
  switch (name) {
    case 'setUrl': {
      const url = strOrNull(value);
      // A refused URL is no URL change: the old one, its platform and its state stay,
      // nothing is saved, and no poll follows.
      const refusal = url && url.startsWith('stub:') ? stubRefusal(url) : null;
      if (refusal) { refuse(refusal); return; }
      config.tournamentUrl = url;
      lastHash = null;
      platformName = config.tournamentUrl ? (detectPlatform(config.tournamentUrl)?.name
        ?? (config.tournamentUrl.startsWith('stub:') ? 'stub' : null)) : null;
      lastError = '';
      eventState = null; // new tournament — forget the old one's state/schedule
      eventStartAt = null;
      autoStopped = null;
      if (!config.tournamentUrl) {
        // ZIP-12: with no URL there is nothing to poll, so clearing it stops polling.
        polling = false;
        waitingUntil = null;
        if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
      }
      persist();
      if (polling) schedulePoll(0);
      pushStatus();
      return;
    }
    case 'start': startPolling(); return;
    case 'stop': stopPolling(); return;
    case 'pollNow':
      if (!config.tournamentUrl) { refuse(NO_URL_MESSAGE); return; }
      if (!polling) { polling = true; autoStopped = null; persist(); }
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
    case 'setCapsLogo': {
      const logo = checkLogo(value);
      if (logo === null) { refuse(LOGO_REFUSED_MESSAGE); return; } // the logo is kept
      config.caps.logo = logo;
      lastHash = null;
      persist();
      if (polling) schedulePoll(0); else { pushCurrent(); pushStatus(); }
      return;
    }
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
        if (typeof o.logo === 'string') {
          // A refused logo keeps the previous one; a valid text still applies.
          const logo = checkLogo(o.logo);
          if (logo === null) notice(LOGO_REFUSED_MESSAGE); else config.caps.logo = logo;
        }
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
    case 'setAutoSchedule':
      config.autoSchedule = truthy(value);
      persist();
      // Turning it off cancels a pending wait-for-start sleep and resumes the
      // normal cadence; turning it on takes effect at the next poll.
      if (polling && !config.autoSchedule) { waitingUntil = null; schedulePoll(nextDelay()); }
      pushStatus();
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
