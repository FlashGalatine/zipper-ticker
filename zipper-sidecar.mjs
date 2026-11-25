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
// The poll loop (see "Poll loop" below): one poll at a time, 45 s at most for a
// poll and 10 s for each request in it (platforms/guarded-fetch.js), at least
// 15 s between the end of one poll and the start of the next whatever asks for
// it, an error backoff that never goes below the interval, and a site's
// Retry-After kept. A result that comes back after setUrl changed the URL is
// thrown away. The status's nextPollAt says when the next poll runs.
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
// variable, such a file can also hold its poll open (see stubHold), keep it held after
// the poll is aborted so its result comes back late (also stubHold), and make its
// poll fail as a rate-limited site would (see stubFailure).
//
// Run:  start-zipper.bat  ·  npm run sidecar
// Env:  SB_WS_URL (default ws://127.0.0.1:8080/) · ZIPPER_GUARD_PORT (7496)
//       ZIPPER_CONFIG (config file path override)
//       ZIPPER_ALLOW_STUB=1 (tests only: turns on stub: URLs and, in a stub file, the
//       test keys stubHoldFile, stubHoldIgnoresAbort and stubFail)
//       ZIPPER_REQUEST_TIMEOUT_MS, ZIPPER_POLL_DEADLINE_MS, ZIPPER_MIN_POLL_GAP_MS
//       (tests only: shorten the 10 s per request, the 45 s per poll and the 15 s
//       between polls; each is clamped, so it can only shorten its limit)

import { readFile, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { createServer } from 'node:net';
import { resolve, relative, isAbsolute, dirname, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';
import { detectPlatform } from './platforms/index.js';
import { guardedFetch, statusError, parseRetryAfter, envLimit, seconds } from './platforms/guarded-fetch.js';

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
// 5 min cap on the error backoff, unless the interval is longer: the backoff never goes
// below the interval (ZIP-13). It also caps a Retry-After, on the same terms.
const MAX_BACKOFF_MS = 300000;
// The whole of one poll: every request in it shares this deadline (a start.gg poll can
// make up to eleven, each with its own 10 s in platforms/guarded-fetch.js). Past it the
// poll is stopped and fails, with a fixed message in lastError.
const POLL_DEADLINE_MS = 45000;
// The least time from the END of one poll to the start of the next, whatever asks for
// it (ZIP-13): a command inside it replies at once and gets its poll when it ends, so a
// burst of commands costs one fetch. 15 s is MIN_INTERVAL_MS, the fastest cadence a
// streamer can set, so no command can make the sidecar poll faster than that either. It
// also keeps a start.gg poll (up to eleven requests) inside start.gg's 80 requests a
// minute. Counted from the end, so a slow poll is never followed straight by another.
const MIN_POLL_GAP_MS = 15000;
// The tests shorten these two (and the 10 s per request) through ZIPPER_* settings.
const pollDeadlineMs = envLimit('ZIPPER_POLL_DEADLINE_MS', POLL_DEADLINE_MS, 50);
const minPollGapMs = envLimit('ZIPPER_MIN_POLL_GAP_MS', MIN_POLL_GAP_MS, 0);
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
let urlGeneration = 0; // ZIP-02: bumped by every accepted setUrl; a poll keeps the one it began under
let pollAgain = false; // a pollNow or start that arrived while a poll was in flight
let pollAbort = null; // the AbortController of the poll in flight
// The three times below are on the monotonic clock (monoNow), never the wall clock.
let lastPollEndedAt = null; // when the last poll ended; the minimum gap runs from here
let pollDueAt = null; // when the armed poll runs (the status's nextPollAt)
let pollDueFor = null; // 'request' (a command asked for it) or 'regular'
// Retry-After holds, by platform: no poll of that platform before the time kept for it.
// A hold is the platform's, not the URL's, so a setUrl neither ends nor moves it.
const retryHolds = new Map();

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
//
// How polls are timed (ZIP-02, ZIP-06, ZIP-13):
//   • One poll at a time, with pollDeadlineMs (45 s) in all. Its AbortSignal reaches
//     every request, through the adapters to platforms/guarded-fetch.js, and the stub
//     hold, so a poll that overruns stops as a whole and fails with a fixed message.
//   • A poll's result belongs to the URL it began under. setUrl bumps urlGeneration and
//     aborts the poll in flight; whatever that poll then brings back, result or failure,
//     is thrown away (lastError, the error count and lastPollAt are left alone), and the
//     new URL is polled instead.
//   • No poll starts within minPollGapMs (15 s) of the end of the last one, whatever
//     asks for it. A command inside the gap gets its status at once and its poll when
//     the gap ends, so a burst of commands costs one fetch. A pollNow or start that
//     arrives while a poll is in flight is kept, and runs after it.
//   • The regular next poll comes from one function, nextRegularPoll: the interval, the
//     error backoff (never below the interval), or auto-scheduling's wait for a start.
//   • A Retry-After from the site (a 429 or 503) holds back every poll of that platform
//     until it ends: capped at MAX_BACKOFF_MS, or at the interval when that is longer.
//     No command cuts it short, pollNow and setUrl included: the hold is the platform's,
//     so a URL on another platform is polled as usual, and a URL on the held platform
//     (set again, set back, or set after the URL was cleared) still waits.
//   • The status's nextPollAt is when the armed poll runs; null when none is armed.
//   • All of this is timed on the monotonic clock, so a system clock set back (by hand,
//     by a time sync, after sleep) cannot hold polls back by the size of the step. The
//     wall clock is read only to show a time in the status and to read an event's start.

const monoNow = () => performance.now();
let wallOffset = Date.now() - performance.now();
// A monotonic time as an ISO wall time, for the status. The offset between the two
// clocks is taken again only when it has moved by more than a second (the wall clock
// was set), so the same moment reads the same in every status.
function wallTime(mono) {
  const fresh = Date.now() - performance.now();
  if (Math.abs(fresh - wallOffset) > 1000) wallOffset = fresh;
  return new Date(Math.round(mono + wallOffset)).toISOString();
}

// The platform a URL polls on, as the status shows it: a stub: URL only when the gate
// lets it through, so a refused one is never labelled "stub".
function platformFor(url) {
  if (!url) return null;
  if (url.startsWith('stub:')) return stubRefusal(url) ? null : 'stub';
  return detectPlatform(url)?.name ?? null;
}

function startPolling() {
  if (!config.tournamentUrl) { refuse(NO_URL_MESSAGE); return; }
  polling = true;
  autoStopped = null; // manual (re)start always gets one fresh poll
  persist();
  requestPoll(true);
  pushStatus();
}

function stopPolling() {
  polling = false;
  waitingUntil = null;
  autoStopped = null;
  pollAgain = false;
  disarmPoll();
  persist();
  pushStatus();
}

function disarmPoll() {
  if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
  pollDueAt = null;
  pollDueFor = null;
}

// The end of a Retry-After hold on the platform of the URL now set, or 0.
function holdUntil() {
  const platform = platformFor(config.tournamentUrl);
  const until = platform ? retryHolds.get(platform) : undefined;
  if (until === undefined) return 0;
  if (until <= monoNow()) { retryHolds.delete(platform); return 0; }
  return until;
}

// The end of the minimum gap after the last poll, or 0 before the first.
function gapEnd() {
  return lastPollEndedAt === null ? 0 : lastPollEndedAt + minPollGapMs;
}

// Arms the poll timer for `at` (monotonic ms), moved past the minimum gap and any
// Retry-After hold. Nothing is armed while polling is off or a poll is in flight: the
// end of that poll arms the next one.
function armPoll(at, why) {
  if (!polling || pollInFlight) return;
  const now = monoNow();
  const when = Math.max(at, gapEnd(), holdUntil(), now);
  if (pollTimer) clearTimeout(pollTimer);
  pollDueAt = when;
  pollDueFor = why;
  pollTimer = setTimeout(() => { pollTimer = null; pollDueAt = null; pollDueFor = null; pollOnce(); }, when - now);
}

// A command asks for a poll: pollNow, start and setUrl (deliberate), and the option
// commands, which re-poll to rebuild the payload. It runs as soon as the gap and any
// Retry-After hold allow, skipping the interval and the error backoff; a poll already
// armed for sooner stands. A deliberate request that arrives while a poll is in flight
// is kept for after it. An option command's is not: the poll in flight builds its
// payload with the new options. (Row 4.3 replaces the option re-poll with a rebuild from
// the last result.) The caller sends the status.
function requestPoll(deliberate) {
  if (!polling) return;
  if (pollInFlight) { if (deliberate) pollAgain = true; return; }
  waitingUntil = null; // a poll is coming before any start-time wait would end
  const hold = holdUntil();
  const when = Math.max(monoNow(), gapEnd(), hold);
  // A poll already armed for no later stands, so a burst costs one poll. One armed before
  // the hold of the URL now set was armed for another platform's URL (setUrl): it moves.
  if (pollTimer && pollDueAt <= when && pollDueAt >= hold) { pollDueFor = 'request'; return; }
  armPoll(when, 'request');
}

function nextDelay() {
  if (!consecutiveErrors) return config.pollIntervalMs;
  // Exponential backoff on consecutive failures, capped at 5 minutes, and never below
  // the interval (ZIP-13): with a 10-minute or one-hour interval the cap alone would
  // make a failing site be polled MORE often.
  return Math.max(config.pollIntervalMs,
    Math.min(config.pollIntervalMs * 2 ** Math.min(consecutiveErrors, 5), MAX_BACKOFF_MS));
}

// The one next-delay function: when the next regular poll is due, counted from the end
// of the last poll, and whether that is auto-scheduling's wait for the event's start.
// Auto-scheduling spends API calls only while the event is running: upcoming → sleep
// until the platform's start time (a slow re-check when it has none), so polling
// resumes by itself once the event goes live. Errors keep the normal backoff, since a
// failed fetch says nothing about the event state. (A completed event stops polling
// outright: see afterPoll.)
function nextRegularPoll() {
  const now = monoNow();
  const from = lastPollEndedAt ?? now;
  if (config.autoSchedule && !consecutiveErrors && eventState === 'upcoming') {
    // The advertised start (a wall time), moved onto the monotonic clock.
    const startsAt = eventStartAt ? now + (Date.parse(eventStartAt) - Date.now()) : NaN;
    const untilStart = startsAt - from;
    const hasStart = Number.isFinite(untilStart) && untilStart > 0;
    // MAX_WAIT_MS chunks a far-off start into cheap once-per-6h re-checks (also
    // catches organisers moving the date). No/passed start time → slow re-check.
    const wait = hasStart ? Math.min(untilStart, MAX_WAIT_MS) : Math.max(WAITING_POLL_MS, config.pollIntervalMs);
    return { at: from + wait, waiting: true, hasStart };
  }
  return { at: from + nextDelay(), waiting: false, hasStart: false };
}

// Arms the next regular poll: after a poll, and when setInterval, setOptions or
// setAutoSchedule changes what it would be. A poll a command asked for stands.
function scheduleRegular() {
  if (!polling || pollInFlight || pollDueFor === 'request') return null;
  const next = nextRegularPoll();
  armPoll(next.at, 'regular');
  waitingUntil = next.waiting && pollDueAt !== null ? wallTime(pollDueAt) : null;
  return next;
}

// After a poll that was not thrown away: a completed event stops polling outright (the
// final payload just pushed stays on screen); otherwise the next regular poll is armed.
function afterPoll() {
  waitingUntil = null;
  if (config.autoSchedule && !consecutiveErrors && eventState === 'completed') {
    polling = false;
    autoStopped = 'completed';
    persist();
    log('event completed — polling auto-stopped');
    return;
  }
  const next = scheduleRegular();
  if (next?.waiting) {
    log(`event not started — ${next.hasStart ? `sleeping until ${waitingUntil} (start ${eventStartAt})` : `re-checking in ${Math.round((pollDueAt - monoNow()) / 60000)} min (no start time from the platform)`}`);
  }
}

async function pollOnce() {
  if (pollInFlight || !polling) return;
  pollInFlight = true;
  // What this poll is for, taken before the fetch: a setUrl during it changes these.
  const gen = urlGeneration;
  const url = config.tournamentUrl;
  const platform = platformFor(url);
  const ac = new AbortController();
  pollAbort = ac;
  const deadline = setTimeout(() => ac.abort(new Error(`the poll took longer than ${seconds(pollDeadlineMs)} and was stopped`)), pollDeadlineMs);
  let stale = false;
  try {
    const result = await fetchCurrent(url, ac.signal);
    stale = gen !== urlGeneration;
    if (stale) {
      log('a poll of the previous tournament URL came back after setUrl; its result is thrown away');
    } else {
      platformName = platform;
      consecutiveErrors = 0;
      lastError = '';
      lastPollAt = new Date().toISOString();
      if (platform) retryHolds.delete(platform);
      eventState = result.state || 'in_progress';
      eventStartAt = strOrNull(result.startAt);
      const payload = buildPayload(result, url, platform);
      lastPayload = payload;
      const hash = hashPayload(payload);
      if (hash !== lastHash) {
        lastHash = hash;
        pushUpdate(payload);
        log(`pushed update (${payload.matches.length} matches, ${payload.standings.length} standings)`);
      }
    }
  } catch (err) {
    stale = gen !== urlGeneration;
    // A Retry-After is kept even from a poll that is thrown away: the site's limit is the
    // platform's, not the URL's, and it applies whenever a URL on that platform is set.
    if (Number.isFinite(err?.retryAfterMs) && platform) {
      retryHolds.set(platform, monoNow() + Math.min(err.retryAfterMs, Math.max(MAX_BACKOFF_MS, config.pollIntervalMs)));
    }
    if (stale) {
      log('a poll of the previous tournament URL failed after setUrl; its error is thrown away');
    } else {
      platformName = platform;
      consecutiveErrors++;
      lastError = err?.message || String(err);
      lastPollAt = new Date().toISOString();
      log(`poll failed (${consecutiveErrors}x): ${lastError}`);
    }
  } finally {
    clearTimeout(deadline);
    pollAbort = null;
    pollInFlight = false;
    lastPollEndedAt = monoNow();
    if (stale || pollAgain) {
      // The new URL, or the poll a command asked for while this one ran: next, once the
      // gap allows. Auto-scheduling decides after that poll.
      pollAgain = false;
      requestPoll(true);
    } else {
      afterPoll(); // may auto-stop; runs before the status so the status shows it
    }
    pushStatus();
  }
}

// Fetches the normalized result for `url`, with the poll's AbortSignal.
async function fetchCurrent(url, signal) {
  if (!url) throw new Error(NO_URL_MESSAGE); // ZIP-12: a plain message, never a TypeError
  // Test hook / offline styling: stub:<path> loads a normalized result file. The gate
  // runs first, so a URL from config.json that fails it reads nothing.
  if (url.startsWith('stub:')) {
    const refusal = stubRefusal(url);
    if (refusal) throw new Error(refusal);
    let raw;
    try { raw = await readFile(resolve(__dirname, url.slice(5)), 'utf-8'); }
    catch { throw new Error(STUB_UNREADABLE_MESSAGE); }
    // The parse error's own text quotes the file, so it is never passed on.
    let result = null;
    try { result = JSON.parse(raw); } catch {}
    if (!result || typeof result !== 'object' || Array.isArray(result)) throw new Error(STUB_NOT_OBJECT_MESSAGE);
    await stubHold(result, signal); // the poll's deadline, or a setUrl, ends the wait (unless stubHoldIgnoresAbort)
    stubFailure(result);
    return result;
  }
  const platform = detectPlatform(url);
  if (!platform) throw new Error(`Unrecognized tournament URL: ${url}`);
  const id = platform.parseId(url);
  if (!id) throw new Error(`${platform.displayName}: could not parse a tournament id from that URL`);
  return platform.fetchTicker(id, config, url, signal);
}

// The other half of the slow source: a stub fixture may make its poll fail as a site
// would, with an HTTP status and a Retry-After ("stubFail": { "status": 429,
// "retryAfter": "120" }), so the tests reach the Retry-After hold through the real poll
// loop. Only a stub: URL reaches this, so only with ZIPPER_ALLOW_STUB=1. The message is
// fixed but for the status number, and quotes nothing else from the file.
function stubFailure(result) {
  const f = result.stubFail;
  if (!f || typeof f !== 'object') return;
  const status = clampInt(f.status, 400, 599, 500);
  throw statusError(`stub: the test file answered as HTTP ${status}`, {
    status,
    retryAfterMs: status === 429 || status === 503 ? parseRetryAfter(f.retryAfter) : null,
  });
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
// A fixture may also set "stubHoldIgnoresAbort": true. The wait then does not end when
// the poll is aborted (by setUrl, or by the deadline), so the poll's result, or its
// stubFail, comes back after setUrl has moved on, as an answer already on its way
// would; that is how the tests reach pollOnce's check for a late result. It is bounded
// all the same: a timer of pollDeadlineMs, started with the hold, ends the wait, failing
// with the signal's reason. The poll's own deadline timer started earlier, so a held
// poll still ends at its deadline, give or take the time the fixture took to read.
// The key does nothing without a hold file, and so nothing without the variable.
const STUB_HOLD_CHECK_MS = 50;
function stubHold(result, signal) {
  const name = result && typeof result.stubHoldFile === 'string' ? result.stubHoldFile : '';
  if (!STUB_ALLOWED || !name) return Promise.resolve();
  const file = resolve(__dirname, name);
  if (!file.startsWith(__dirname + sep)) return Promise.resolve(); // only a hold file inside the repo folder counts
  const ignoreAbort = result.stubHoldIgnoresAbort === true;
  return new Promise((done, fail) => {
    let timer = null;
    let bound = null;
    let logged = false;
    const finish = (err) => {
      if (timer !== null) { clearTimeout(timer); timer = null; }
      if (bound !== null) { clearTimeout(bound); bound = null; }
      if (signal) signal.removeEventListener('abort', onAbort);
      if (err) fail(err); else done();
    };
    const onAbort = () => finish(signal.reason ?? new Error('stub hold aborted'));
    if (ignoreAbort) {
      bound = setTimeout(() => {
        bound = null;
        finish(signal?.reason ?? new Error('stub hold: the poll\'s deadline passed'));
      }, pollDeadlineMs);
    } else if (signal) {
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

// Normalize + sort + cap into the ticker:update wire payload (docs/PROTOCOL.md). The URL
// and platform are the ones the poll was made for, never the ones set since (ZIP-02).
function buildPayload(result, url, platform) {
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
      platform,
      url,
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

// Through the fetch wrapper (ZIP-06), so a StreamElements that stops answering fails
// this poll after 10 s, naming the host, instead of holding it for minutes. (The poller
// still runs on setInterval; row 4.5 chains its polls with an in-flight guard.)
async function pollSe(channel) {
  const res = await guardedFetch(`${SE_BASE}/kappa/v2/sessions/${channel}`, {
    headers: { Authorization: `Bearer ${config.streamElements.jwtToken}`, Accept: 'application/json' },
  });
  if (!res.ok) {
    throw new Error(`StreamElements ${res.status}${res.status === 401 ? ' — JWT invalid or expired' : ''}`);
  }
  const j = res.json();
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
    // When the next poll runs (ZIP-13): the interval, the backoff, a Retry-After or the
    // minimum gap, whichever is latest; null while none is armed (polling off, or a poll
    // in flight). Unlike auto.waitingUntil it does not mean the page should show WAITING.
    nextPollAt: pollDueAt !== null ? wallTime(pollDueAt) : null,
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
      // ZIP-02: a poll in flight now belongs to the old URL. It is stopped, and whatever
      // it brings back is thrown away; the new URL is polled once the gap allows.
      urlGeneration++;
      if (pollAbort) pollAbort.abort(new Error('the tournament URL changed'));
      lastHash = null;
      platformName = platformFor(config.tournamentUrl);
      // A Retry-After hold is left as it is: it belongs to its platform (see holdUntil).
      lastError = '';
      consecutiveErrors = 0; // the new URL does not inherit the old one's backoff
      eventState = null; // new tournament — forget the old one's state/schedule
      eventStartAt = null;
      autoStopped = null;
      waitingUntil = null;
      if (!config.tournamentUrl) {
        // ZIP-12: with no URL there is nothing to poll, so clearing it stops polling.
        polling = false;
        pollAgain = false;
        disarmPoll();
      }
      persist();
      requestPoll(true); // nothing while polling is off
      pushStatus();
      return;
    }
    case 'start': startPolling(); return;
    case 'stop': stopPolling(); return;
    case 'pollNow':
      if (!config.tournamentUrl) { refuse(NO_URL_MESSAGE); return; }
      if (!polling) { polling = true; autoStopped = null; persist(); }
      // Once the gap and any Retry-After allow; the status says when (nextPollAt).
      requestPoll(true);
      pushStatus();
      return;
    case 'setInterval':
      config.pollIntervalMs = clampInt(value, MIN_INTERVAL_MS, 3600000, config.pollIntervalMs);
      persist();
      // Through the one next-delay function, so a longer backoff or a Retry-After is not
      // cut short.
      scheduleRegular();
      pushStatus();
      return;
    case 'setCapsText':
      config.caps.text = String(value ?? '').slice(0, 120);
      lastHash = null; // caps travel in the update payload — force a re-push
      persist();
      // Stopped (or message-only, with no tournament) still needs the new caps
      // on screen, so push rather than wait for a poll that may never come.
      if (polling) requestPoll(false); else pushCurrent();
      pushStatus();
      return;
    case 'setCapsLogo': {
      const logo = checkLogo(value);
      if (logo === null) { refuse(LOGO_REFUSED_MESSAGE); return; } // the logo is kept
      config.caps.logo = logo;
      lastHash = null;
      persist();
      if (polling) requestPoll(false); else pushCurrent();
      pushStatus();
      return;
    }
    case 'setTopN':
      config.topN = clampInt(value, 0, 100, config.topN);
      lastHash = null;
      persist();
      requestPoll(false);
      pushStatus();
      return;
    case 'setMaxItems':
      config.maxItems = clampInt(value, 1, 100, config.maxItems);
      lastHash = null;
      persist();
      requestPoll(false);
      pushStatus();
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
      if (polling) requestPoll(false); else pushCurrent();
      pushStatus();
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
      if (polling && !config.autoSchedule) { waitingUntil = null; scheduleRegular(); }
      pushStatus();
      return;
    case 'setOptions': {
      // The control page sends all three on every Apply. Only a changed maxItems or topN
      // needs the payload rebuilt, which takes a poll (row 4.3 replaces it with a rebuild
      // from the last result); that re-poll is a request, as setMaxItems and setTopN make.
      // Otherwise, a new interval alone or no change at all, it is a reschedule through
      // the one next-delay function, so it cannot cut a longer error backoff or a
      // Retry-After short (ZIP-13).
      const o = parseJson(value);
      let rebuild = false;
      if (o && typeof o === 'object') {
        if (o.intervalMs !== undefined) config.pollIntervalMs = clampInt(o.intervalMs, MIN_INTERVAL_MS, 3600000, config.pollIntervalMs);
        if (o.maxItems !== undefined) {
          const n = clampInt(o.maxItems, 1, 100, config.maxItems);
          if (n !== config.maxItems) { config.maxItems = n; rebuild = true; }
        }
        if (o.topN !== undefined) {
          const n = clampInt(o.topN, 0, 100, config.topN);
          if (n !== config.topN) { config.topN = n; rebuild = true; }
        }
      }
      persist();
      if (rebuild) { lastHash = null; requestPoll(false); } else scheduleRegular();
      pushStatus();
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
