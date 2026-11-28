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
//             Every DoAction goes through one send queue (see "send queue"): one in
//             flight, the next once its reply and its own broadcast are both in, or
//             after 800 ms; a send's outcome waits for its reply up to 5 s. When the
//             newest push is not queued it is owed, and the current payload is pushed
//             when the sidecar next connects (ZIP-09), or, while it stays connected, again
//             30 s later and on the control page's next status request.
//   inbound   subscribes to General.Custom and consumes
//             { type:'ticker:command', command, value } broadcasts emitted by
//             the control page via the "Ticker Command" action.
//
// Commands: setUrl, start, stop, pollNow, setInterval, setCaps, setCapsText,
//           setCapsLogo, setOptions, setMaxItems, setTopN, setMessage,
//           setMessageText, setMessageEnabled, setAutoSchedule, status.
//           Names match without regard to case. An unknown or refused command is
//           reported in the status's commandNotice (docs/PROTOCOL.md).
//           A new URL pushes a blank payload at once, caps and message kept
//           (decision 5). A URL pasted without https:// is given it (decision 11 d).
//           Cap and option changes push at once, polling or not, from
//           the last result the sidecar keeps (ZIP-14).
//           A good poll's non-fatal notice (start.gg's page limit) is the status's
//           pollNotice until the next good poll or a setUrl that changes the URL.
//
// Auto-scheduling (config.autoSchedule, default ON): API calls are only spent
// while the event is actually running. A poll that comes back `completed`
// pushes the final payload and then stops polling; one that comes back
// `upcoming` checks again every 15 minutes (or at the poll interval, if that is
// longer) until the platform's advertised start time, so a bracket started early
// is picked up (slow re-check when it has no start time), and resumes normal
// polling once the event goes live.
//
// The poll loop (see "Poll loop" below): one poll at a time, 45 s at most for a
// poll and 10 s for each request in it (platforms/guarded-fetch.js), at least
// 15 s between the end of one poll and the start of the next whatever asks for
// it, an error backoff that never goes below the interval, and a site's
// Retry-After kept. A result that comes back after setUrl changed the URL is
// thrown away. The status's nextPollAt says when the next poll runs.
//
// Config lives in ./config.json (gitignored; seeded from config.example.json when it is
// missing, unless config.json.tmp holds a whole copy of the settings: then nothing is
// copied, and configWarning says so). It is read once, at start: edit it with the sidecar
// stopped, or restart the sidecar afterwards; a hand edit made while it runs is left as
// it is, and nothing more is saved until the restart. A config.json that does not parse,
// cannot be read, holds no JSON object, is saved as UTF-16, is empty or is not valid UTF-8
// is never written over: the sidecar runs on its built-in defaults with no keys, not even the
// environment's, and saves nothing (see "Config" below). The status's configWarning says
// why, and the control page shows it. A config.json.tmp left by a crash, or by a save
// whose write in place failed, is kept while config.json is refused, and configWarning
// says when it holds a whole copy of the settings.
// API keys may also come from env: CHALLONGE_API_KEY, STARTGG_API_KEY, used when
// config.json has none, and never saved to it. A key that is not printable ASCII (a
// space or a line break inside it) is not used, and configWarning names it. A key
// the sidecar knows (8 characters or more) is replaced by a label such as
// [start.gg key] in the status's errors, notices and url, and in the log. It is
// replaced in the caps text, the caps logo and the message text too, in the status and
// in every ticker:update the sidecar pushes. config.json keeps what was typed.
//
// Log: every line also goes to config.log beside config.json (named after the config
// file), which the running sidecar moves to config.old.log once it passes 1 MB. A
// crash is logged there too, and the sidecar then exits with code 1.
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
//       test keys stubHoldFile, stubHoldIgnoresAbort and stubFail, and lets the test
//       settings below apply; without it each of them is ignored)
//       ZIPPER_REQUEST_TIMEOUT_MS, ZIPPER_POLL_DEADLINE_MS, ZIPPER_MIN_POLL_GAP_MS,
//       ZIPPER_PUSH_RETRY_MS (tests only, and only with ZIPPER_ALLOW_STUB=1: shorten the
//       10 s per request, the 45 s per poll, the 15 s between polls and the 30 s before an
//       owed push is retried; each is clamped, so it can only shorten its limit. Without
//       ZIPPER_ALLOW_STUB=1 the full limits apply whatever these say. The sidecar logs
//       the limits in effect as it starts.)
//       ZIPPER_SE_BASE (tests only, and only with ZIPPER_ALLOW_STUB=1: points the
//       StreamElements requests at another address)

import { readFile, writeFile, rename, rm, open } from 'node:fs/promises';
import { existsSync, appendFileSync, statSync, renameSync } from 'node:fs';
import { createServer } from 'node:net';
import { resolve, relative, isAbsolute, dirname, basename, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';
import { detectPlatform, normalizeInput } from './platforms/index.js';
import { guardedFetch, statusError, parseRetryAfter, envLimit, seconds, requestTimeoutMs } from './platforms/guarded-fetch.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = process.env.ZIPPER_CONFIG || resolve(__dirname, 'config.json');
const EXAMPLE_PATH = resolve(__dirname, 'config.example.json');
// A save is written here, flushed, and renamed over config.json (row 5.1).
const TMP_PATH = `${CONFIG_PATH}.tmp`;
// The log file (row 5.2): beside the config file and named after it (config.json →
// config.log), so the tests' sidecars, each with a config of its own, never share one.
// Past LOG_MAX_BYTES the running sidecar moves it to config.old.log, replacing the one
// before, and starts it again.
const LOG_BASE = CONFIG_PATH.replace(/\.json$/i, '');
const LOG_PATH = `${LOG_BASE}.log`;
const LOG_OLD_PATH = `${LOG_BASE}.old.log`;
const LOG_MAX_BYTES = 1024 * 1024;
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
// configWarning's sentences (row 5.1 to 5.4). A config.json that did not load is named by
// what went wrong, never by its content: V8's own message quotes the file beside the
// error, and that can be part of a key.
const CONFIG_LOAD_TAIL = '; the sidecar runs on its defaults and saves nothing until the file is fixed and the sidecar restarted';
const CONFIG_UNREADABLE_MESSAGE = `config.json could not be read${CONFIG_LOAD_TAIL}`;
const CONFIG_NOT_OBJECT_MESSAGE = `config.json does not hold a JSON object${CONFIG_LOAD_TAIL}`;
// The owner's ruling of 2026-11-26: a file saved as UTF-16 and an empty one each have a
// sentence of their own, not "not valid JSON".
const CONFIG_UTF16_MESSAGE = `config.json is saved as UTF-16; save it as UTF-8${CONFIG_LOAD_TAIL}`;
const CONFIG_EMPTY_MESSAGE = `config.json is empty${CONFIG_LOAD_TAIL}`;
// R5 (the owner's ruling of 2026-11-26): a file whose bytes are not valid UTF-8, such as
// one saved as ANSI with an accented letter in it.
const CONFIG_NOT_UTF8_MESSAGE = `config.json is not saved as UTF-8; save it as UTF-8${CONFIG_LOAD_TAIL}`;
// A config.json that is missing while config.json.tmp holds a whole copy of the settings
// (a choice: see loadConfig).
const CONFIG_MISSING_MESSAGE = `config.json is missing${CONFIG_LOAD_TAIL}`;
// Added after a load sentence when config.json.tmp holds a whole copy of the settings. It
// quotes neither file.
const CONFIG_TMP_WHOLE_MESSAGE = 'a whole copy of the settings is in config.json.tmp';
const CONFIG_HAND_EDIT_MESSAGE = 'config.json was edited while the sidecar was running, so the sidecar no longer saves it; restart the sidecar to load the edit (changes made since then last only until the restart)';
const LOGO_DROPPED_MESSAGE = 'config.json: the saved logo was dropped (it must be an http(s) URL or a path on the overlay\'s server)';
// A key that is not printable ASCII (row 5.3), named by where it came from.
const badKeyMessage = (where) => `${where} contains a space, a line break or another character that cannot be part of a key, so it is not used. Paste it again.`;
// Windows' answers when another program (an editor, an antivirus, OneDrive) holds a file.
const LOCK_CODES = new Set(['EBUSY', 'EPERM', 'EACCES']);
// How long a locked config.json is read again at start before it counts as unreadable, and
// how often (row 5.1, as Greenroom does).
const CONFIG_READ_RETRY_MS = 2000;
const CONFIG_READ_STEP_MS = 100;
// How many times a save's rename over config.json is tried when the file is held, and how
// far apart, before it is written in place instead.
const RENAME_TRIES = 10;
const RENAME_RETRY_MS = 50;
// Key values shorter than this are not redacted (row 5.3): too short to be a key, and
// likely to match ordinary text.
const REDACT_MIN_CHARS = 8;
// An unknown command is reported by its name only, cut to this many characters (the
// longest real name, setMessageEnabled, has 17). The name can come from any local
// program, or from chat through a Ticker Command pasted before 1.0.32 (the one from
// 1.0.32 relays only five names from chat), so nothing else limits its length.
const COMMAND_NAME_MAX = 40;
// A result's pollNotice is kept to this many characters, cut after redaction (row 8.3).
// An adapter's notice is fixed words well under it; a stub: file can carry anything.
const POLL_NOTICE_MAX = 200;
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
// The tests shorten these two (and the 10 s per request, in platforms/guarded-fetch.js)
// through ZIPPER_* settings, and only with ZIPPER_ALLOW_STUB=1 (the owner's ruling of
// 2026-11-26): a normal install keeps the full limits whatever those settings say, so no
// setting can make it poll a site more often or cut a poll short.
const pollDeadlineMs = STUB_ALLOWED ? envLimit('ZIPPER_POLL_DEADLINE_MS', POLL_DEADLINE_MS, 50) : POLL_DEADLINE_MS;
const minPollGapMs = STUB_ALLOWED ? envLimit('ZIPPER_MIN_POLL_GAP_MS', MIN_POLL_GAP_MS, 0) : MIN_POLL_GAP_MS;
const MESSAGE_MAX_CHARS = 1000;
const MESSAGE_MAX_LINES = 20;
const WAITING_POLL_MS = 300000; // 5 min re-check while upcoming with no usable start time
// The longest wait before an upcoming event (row 4.6), so a bracket started early is
// picked up within it. It never goes below the poll interval: a streamer who set a longer
// interval to spend fewer calls keeps it. (With no start time the re-check above is
// already no longer than this.) A choice the plan left open: see the sprint notes.
const UPCOMING_WAIT_CAP_MS = 15 * 60000;
// What a timed-out milestone announcement (no reply from Streamer.bot within 5 s, see the
// send queue) does to the saved threshold (row 4.5, a choice left to the owner). false: it
// stays, so the next StreamElements poll announces it again (a repeat is possible, a loss
// is not). A reply that comes after the 800 ms release but within the 5 s settles it.
const MILESTONE_ADVANCES_ON_TIMEOUT = false;
// A push that was not queued is owed (ZIP-09). Besides the next connect, it is pushed again
// this long after, while the socket stays open: Streamer.bot keeps the connection when the
// missing Ticker Push is added, so no reconnect would come to pay it. The tests shorten it,
// and only with ZIPPER_ALLOW_STUB=1: a normal install keeps the 30 s, so no setting can make
// a missing Ticker Push be retried many times a second.
const PUSH_RETRY_MS = 30000;
const pushRetryMs = STUB_ALLOWED ? envLimit('ZIPPER_PUSH_RETRY_MS', PUSH_RETRY_MS, 50) : PUSH_RETRY_MS;

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
  // The StreamElements key each metric's value was last read from (persisted), so a subs
  // reading from the other key is recorded silently instead of taken as a drop (ZIP-16).
  milestoneKeys: {},
  // Auto-scheduling: stop polling when the event completes; while it hasn't started,
  // check only every 15 minutes (or at the interval, if longer) until the platform's
  // start time instead of burning API calls.
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
// A non-fatal fact about the last good poll, from its adapter's result (row 8.3: start.gg's
// page limit). Set or cleared by every good poll, left alone by a failed one, cleared by a
// setUrl that changes the URL. Not an error: it counts for nothing in the backoff.
let pollNotice = '';
let lastPollAt = null;
let lastHash = null;
let lastPayload = null; // last payload built, so config-only changes (message,
                        // caps) can re-push without waiting for a poll
// The last good result, with what it was fetched under: { result, gen, url, platform,
// maxItems, topN }. Option changes rebuild from it (ZIP-14); setUrl drops it.
let lastRaw = null;
// Set when the newest push ends any way but "queued" (an older push's outcome counts for
// nothing, see pushUpdate): the current payload is pushed again when the sidecar next
// connects to Streamer.bot, or while it stays connected, PUSH_RETRY_MS later and on a
// `status` command (ZIP-09). A later queued push clears it.
let pushOwed = false;
let pushRetryTimer = null;
let platformName = null;
let eventState = null; // last fetched tournament state (upcoming/in_progress/completed)
let eventStartAt = null; // platform's advertised start time (ISO), when it has one
let waitingUntil = null; // set while auto-scheduling waits before an upcoming event: its next check
let autoStopped = null; // 'completed' after an automatic stop, for the status line
let urlGeneration = 0; // ZIP-02: bumped by every accepted setUrl; a poll keeps the one it began under
let pollAgain = false; // a pollNow or start that arrived while a poll was in flight
let pollAbort = null; // the AbortController of the poll in flight
// One fetch outside the poll loop, for a command while polling is off (row 4.3): the
// timer that waits for the gap, and whether another was asked for while one ran.
let oneOffTimer = null;
let oneOffAgain = false;
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
//
// How config.json is kept safe (ZIP-05, rows 5.1 and 5.4):
//   • It is read once, at start. A file that does not parse, cannot be read (anything but
//     missing; a locked one is tried again for CONFIG_READ_RETRY_MS first), does not hold
//     a JSON object, is saved as UTF-16, is empty or is not valid UTF-8 is never written
//     over: the sidecar runs on its built-in defaults with no keys (those from the
//     environment are not used either: a choice the plan left open, settled the
//     conservative way), saves nothing for the rest of the run, and says why in
//     configWarning, in a fixed sentence. A UTF-8 byte-order mark is stripped first.
//     config.example.json is copied only when config.json is missing.
//   • config.json.tmp, which a crash between a save's write and its rename can leave, is
//     removed at start only once config.json has loaded, and only by an instance that
//     holds the guard port. While config.json is refused it is kept as it is (nothing is
//     saved, so it is never written either), and when it holds a whole copy of the
//     settings configWarning says so. A config.json that is missing beside such a copy is
//     not seeded from the example (see loadConfig).
//   • A save writes config.json.tmp, flushes it, and renames it over config.json, trying
//     again while another program holds the file; if the rename keeps failing it writes
//     config.json in place, and puts the old bytes back if that fails too. Saves run one at
//     a time (persist), each writing what is true when it starts. A failed write in place
//     can leave config.json cut short, so from then until a save writes config.json whole
//     the temp file, the one whole copy of the settings, is kept (configMayBeCut): a
//     restart meanwhile refuses the cut file and names the copy.
//   • A hand edit made while the sidecar runs is not written over: before each save, and
//     again before an in-place write, the file must still have the size and modified time
//     it had when the sidecar last read or wrote it. If not, the sidecar stops saving until
//     it is restarted, and says so in configWarning. A failed in-place write counts as the
//     sidecar's own change, so the next save tries again instead of stopping.
//   • Keys from the environment are used but never saved (ZIP-21): `config` holds what
//     config.json holds, and the keys in use are kept apart (keysInUse).

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Set when config.json did not load (one of the sentences above): nothing is saved.
let configLoadProblem = '';
// config.json's size and modified time as the sidecar last read or wrote it; null while
// it does not exist; undefined before it is first looked at.
let configStamp;
// Set once a hand edit is seen: nothing more is saved until a restart.
let configEditedByHand = false;
let saveRefusedLogged = false;
// Set when a save's write in place fails, which can leave config.json cut short, and
// cleared once a save writes it whole: meanwhile config.json.tmp may be the one whole copy
// of the settings, so no save removes it (saveConfig).
let configMayBeCut = false;
// Set once this instance holds the guard port (acquireGuard). Only then does it remove a
// config.json.tmp at start (loadConfig).
let guardHeld = false;

// configWarning (docs/PROTOCOL.md): config.json load problems, a saved logo dropped, a
// refused key, saves stopped after a hand edit, and a failed write, one entry each, in the
// order they arose. A restart clears them; a failed write's clears on the next good one.
const configWarnings = new Map();
// logText, when given, is what the log says instead: the same sentence with the system's
// error code, which the status leaves out.
function warnConfig(id, text, logText = text) {
  if (configWarnings.get(id) === text) return;
  configWarnings.set(id, text);
  log(logText);
}
function configWarning() {
  return [...configWarnings.values()].join(' · ');
}

async function loadConfig() {
  let buf;
  try {
    buf = await readConfigFile();
  } catch (err) {
    // The log line names the system's code (EPERM, EISDIR, EBUSY...), never its message,
    // which quotes the path; the status keeps the fixed sentence.
    await refuseConfig(CONFIG_UNREADABLE_MESSAGE, `config.json could not be read${errorCode(err)}${CONFIG_LOAD_TAIL}`);
    return;
  }
  if (buf === null) {
    // A choice (the owner's ruling covers a config.json that is refused, not a missing one):
    // a config.json that is missing while config.json.tmp holds a whole copy of the settings
    // is treated as refused, the conservative way. The example is not copied in, nothing is
    // saved, the temp file is kept, and configWarning says both, so the operator can put
    // the copy back (or delete it to start from the example) and restart.
    if (await tempIsWhole()) {
      await refuseConfig(CONFIG_MISSING_MESSAGE);
      return;
    }
    let example;
    try {
      example = await readFile(EXAMPLE_PATH);
    } catch {
      configStamp = null;
      log('No config.example.json — using built-in defaults');
      return;
    }
    try {
      // 'wx': only a config.json that is still missing; one made meanwhile is read instead.
      await writeFile(CONFIG_PATH, example, { flag: 'wx' });
      log('Seeded config.json from config.example.json');
    } catch (err) {
      if (err?.code === 'EEXIST') { await loadConfig(); return; }
      log(`config.json could not be created from config.example.json${errorCode(err)}; the next save tries again`);
    }
    buf = example;
  }
  configStamp = fileStamp();
  if (looksUtf16(buf)) {
    await refuseConfig(CONFIG_UTF16_MESSAGE);
    return;
  }
  // No bytes, or only white space once a UTF-8 byte-order mark is stripped.
  if (!buf.toString('utf-8').replace(/^\uFEFF/, '').trim()) {
    await refuseConfig(CONFIG_EMPTY_MESSAGE);
    return;
  }
  // R5 (the owner's ruling of 2026-11-26): bytes that are not valid UTF-8 are refused. Read
  // leniently, each bad byte would become U+FFFD (an accented letter in a file saved as
  // ANSI, say), and the next save would write that over the file for good. A file of plain
  // ASCII saved as ANSI is valid UTF-8, so it still loads (one saved as UTF-16 is refused
  // above). ignoreBOM keeps a UTF-8 byte-order mark in the text, so it is stripped below as
  // before: one mark only, so a file with two stays "not valid JSON". Nothing of the file
  // or the decoder's error is quoted.
  let text;
  try {
    text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buf).replace(/^\uFEFF/, '');
  } catch {
    await refuseConfig(CONFIG_NOT_UTF8_MESSAGE);
    return;
  }
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    await refuseConfig(notJsonMessage(err, text));
    return;
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    await refuseConfig(CONFIG_NOT_OBJECT_MESSAGE);
    return;
  }
  adoptConfig(parsed);
  // config.json has loaded: a temp file a crash left is removed now, and only now (the
  // owner's ruling of 2026-11-26; while config.json is refused it is kept, see
  // refuseConfig), and only by an instance that holds the guard port. A second launch exits
  // at the guard before it gets here; one that goes on without the guard, because the port
  // could not be opened (main), may share the folder with a running sidecar whose save is
  // writing that file, so it leaves the file alone (its own first save writes over it).
  if (guardHeld) await rm(TMP_PATH, { force: true }).catch(() => {});
}

// Whether config.json's bytes are UTF-16 (the owner's ruling of 2026-11-26): a UTF-16
// byte-order mark (FF FE, as Windows PowerShell's ">" and plain Out-File write it, or
// FE FF), or, without one, a NUL as every second byte at the start (up to the first four
// characters), the other byte of each pair not NUL. A file of NULs alone is not taken for
// UTF-16: it stays "not valid JSON".
function looksUtf16(buf) {
  if (buf.length >= 2 && ((buf[0] === 0xff && buf[1] === 0xfe) || (buf[0] === 0xfe && buf[1] === 0xff))) return true;
  const pairs = Math.min(4, buf.length >> 1);
  if (!pairs) return false;
  let le = true;
  let be = true;
  for (let i = 0; i < pairs; i++) {
    const a = buf[2 * i];
    const b = buf[2 * i + 1];
    if (!(a !== 0 && b === 0)) le = false;
    if (!(a === 0 && b !== 0)) be = false;
  }
  return le || be;
}

// Whether config.json.tmp holds a whole copy of the settings: it can be read, and parses
// (a UTF-8 byte-order mark stripped) to a JSON object. It is only read, never changed, and
// nothing of it is quoted anywhere.
async function tempIsWhole() {
  try {
    const parsed = JSON.parse((await readFile(TMP_PATH)).toString('utf-8').replace(/^\uFEFF/, ''));
    return !!parsed && typeof parsed === 'object' && !Array.isArray(parsed);
  } catch {
    return false;
  }
}

// config.json's bytes, or null when it is missing. A file another program holds is read
// again for CONFIG_READ_RETRY_MS before the error is passed on.
async function readConfigFile() {
  const until = monoNow() + CONFIG_READ_RETRY_MS;
  for (;;) {
    try {
      return await readFile(CONFIG_PATH);
    } catch (err) {
      if (err?.code === 'ENOENT') return null;
      if (!LOCK_CODES.has(err?.code) || monoNow() >= until) throw err;
      await sleep(CONFIG_READ_STEP_MS);
    }
  }
}

// config.json did not load: nothing is saved for the rest of the run (saveConfig returns
// before it writes anything, config.json.tmp included), and config.json.tmp is left as it
// is. When that temp file holds a whole copy of the settings, configWarning says so after
// the load sentence; when it does not, or there is none, nothing is said about it.
async function refuseConfig(text, logText = text) {
  configLoadProblem = text;
  warnConfig('load', text, logText);
  if (await tempIsWhole()) warnConfig('tmp', CONFIG_TMP_WHOLE_MESSAGE);
}

// "config.json is not valid JSON (line N)". N is counted from the position V8 reports, in
// the text after the byte-order mark; with no position it is left out. Only the digits are
// taken from V8's message, which otherwise quotes the file.
function notJsonMessage(err, text) {
  const m = /\bposition (\d+)\b/.exec(String(err?.message ?? ''));
  const pos = m ? Number(m[1]) : NaN;
  const line = Number.isFinite(pos) && pos <= text.length ? text.slice(0, pos).split('\n').length : 0;
  return `config.json is not valid JSON${line ? ` (line ${line})` : ''}${CONFIG_LOAD_TAIL}`;
}

// " (EPERM)" for an error with a system code, or "".
function errorCode(err) {
  return typeof err?.code === 'string' && /^E[A-Z]+$/.test(err.code) ? ` (${err.code})` : '';
}

// The keys the sidecar uses (row 5.3), from config.json, or from the environment when the
// file has none, and only when they pass checkKey. Never saved: `config` holds the file's own.
const keysInUse = { challonge: null, startgg: null, jwt: null };
const KEY_LABELS = { challonge: '[Challonge key]', startgg: '[start.gg key]', jwt: '[StreamElements token]' };

// Tally's cleanKey rule: printable ASCII 0x21-0x7E after trimming. { key } to use (null
// for none), or { refused: true }.
function checkKey(value) {
  if (value == null) return { key: null };
  if (typeof value !== 'string' && typeof value !== 'number') return { refused: true };
  const s = String(value).trim();
  if (!s) return { key: null };
  return /^[\x21-\x7e]+$/.test(s) ? { key: s } : { refused: true };
}

// The key a field uses: the file's, or, when the file has none, the environment's. A
// refused one is not used (and the other source is not tried in its place); configWarning
// names it without quoting it.
function keyFor(fileValue, field, envName) {
  const fromFile = checkKey(fileValue);
  if (fromFile.refused) { warnConfig(`key:${field}`, badKeyMessage(`${field} in config.json`)); return null; }
  if (fromFile.key || !envName) return fromFile.key;
  const fromEnv = checkKey(process.env[envName]);
  if (fromEnv.refused) { warnConfig(`key:${envName}`, badKeyMessage(`${envName} (environment variable)`)); return null; }
  return fromEnv.key;
}

// Every key value known, accepted or refused, from config.json and the environment, with
// its label (row 5.3): the forms it can take in a text (as given, trimmed, as a URL
// encodes it) and, for a refused one, each printable run of it. Each of these is also
// taken as a JSON string escapes it: a setCaps or setMessage value is JSON, so a key with
// a double quote or a backslash in it arrives in the command's value as \" or \\, and the
// log line of that command is redacted in that form. Longest first. It never
// throws: a value with a lone surrogate (an escape such as \ud800 in config.json) has no
// URL form, and is refused by checkKey, so it is never sent anywhere to need one.
let redactions = [];
function urlForm(s) {
  try { return encodeURIComponent(s); } catch { return ''; }
}
function jsonForm(s) {
  return JSON.stringify(s).slice(1, -1);
}
function setRedactions() {
  const forms = (value, label) => {
    if (typeof value !== 'string' && typeof value !== 'number') return [];
    const raw = String(value);
    const plain = [raw, raw.trim(), urlForm(raw.trim()), ...raw.split(/[^\x21-\x7e]+/)];
    const all = new Set([...plain, ...plain.map(jsonForm)]);
    return [...all].filter((f) => f.length >= REDACT_MIN_CHARS).map((f) => [f, label]);
  };
  redactions = [
    ...forms(process.env.CHALLONGE_API_KEY, KEY_LABELS.challonge),
    ...forms(process.env.STARTGG_API_KEY, KEY_LABELS.startgg),
    ...forms(config.challongeApiKey, KEY_LABELS.challonge),
    ...forms(config.startggApiKey, KEY_LABELS.startgg),
    ...forms(config.streamElements?.jwtToken, KEY_LABELS.jwt),
  ].sort((a, b) => b[0].length - a[0].length);
}
setRedactions(); // the environment's keys, from the first line on; adoptConfig adds the file's

// A text with every key value in it replaced by its label (row 5.3). It runs in log(), on
// every line before it reaches the console or the file, and on every error before the
// status carries it (errorText), on a command's name and value before they are cut, and on
// the caps and the message as the status and every pushed ticker:update carry them (R6).
// A key cut off at the end of a text the sidecar did not cut itself (an adapter quotes a
// site's answer cut to 200 characters) is matched by its first REDACT_MIN_CHARS or more
// characters; a piece shorter than that, 1 to 7 characters, is left (Sprint 8, the
// adapters, can move that cut after the redaction).
function redact(text) {
  let s = String(text ?? '');
  for (const [value, label] of redactions) if (s.includes(value)) s = s.split(value).join(label);
  for (const [value, label] of redactions) {
    for (let n = Math.min(value.length - 1, s.length); n >= REDACT_MIN_CHARS; n--) {
      if (s.endsWith(value.slice(0, n))) return s.slice(0, s.length - n) + label;
    }
  }
  return s;
}

// The text of an error as the status carries it (lastError, the StreamElements error).
function errorText(err) {
  return redact(err?.message || String(err));
}

// The tournament URL as the status and every pushed payload carry it, to every client of
// the bus: a key pasted into it (a Challonge API address with its api_key, say) is
// replaced by its label (row 5.3). config.tournamentUrl, and config.json, keep it as set.
function shownUrl(url) {
  return url == null ? url : redact(url);
}

// R6 (the owner's ruling of 2026-11-26): a key typed into the caps text, the caps logo or
// the message text is replaced by its label in the status and in every ticker:update the
// sidecar pushes, as in the URL. The command that set it is not the sidecar's to change:
// Streamer.bot broadcasts each ticker:command to every client of the bus as it was sent.
// A logo is an http(s) URL or a path on the overlay's server (checkLogo refuses a data:
// URI on every way in); a key in it is replaced as given and as a URL encodes it. A key
// that the sidecar's own cut fell inside (the caps text at 120 characters, the logo at
// 500, the message at 1000) can keep its first 1 to 7 characters, as redact() says.
// Only what is sent changes: config, config.json, lastPayload and the change check
// (hashPayload) keep what was typed. The labels for a run never change (the keys are read
// at start), so a payload and the one before it are redacted alike: a key in the caps or
// the message neither makes a poll push again nor stops a change from being pushed.
function shownCaps(caps) {
  return { ...caps, text: redact(caps?.text), logo: redact(caps?.logo) };
}
function shownMessage(message) {
  return { ...message, text: redact(message?.text) };
}
// A ticker:update as it is pushed (pushUpdate): the caps and the message lines redacted.
function shownPayload(payload) {
  const shown = { ...payload, caps: shownCaps(payload.caps) };
  if (payload.message) shown.message = { ...payload.message, lines: payload.message.lines.map((line) => redact(line)) };
  return shown;
}

// What a platform adapter is given: the settings, with the keys in use.
function adapterConfig() {
  return { ...config, challongeApiKey: keysInUse.challonge, startggApiKey: keysInUse.startgg };
}

function adoptConfig(parsed) {
  // A saved logo that fails the rule is dropped here, and written out on the next save. The
  // reason is in configWarning as well as the log.
  let logo = checkLogo(parsed.caps?.logo);
  if (logo === null) {
    logo = '';
    warnConfig('logo', LOGO_DROPPED_MESSAGE);
  }
  config = {
    // The keys as config.json holds them, written back as they are: a key from the
    // environment never lands in the file (ZIP-21), and a refused one is never deleted
    // from it (row 5.3).
    challongeApiKey: parsed.challongeApiKey ?? null,
    startggApiKey: parsed.startggApiKey ?? null,
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
      jwtToken: parsed.streamElements?.jwtToken ?? null,
      pollSeconds: clampInt(parsed.streamElements?.pollSeconds, 5, 3600, 60),
    },
    milestones: {
      followers: clampInt(parsed.milestones?.followers, 0, 1000000, 0),
      subs: clampInt(parsed.milestones?.subs, 0, 1000000, 0),
      tips: clampInt(parsed.milestones?.tips, 0, 1000000, 0),
      cheers: clampInt(parsed.milestones?.cheers, 0, 1000000, 0),
    },
    milestoneState: (parsed.milestoneState && typeof parsed.milestoneState === 'object') ? { ...parsed.milestoneState } : {},
    milestoneKeys: Object.fromEntries(Object.entries((parsed.milestoneKeys && typeof parsed.milestoneKeys === 'object') ? parsed.milestoneKeys : {})
      .filter(([, k]) => typeof k === 'string')),
    autoSchedule: parsed.autoSchedule === undefined ? true : !!parsed.autoSchedule,
    autoStart: !!parsed.autoStart,
  };
  setRedactions();
  keysInUse.challonge = keyFor(config.challongeApiKey, 'challongeApiKey', 'CHALLONGE_API_KEY');
  keysInUse.startgg = keyFor(config.startggApiKey, 'startggApiKey', 'STARTGG_API_KEY');
  keysInUse.jwt = keyFor(config.streamElements.jwtToken, 'streamElements.jwtToken', null);
}

// Saves the settings: one save at a time, each writing what is true when it starts, so a
// burst of commands costs at most one save running and one waiting. Never rejects.
let saveChain = Promise.resolve();
let saveWaiting = false;
function persist() {
  if (saveWaiting) return saveChain;
  saveWaiting = true;
  saveChain = saveChain
    .then(() => { saveWaiting = false; return saveConfig(); })
    .catch((err) => { log(`config.json: a save went wrong${errorCode(err)}`); });
  return saveChain;
}

async function saveConfig() {
  // First, before anything is written: a sidecar whose config.json was refused writes
  // neither config.json nor config.json.tmp, and removes neither (the temp file a crash left
  // may be the whole copy configWarning names).
  if (configLoadProblem) {
    if (!saveRefusedLogged) { saveRefusedLogged = true; log('config.json is not saved while it does not load (see the warning above)'); }
    return;
  }
  if (configEditedByHand) return;
  // autoStart persists the CURRENT polling intent so a sidecar restart resumes.
  const text = JSON.stringify({ ...config, autoStart: polling }, null, 2);
  try {
    await writeFlushed(TMP_PATH, text);
    // Row 5.4: a hand edit since the last read or write is left as it is.
    if (stopForHandEdit()) return;
    if (!(await replaceConfig(text))) return; // a hand edit, seen before the in-place write
    configMayBeCut = false; // config.json is whole: renamed into place, or written in place
    configStamp = fileStamp();
    if (configWarnings.delete('write')) { log('config.json saved again'); pushStatus(); }
  } catch (err) {
    warnConfig('write', `config.json could not be saved${errorCode(err)}; the sidecar tries again on the next change`);
    pushStatus();
  } finally {
    // The temp file goes, unless config.json may be cut short (a write in place failed,
    // in this save or an earlier one, and no save has written it whole since): the temp
    // file is then the one whole copy of the settings, and is kept. The next save writes it
    // again and renames it as usual; a restart before that refuses the cut-short
    // config.json and names the copy (R3).
    if (!configMayBeCut) await rm(TMP_PATH, { force: true }).catch(() => {});
  }
}

async function writeFlushed(path, data) {
  const fh = await open(path, 'w');
  try {
    await fh.writeFile(data);
    await fh.sync();
  } finally {
    await fh.close();
  }
}

// Row 5.4: true when config.json was edited by hand since the sidecar last read or wrote
// it. Saving then stops until a restart, and configWarning says so.
function stopForHandEdit() {
  if (!editedSince()) return false;
  configEditedByHand = true;
  warnConfig('handEdit', CONFIG_HAND_EDIT_MESSAGE);
  pushStatus();
  return true;
}

// The temp file renamed over config.json, tried again while another program holds the
// file. If the rename keeps failing, config.json is written in place, and its old bytes
// are put back if that fails too; a failed write in place sets configMayBeCut, since its
// open may have emptied the file. False when a hand edit stopped it before the in-place
// write: the file was held through every rename, which is when an editor saving it holds
// it, so it is looked at again first (row 5.4).
async function replaceConfig(text) {
  for (let i = 1; ; i++) {
    try {
      await rename(TMP_PATH, CONFIG_PATH);
      return true;
    } catch (err) {
      if (!LOCK_CODES.has(err?.code)) throw err;
      if (i >= RENAME_TRIES) break;
      await sleep(RENAME_RETRY_MS);
    }
  }
  if (stopForHandEdit()) return false;
  let original = null;
  try { original = await readFile(CONFIG_PATH); } catch { /* nothing to put back */ }
  try {
    await writeFlushed(CONFIG_PATH, text);
  } catch (err) {
    configMayBeCut = true; // saveConfig keeps the temp file
    if (original) { try { await writeFlushed(CONFIG_PATH, original); } catch { /* left as it is */ } }
    throw err;
  } finally {
    // Whatever config.json now holds (the save, the old bytes put back, or a file the
    // failed write cut short) is the sidecar's own doing, so the next save does not take
    // it for a hand edit: it tries again, and writes the file whole.
    configStamp = fileStamp();
  }
  return true;
}

// config.json's size and modified time; null when it does not exist; undefined when it
// cannot be looked at.
function fileStamp() {
  try {
    const s = statSync(CONFIG_PATH);
    return { size: s.size, mtimeMs: s.mtimeMs };
  } catch (err) {
    return err?.code === 'ENOENT' ? null : undefined;
  }
}

// Whether config.json has changed since the sidecar last read or wrote it: made, removed,
// or a size or modified time of its own.
function editedSince() {
  const now = fileStamp();
  if (now === undefined || configStamp === undefined) return false;
  if (now === null || configStamp === null) return now !== configStamp;
  return now.size !== configStamp.size || now.mtimeMs !== configStamp.mtimeMs;
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
// a Stream Deck (setMessageEnabled on).
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
  cancelOneOff(); // the poll asked for here covers a fetch a command was waiting for
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

// A command asks for a poll: pollNow, start and setUrl (deliberate), and, while polling,
// a cap or option change that has no result to rebuild from or needs more than the last
// result holds (see refetch). It runs as soon as the gap and any Retry-After hold allow,
// skipping the interval and the error backoff; a poll already armed for sooner stands. A
// deliberate request that arrives while a poll is in flight is kept for after it. A
// command's is not: the poll in flight builds its payload with the new settings (and if
// its result, fetched under the old ones, holds less than the new ones ask for, pollOnce
// asks again: wantsMore). The caller
// sends the status.
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
// Auto-scheduling spends API calls only while the event is running: upcoming → wait for
// the platform's start time, checking again at least every UPCOMING_WAIT_CAP_MS (15 min,
// or the interval when that is longer) so a bracket started early is picked up (a slow
// re-check when it has no start time), so polling resumes by itself once the event goes
// live. Errors keep the normal backoff, since a failed fetch says nothing about the event
// state. (A completed event stops polling outright: see afterPoll.)
function nextRegularPoll() {
  const now = monoNow();
  const from = lastPollEndedAt ?? now;
  if (config.autoSchedule && !consecutiveErrors && eventState === 'upcoming') {
    // The advertised start (a wall time), moved onto the monotonic clock.
    const startsAt = eventStartAt ? now + (Date.parse(eventStartAt) - Date.now()) : NaN;
    const untilStart = startsAt - from;
    const hasStart = Number.isFinite(untilStart) && untilStart > 0;
    // The cap applies to both waits but never goes below the interval, so it binds only
    // the wait for a start time: the no-start re-check, max(5 min, interval), is never
    // longer than max(15 min, interval). It also catches organisers moving the date.
    const cap = Math.max(UPCOMING_WAIT_CAP_MS, config.pollIntervalMs);
    const wait = Math.min(hasStart ? untilStart : Math.max(WAITING_POLL_MS, config.pollIntervalMs), cap);
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
    log(`event not started — ${next.hasStart ? `checking again at ${waitingUntil} (start ${eventStartAt})` : `re-checking in ${Math.round((pollDueAt - monoNow()) / 60000)} min (no start time from the platform)`}`);
  }
}

// A poll, or with oneOff a single fetch outside the poll loop (refetch, while polling is
// off): the same fetch and the same handling of its result, but a one-off neither arms
// the next poll nor auto-stops polling, so it does not change whether polling is on.
async function pollOnce({ oneOff = false } = {}) {
  if (pollInFlight) { if (oneOff) oneOffAgain = true; return; }
  if (!polling && !oneOff) return;
  pollInFlight = true;
  // What this poll is for, taken before the fetch: a setUrl during it changes these.
  const gen = urlGeneration;
  const url = config.tournamentUrl;
  const platform = platformFor(url);
  const fetchedWith = { maxItems: config.maxItems, topN: config.topN };
  const ac = new AbortController();
  pollAbort = ac;
  const deadline = setTimeout(() => ac.abort(new Error(`the poll took longer than ${seconds(pollDeadlineMs)} and was stopped`)), pollDeadlineMs);
  let stale = false;
  let needMore = false; // a command since asks for more than the result kept holds (wantsMore)
  try {
    const result = await fetchCurrent(url, ac.signal, fetchedWith);
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
      // Kept only once it has built: a result that cannot be built fails the poll, and an
      // option change then rebuilds from the last one that could (row 4.3).
      lastRaw = { result, gen, url, platform, ...fetchedWith };
      needMore = wantsMore(lastRaw);
      // Set or cleared by this good poll ("Status messages"), and only once it has built.
      const notice = pollNoticeOf(result);
      if (notice && notice !== pollNotice) log(`poll notice: ${notice}`);
      pollNotice = notice;
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
      lastError = errorText(err); // every key redacted (row 5.3)
      lastPollAt = new Date().toISOString();
      log(`poll failed (${consecutiveErrors}x): ${lastError}`);
    }
  } finally {
    clearTimeout(deadline);
    pollAbort = null;
    pollInFlight = false;
    lastPollEndedAt = monoNow();
    const again = oneOffAgain;
    oneOffAgain = false;
    if (stale || pollAgain) {
      // The new URL, or the poll a command asked for while this one ran: next, once the
      // gap allows. Auto-scheduling decides after that poll.
      pollAgain = false;
      requestPoll(true);
    } else if (!oneOff) {
      // May auto-stop; runs before the status so the status shows it. Never after a one-off:
      // one starts only while polling is off, and polling comes on during it only through
      // start or pollNow, which ask for a poll (pollAgain), so such a one-off takes the
      // branch above.
      afterPoll();
    }
    // One more fetch only when the reason for it still holds (row 4.3). A fetch a command
    // asked for while this one ran (polling off): this one, if it came back, was built
    // with that command's settings, so it is needed only while nothing is kept (this one
    // failed). A result is fetched again when a command during the fetch asked for more
    // than it holds (wantsMore: more than it was cut short at, or more standings than were
    // asked for); while polling, a command's request made during a poll is not kept, so
    // this is where that re-fetch is asked for.
    if (!stale && (needMore || (again && !lastRaw && !polling))) refetch();
    pushStatus();
  }
}

// Whether the settings now ask for more than a kept result holds (row 4.3). Either its
// adapter cut it short (`cutShort`; only start.gg does, when its paging stops early), and
// maxItems is higher than it was fetched with, or topN has been switched on since. Or topN
// is higher than the standings its adapter asked for (`standingsAsked`, a number; only
// start.gg records it, the standings its first page asked for), cut short or not, at the
// page limit or not (the owner's ruling of 2026-11-27); a lower topN, one within what was
// asked for, or a result without the field fetches nothing for this. That topN must also
// be higher than the one the result was fetched with: start.gg always asks for at least
// that many, so this changes nothing for it, but a result that asked for fewer than its
// own topN (a stub file) is then not fetched again and again.
function wantsMore(raw) {
  const r = raw.result;
  if (r.cutShort === true && (config.maxItems > raw.maxItems || (config.topN > 0 && raw.topN === 0))) return true;
  return Number.isFinite(r.standingsAsked) && config.topN > r.standingsAsked && config.topN > raw.topN;
}

// A result's pollNotice as the status carries it: a string, every key redacted before it
// is cut to POLL_NOTICE_MAX (row 5.3's order), or '' when there is none.
function pollNoticeOf(result) {
  const text = typeof result?.pollNotice === 'string' ? result.pollNotice.trim() : '';
  return text ? redact(text).slice(0, POLL_NOTICE_MAX) : '';
}

// One fetch a command asks for (row 4.3): a cap or option change with no result held and a
// URL set, or one that needs more than the result held (wantsMore). While polling it
// is a poll asked for, under polling's rules. Otherwise it is one fetch outside the poll
// loop: it waits for the minimum gap and any Retry-After hold, it does not go through the
// poll timer or the next-delay function, so it does not start polling, and its result is
// thrown away if setUrl comes first (as a poll's is). With no URL it does nothing.
function refetch() {
  if (!config.tournamentUrl) return;
  if (polling) { requestPoll(false); return; }
  if (pollInFlight) { oneOffAgain = true; return; }
  if (oneOffTimer) return; // one is already waiting for the gap
  const now = monoNow();
  const at = Math.max(now, gapEnd(), holdUntil());
  oneOffTimer = setTimeout(() => {
    oneOffTimer = null;
    if (polling) { requestPoll(false); return; }
    pollOnce({ oneOff: true });
  }, at - now);
}

function cancelOneOff() {
  if (oneOffTimer) { clearTimeout(oneOffTimer); oneOffTimer = null; }
  oneOffAgain = false;
}

// Fetches the normalized result for `url`, with the poll's AbortSignal. `fetchedWith` is the
// maxItems and topN the poll is made with (and keeps beside its result): the adapter is
// given those, so a start.gg early stop matches what wantsMore compares against.
async function fetchCurrent(url, signal, fetchedWith) {
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
  return platform.fetchTicker(id, { ...adapterConfig(), ...fetchedWith }, url, signal);
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
      url: shownUrl(url), // a key in it redacted (row 5.3)
      state: result.state || 'in_progress',
    },
    generatedAt: stampNow(),
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
// same payload underneath, so toggling the message off needs no re-poll: the
// overlay brings them back as it does any new crawl (at its wrap, or under a fade).
function messageBlock() {
  if (!config.message.enabled) return null;
  const lines = config.message.text
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean)
    .slice(0, MESSAGE_MAX_LINES);
  return lines.length ? { lines } : null;
}

// A results-free payload: setUrl's own blank (decision 5), and a message pushed when no
// poll has ever run (the message-only use case: no tournament configured at all).
function blankPayload() {
  return {
    type: 'ticker:update',
    tournament: { name: '', platform: platformName, url: shownUrl(config.tournamentUrl), state: 'in_progress' },
    generatedAt: stampNow(),
    matches: [],
    standings: [],
    caps: { ...config.caps },
  };
}

// The generatedAt of each payload: the wall time, but never at or before the last one
// this sidecar stamped. The pages ignore an update stamped older than the newest they
// have when it comes within 5 s of that newest one (decision 14), so a wall clock set back
// would otherwise drop the pushes that follow each other closely; and the send queue tells
// its pushes' broadcasts apart by it.
let lastStampMs = 0;
function stampNow() {
  lastStampMs = Math.max(Date.now(), lastStampMs + 1);
  return new Date(lastStampMs).toISOString();
}

// Re-push the current strip state NOW, restamped with the live message + caps.
// Config-only changes have to reach the overlay even when polling is off, so
// they can't wait for the next poll to carry them.
function pushCurrent() {
  const payload = {
    ...(lastPayload || blankPayload()),
    generatedAt: stampNow(),
    caps: { ...config.caps },
  };
  const msg = messageBlock();
  if (msg) payload.message = msg;
  else delete payload.message;
  lastPayload = payload;
  lastHash = hashPayload(payload); // a push that is not queued resets this to null
  pushUpdate(payload);
}

// Change detection: only what the strip renders — ids, states, rounds, winners,
// scores, names, the event's state, caps, message (ZIP-15: the winner, the round and
// the event's state are in it, so a corrected winner or an event that completes with no
// set changing is pushed).
function hashPayload(p) {
  return JSON.stringify([
    p.tournament?.name ?? '',
    p.tournament?.state ?? '',
    p.caps,
    p.message ?? null,
    p.matches.map((m) => [m.id, m.state, m.round ?? '', m.winner ?? 0, m.p1?.score, m.p2?.score, m.p1?.name, m.p2?.name]),
    p.standings,
  ]);
}

// Row 4.3 (ZIP-14): a cap change pushes the payload the sidecar holds with the new caps,
// polling or not, so it shows at once even while the site is failing. Since a setUrl that
// payload is setUrl's blank, so it pushes the blank with the new caps. It is the blank too
// once a message has gone out with nothing held (on connect with polling off, or a message
// edit: pushCurrent keeps what it pushed), since that blank has already replaced the stored
// strip. With nothing held at all (after a restart, before the first good poll or any
// message push) and a URL set, it fetches once instead,
// rather than push a blank over the strip Streamer.bot stored, and the caps show with that
// fetch's results; with no URL (a message-only strip) it pushes the blank, with the caps.
function showCaps() {
  if (lastPayload || !config.tournamentUrl) pushCurrent();
  else refetch();
}

// Row 4.3 (ZIP-14): an option change (maxItems, topN) rebuilds the payload from the last
// result, with the URL and platform it was fetched under, and pushes it at once, polling
// or not. The result is fetched again when the change asks for more than it held
// (wantsMore): when its adapter cut it short (`cutShort`; only start.gg marks one, when its
// paging stops early), a higher maxItems than it was fetched with, or topN switched on; and,
// cut short or not, a topN higher than the standings its adapter asked for
// (`standingsAsked`; only start.gg records it). With nothing held (after a restart, or
// since setUrl) and a URL set, it fetches once; with no URL it only saves the setting.
// Apart from setUrl's own blank (decision 5), no blank payload is pushed here. (setUrl
// drops the result kept, so what is kept is always the URL now set's.)
// The rebuild runs inside a command, outside any poll's try: a result that built under the
// settings it was polled with can still throw under others (a standing that is not an
// object, read only once topN is on), and that must not end the process.
function applyOptions() {
  const raw = lastRaw;
  if (raw) {
    let payload;
    try {
      payload = buildPayload(raw.result, raw.url, raw.platform);
    } catch (err) {
      log(`could not rebuild the payload from the last result with the new options: ${err?.message || err}`);
      return;
    }
    lastPayload = payload;
    lastHash = hashPayload(payload);
    pushUpdate(payload);
    if (wantsMore(raw)) refetch();
  } else if (config.tournamentUrl) {
    refetch();
  }
}

// ── StreamElements milestone poller ───────────────────────────────────────────
// Same recipe as the live-verified Midnight Velvet goal meter: poll
// kappa/v2/sessions/<channel> (channel decoded from the JWT), read the session
// counters, and fire a "Ticker Announce" (kind milestone) whenever a counter
// crosses its configured step. The first successful poll only RECORDS the
// current thresholds — no announcement blast when you first configure it.
// A threshold moves on only once its announcement was queued by Streamer.bot (ZIP-16);
// the rule for each reading is milestoneStep below. Polls are chained: the next is
// timed from the end of the last, so two never overlap.

// ZIPPER_SE_BASE points the StreamElements requests, token and all, at another address:
// a test hook (the verify suite's StreamElements mock), honoured only with
// ZIPPER_ALLOW_STUB=1, so no setting on a normal install can send the token elsewhere.
const SE_BASE = (STUB_ALLOWED && process.env.ZIPPER_SE_BASE) || 'https://api.streamelements.com';

const SE_METRICS = {
  followers: { keys: ['follower-total'], text: (n) => `${n} followers — thank you!` },
  subs: { keys: ['subscriber-total', 'subscriber-count'], text: (n) => `${n} subs — thank you!` },
  tips: { keys: ['tip-total'], text: (n) => `$${n} in tips — thank you!` },
  cheers: { keys: ['cheer-total'], text: (n) => `${n} bits — thank you!` },
};

let seTimer = null;
let seLastPollAt = null;
let seLastError = '';
// Per metric, the announcement that last failed to be queued ({ threshold, outcome }), so
// one that keeps failing on every poll (Ticker Announce missing, Streamer.bot down) is
// logged once, not twice a poll: again only when its threshold or its outcome changes.
const milestoneLogged = new Map();

// ── milestone rule: BEGIN (verify.mjs runs this block on its own, in-process) ──
// What one StreamElements reading does to a metric's saved threshold (ZIP-16). `saved` is
// the threshold saved for the metric (not a number before its first sighting), `savedKey`
// the StreamElements key that was read from, `value` and `key` the reading, `step` the
// metric's step. Returns { record } to save a new threshold silently, { announce } for a
// milestone to announce (its threshold is saved only once the announcement is queued), or
// nothing to do ({}).
//   • A first sighting, or a reading from another key than the saved one's (the subs
//     reader takes subscriber-total, or subscriber-count when that is missing), is
//     recorded silently: configuring Zipper mid-stream announces nothing, and a switch of
//     key is not taken as a drop.
//   • A fall to 0, or to more than one step below the saved threshold, is a reset (a new
//     session): it is recorded silently, and milestones above it are announced again as
//     the counter climbs back. A dip of a step or less never resets, so an unfollow and a
//     refollow within one step, or a sub that lapses and renews, never repeat a milestone.
//   • A threshold above the saved one is announced.
function milestoneStep({ saved, savedKey, value, key, step }) {
  const threshold = Math.max(0, Math.floor(value / step) * step);
  if (!Number.isFinite(saved) || (savedKey !== undefined && savedKey !== key)) return { record: threshold };
  if (value <= 0 || value < saved - step) return { record: threshold };
  if (threshold > saved) return { announce: threshold };
  return {};
}
// ── milestone rule: END ──

function seChannelFromJwt(jwt) {
  try {
    let s = String(jwt).split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    while (s.length % 4) s += '=';
    return JSON.parse(Buffer.from(s, 'base64').toString('utf-8')).channel || null;
  } catch { return null; }
}

function seConfigured() {
  return !!keysInUse.jwt
    && Object.values(config.milestones).some((step) => step > 0);
}

// Chained timeouts (ZIP-16): the next poll is armed only in the finally of the last, once
// it has settled, its announcements included, so a slow answer never makes two overlap.
// That alone keeps them apart: startSePolling runs once, at start-up, and no timer exists
// while a poll runs, so no in-flight flag is needed, and no timer is there to clear when it
// starts (an earlier flag and an earlier clear could never be reached, and are gone).
function startSePolling() {
  if (!seConfigured()) return;
  const channel = seChannelFromJwt(keysInUse.jwt);
  if (!channel) { seLastError = 'Could not read the channel id from the StreamElements JWT'; log(seLastError); return; }
  const run = async () => {
    seTimer = null;
    try {
      await pollSe(channel);
    } catch (e) {
      seLastError = errorText(e); // every key redacted (row 5.3)
      log(`SE poll failed: ${seLastError}`);
    } finally {
      seTimer = setTimeout(run, config.streamElements.pollSeconds * 1000);
    }
  };
  run();
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
// this poll after 10 s, naming the host, instead of holding it for minutes.
async function pollSe(channel) {
  const res = await guardedFetch(`${SE_BASE}/kappa/v2/sessions/${channel}`, {
    headers: { Authorization: `Bearer ${keysInUse.jwt}`, Accept: 'application/json' },
  });
  if (!res.ok) {
    throw new Error(`StreamElements ${res.status}${res.status === 401 ? ' — JWT invalid or expired' : ''}`);
  }
  const j = res.json();
  const data = j?.data || j || {};
  seLastPollAt = new Date().toISOString();
  seLastError = '';

  let stateChanged = false;
  const announcing = [];
  for (const [metric, def] of Object.entries(SE_METRICS)) {
    const step = config.milestones[metric];
    if (!step) continue;
    let value = null;
    let key = null;
    for (const k of def.keys) {
      value = seCounterValue(data[k]);
      if (value != null) { key = k; break; }
    }
    if (value == null) continue;
    const saved = config.milestoneState[metric];
    const next = milestoneStep({ saved, savedKey: config.milestoneKeys[metric], value, key, step });
    if (config.milestoneKeys[metric] !== key) { config.milestoneKeys[metric] = key; stateChanged = true; }
    if (next.record !== undefined && next.record !== saved) {
      config.milestoneState[metric] = next.record;
      stateChanged = true;
    }
    if (next.announce !== undefined) {
      const threshold = next.announce;
      if (milestoneLogged.get(metric)?.threshold !== threshold) log(`milestone: ${metric} crossed ${threshold} → announcing`);
      announcing.push(announce(def.text(threshold)).then((outcome) => ({ metric, threshold, outcome })));
    }
  }
  // Only an announcement Streamer.bot queued moves its threshold on, in memory as well as
  // in the file (any later save writes the in-memory value). One that was dropped (the bus
  // down), refused or failed leaves it, so the next poll after a reconnect announces it.
  for (const { metric, threshold, outcome } of await Promise.all(announcing)) {
    const failedBefore = milestoneLogged.get(metric);
    if (outcome === 'queued' || (outcome === 'timed out' && MILESTONE_ADVANCES_ON_TIMEOUT)) {
      config.milestoneState[metric] = threshold;
      stateChanged = true;
      milestoneLogged.delete(metric);
      if (failedBefore) log(`milestone: the ${metric} announcement for ${threshold} was ${outcome} at last`);
    } else {
      if (failedBefore?.threshold !== threshold || failedBefore.outcome !== outcome) {
        log(`milestone: the ${metric} announcement for ${threshold} was ${outcome}; each poll announces it again until it is queued (logged once)`);
      }
      milestoneLogged.set(metric, { threshold, outcome });
    }
  }
  if (stateChanged) await persist();
}

// ── Streamer.bot WS client ────────────────────────────────────────────────────

// ── send queue: BEGIN (verify.mjs runs this block on its own, in-process) ──
// The sidecar's own DoActions (ZIP-11, decision 3): one in flight at a time. Streamer.bot
// replies to a DoAction when the action is QUEUED, not when it has run, so the reply
// alone cannot pace the next send: in its Default queue two runs of one action close
// together both see the later one's arguments (two milestones in one poll would show one
// text twice). The next send goes once the job's reply AND its own broadcast (the end of
// its run) have both arrived, in either order, or after SEND_RELEASE_MS. An error reply
// releases at once, since nothing was queued; a ticker:error while the job is in flight
// is the end of its run too. The blocking action queue the README asks for is what keeps
// a page's, chat's or a Stream Deck's run from mixing with the sidecar's; this orders only
// the sidecar's own sends.
//
// Each job ends with exactly one outcome, which the promise enqueue returns resolves with.
// The release above does not wait for it: a job released after SEND_RELEASE_MS with no
// reply keeps waiting for its reply, up to SEND_OUTCOME_MS after it was sent, while the
// next job goes, so a slow Streamer.bot that answers late is not taken for one that did
// not answer. Several jobs can be waiting for their replies at once; each reply is
// matched to its own job by its request id.
//   'queued'     an ok reply: the action was queued (it says nothing of the run); or no
//                reply yet, but the job's own broadcast arrived while it was in flight,
//                when its release comes or the socket closes (the run has happened)
//   'refused'    an error reply, such as "The action 'Ticker Push' was not found"
//   'failed'     a ticker:error arrived while it was in flight (the queue does not read
//                which action sent it, so a page's failed run at that moment, or the run
//                of an earlier job still waiting for its reply, counts against the job)
//   'timed out'  no reply within SEND_OUTCOME_MS of the send, and no broadcast of its own
//                while it was in flight
//   'dropped'    the socket was closed when it was to be sent, or closed while it waited,
//                while it was in flight with no reply and no broadcast of its own (a
//                ticker:error then counts for nothing: the close comes first), or while it
//                waited for its reply
// Every reply is passed on to be read (onReply), with whether it answers the newest job
// sent to its action: a late reply to an older send must not undo what the newest one's
// reply said (an older push's late ok after a newer push was refused).
// While a push waits, a newer push takes its place (each push is the whole payload), and
// a newer status the place of a waiting status, so a status never waits behind an old
// one; the job replaced settles with its replacement's outcome. Announcements are never
// merged or replaced. The job in flight is never replaced.
// A job's args may be a function, called when the job is sent, so a status that waited
// says what is true when it goes. `match(d)` says whether a broadcast is the job's own.
const SEND_RELEASE_MS = 800;
const SEND_OUTCOME_MS = 5000;
function createSendQueue({ send, isOpen, nextId, onReply, setTimeout, clearTimeout }) {
  let waiting = [];
  let inFlight = null;
  const awaiting = new Set(); // released with no reply: their outcome waits for it
  const byId = new Map(); // request id → job, until its reply is read (also after it settles)
  const REPLIES_KEPT = 64;
  const newestSent = new Map(); // action name → the newest job sent to it

  // A job's one timer: its release while in flight, the end of its wait for a reply after.
  function settle(job, outcome) {
    if (job.outcome) return;
    job.outcome = outcome;
    if (job.timer !== null) { clearTimeout(job.timer); job.timer = null; }
    awaiting.delete(job);
    for (const done of job.done) done(outcome);
  }
  function enqueue(spec) {
    return new Promise((resolve) => {
      const job = { ...spec, done: [resolve], id: null, replied: false, refused: false, failed: false, ended: false, timer: null, outcome: null };
      if (!isOpen()) { settle(job, 'dropped'); return; }
      const merges = job.kind === 'push' || job.kind === 'status';
      const older = merges ? waiting.findIndex((w) => w.kind === job.kind) : -1;
      if (older >= 0) { job.done.push(...waiting[older].done); waiting[older] = job; }
      else waiting.push(job);
      pump();
    });
  }
  function pump() {
    while (!inFlight && waiting.length) {
      const job = waiting.shift();
      if (!isOpen()) { settle(job, 'dropped'); continue; }
      job.id = nextId();
      const args = typeof job.args === 'function' ? job.args() : job.args;
      if (!send({ request: 'DoAction', id: job.id, action: { name: job.name }, args })) { settle(job, 'dropped'); continue; }
      inFlight = job;
      byId.set(job.id, job);
      newestSent.set(job.name, job);
      // The oldest entry goes. It is never a job still waiting for its reply: a reply removes
      // its entry, and a job with none stays in flight for SEND_RELEASE_MS, so no more than
      // about seven such jobs are sent in the SEND_OUTCOME_MS a reply is waited for, far
      // fewer than REPLIES_KEPT.
      if (byId.size > REPLIES_KEPT) byId.delete(byId.keys().next().value);
      job.timer = setTimeout(() => release(job), SEND_RELEASE_MS);
    }
  }
  // The job in flight goes, and the next is sent. Its outcome is settled now when what it
  // has had settles it; a job with no reply and no broadcast of its own waits for its reply.
  function release(job) {
    if (inFlight !== job) return;
    clearTimeout(job.timer);
    job.timer = null;
    inFlight = null;
    const outcome = job.refused ? 'refused' : job.failed ? 'failed' : (job.replied || job.ended) ? 'queued' : null;
    if (outcome) settle(job, outcome);
    else {
      awaiting.add(job);
      job.timer = setTimeout(() => settle(job, 'timed out'), SEND_OUTCOME_MS - SEND_RELEASE_MS);
    }
    pump();
  }
  // A reply to one of our DoActions; false when it is not one (the Subscribe ack). A reply
  // to a job already released settles the outcome it was waiting for; one to a job whose
  // outcome is settled (timed out, or queued by its own broadcast) is read and changes it not.
  function reply(m) {
    const id = m && m.id != null ? String(m.id) : null;
    const job = id === null ? undefined : byId.get(id);
    if (!job) return false;
    byId.delete(id);
    onReply(job.name, m, newestSent.get(job.name) === job);
    if (inFlight !== job) { settle(job, m.status === 'error' ? 'refused' : 'queued'); return true; }
    if (m.status === 'error') { job.refused = true; release(job); return true; }
    job.replied = true;
    if (job.ended) release(job);
    return true;
  }
  // A General.Custom broadcast: the end of the in-flight job's run, if it is its own.
  function broadcast(d) {
    const job = inFlight;
    if (!job || !d || typeof d !== 'object') return;
    if (d.type === 'ticker:error') job.failed = true;
    else if (!job.match(d)) return;
    job.ended = true;
    if (job.replied) release(job);
  }
  // The socket closed: every job not yet settled is, at once, and no timer is left. No
  // reply can come on a closed socket, so a job waiting for one is dropped.
  function close() {
    const job = inFlight;
    inFlight = null;
    if (job) settle(job, job.replied ? (job.failed ? 'failed' : 'queued') : (job.ended && !job.failed) ? 'queued' : 'dropped');
    for (const j of [...awaiting]) settle(j, 'dropped');
    const rest = waiting;
    waiting = [];
    for (const w of rest) settle(w, 'dropped');
    byId.clear();
    newestSent.clear();
  }
  return { enqueue, reply, broadcast, close };
}

// Which broadcast is a job's own, from what the C# actions already echo (no re-paste): a
// push by its payload's generatedAt (each is a unique millisecond, see stampNow); a status
// by the seq inside its value (Ticker Command passes the value through as it is); an
// announcement by its kind and its text, trimmed (the C# trims its arguments). A page's
// connect-sync, its commands and a Twitch announcement do not match.
// The stamp is compared as a time, never as text: Streamer.bot's WebsocketBroadcastJson
// parses the payload with Newtonsoft's default date handling and writes it out again, so a
// date-shaped string can come back in another form (measured on 1.0.7 for Greenroom:
// "…:00.000Z" comes back "…:00Z"; by the same format "…:00.120Z" would come back
// "…:00.12Z"), and as text one push in ten would never match its own broadcast.
function pushMatch(stamp) {
  const at = Date.parse(stamp);
  return (d) => d.type === 'ticker:update' && typeof d.generatedAt === 'string' && Date.parse(d.generatedAt) === at;
}
function statusMatch(seq) {
  return (d) => {
    if (d.type !== 'ticker:command' || d.command !== '__status' || typeof d.value !== 'string') return false;
    try { return JSON.parse(d.value)?.seq === seq; } catch { return false; }
  };
}
function announceMatch(kind, text) {
  const want = String(text).trim();
  return (d) => d.type === 'ticker:announce' && d.kind === kind && typeof d.text === 'string' && d.text.trim() === want;
}
// ── send queue: END ──

const sendQueue = createSendQueue({
  send: (msg) => { try { sbWs.send(JSON.stringify(msg)); return true; } catch { return false; } },
  isOpen: () => !!sbWs && sbWs.readyState === WebSocket.OPEN,
  nextId: () => String(++sbMsgId),
  onReply: readReply,
  setTimeout,
  clearTimeout,
});
const sendJob = (name, kind, args, match) => sendQueue.enqueue({ name, kind, args, match });

// Replies are read (ZIP-11), late ones too. An error reply is logged once per action until
// that action answers ok again; Ticker Push's and Ticker Announce's go in the status's
// actionNotice, which the control page shows. Ticker Command's can only be logged: the
// status itself travels on Ticker Command. Only the reply to the newest send to an action
// sets or clears its notice (`newest`, from the send queue): a late reply to an older send
// changes nothing, so an older push's late ok does not clear "Ticker Push not found" that a
// newer push's refusal set, as it does not pay that push (pushUpdate). A queueName of
// "Default" is logged once per action, from any ok reply: it is a fact of the action's
// setup, not of one send.
const actionErrors = new Map(); // action name → its notice, from an error reply until an ok one
const defaultQueueLogged = new Set();
function readReply(name, m, newest) {
  if (m.status !== 'error' && m.args && m.args.queueName === 'Default' && !defaultQueueLogged.has(name)) {
    defaultQueueLogged.add(name);
    log(`${name} runs in Streamer.bot's Default action queue; put the three bus actions in one blocking queue (README, step 1), or overlapping runs can mix their arguments`);
  }
  if (!newest) return;
  if (m.status === 'error') {
    const error = typeof m.error === 'string' ? m.error : '';
    const text = /was not found/i.test(error) ? `${name} not found` : `${name}: Streamer.bot answered with an error (${error.slice(0, 200)})`;
    if (actionErrors.get(name) === text) return;
    const first = !actionErrors.has(name);
    actionErrors.set(name, text);
    if (first) log(name === 'Ticker Command' ? `${text}: the sidecar's status cannot reach the control page` : text);
    if (name !== 'Ticker Command') pushStatus();
    return;
  }
  if (actionErrors.delete(name)) {
    log(`${name} answers again`);
    if (name !== 'Ticker Command') pushStatus();
  }
}
function actionNotice() {
  return [...actionErrors].filter(([name]) => name !== 'Ticker Command').map(([, text]) => text).join(' · ');
}

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
      // ZIP-09: a push that did not get through (Streamer.bot down, still starting, or
      // the action missing) is made good now: the payload the sidecar holds, pushed again.
      // A message-only setup never polls, so no other code path would ever put the
      // configured message on screen after a restart.
      if ((pushOwed && lastPayload) || (!polling && messageBlock())) pushCurrent();
    });
    sbWs.on('close', () => { sbWs = null; sendQueue.close(); scheduleSbReconnect(); });
    sbWs.on('error', (e) => { log(`SB WS error: ${e.message} — is SB's WebSocket Server on ${SB_WS_URL} with auth OFF?`); });
    sbWs.on('message', (raw) => {
      let m;
      try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m && m.id != null && !m.event) { sendQueue.reply(m); return; } // request acks: replies to our DoActions, and the Subscribe ack
      if (!(m && m.event && m.event.source === 'General' && m.event.type === 'Custom')) return;
      let d = m.data;
      if (typeof d === 'string') { try { d = JSON.parse(d); } catch { return; } }
      if (!d || typeof d !== 'object') return;
      sendQueue.broadcast(d); // the end of the run of the send in flight, if it is its own
      // Echo-loop guard: this client also hears its own ticker:update /
      // ticker:status broadcasts. Consume ONLY the command channel.
      if (d.type !== 'ticker:command') return;
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

// ticker:update rides the "Ticker Push" action so the C# side persists it for
// the sync-on-connect replay. The newest push, when it ends any way but "queued", is owed (ZIP-09):
// the current payload is pushed again when the sidecar next connects, or, while the socket
// stays open, PUSH_RETRY_MS later and on the next `status` command (payOwedPush); the next
// poll pushes whatever it brings (lastHash is cleared). A retry refused again stays owed,
// and "Ticker Push not found" is still logged only once (readReply).
// Each push acts on its own settled outcome, which can come up to SEND_OUTCOME_MS after it
// was sent, after a newer push was made (several can be waiting for their replies at once).
// Only the newest push's outcome counts: once a newer push has been made, it carries the
// whole payload and its outcome, whatever it is, settles what is owed (every push settles,
// at the latest SEND_OUTCOME_MS after it is sent, or when the socket closes). So a push that
// times out while a newer one is queued or still waiting for its reply owes nothing, logs
// nothing and leaves lastHash alone, and a late "queued" for an older push does not pay a
// newer one that was refused.
let pushSeq = 0;
function pushUpdate(payload) {
  const seq = ++pushSeq;
  // Sent with every key in the caps and the message replaced by its label (R6); the
  // payload kept (lastPayload) stays as typed.
  sendJob('Ticker Push', 'push', { tickerPayload: JSON.stringify(shownPayload(payload)) }, pushMatch(payload.generatedAt)).then((outcome) => {
    if (seq !== pushSeq) return; // a newer push has been made: its outcome counts, not this one's
    if (outcome === 'queued') {
      pushOwed = false;
      if (pushRetryTimer) { clearTimeout(pushRetryTimer); pushRetryTimer = null; }
      return;
    }
    lastHash = null;
    if (!pushOwed) log(`Ticker Push ${outcome}: the current payload is pushed again when the sidecar next connects to Streamer.bot, or in ${seconds(pushRetryMs)} while it stays connected`);
    pushOwed = true;
    if (!pushRetryTimer) pushRetryTimer = setTimeout(() => { pushRetryTimer = null; payOwedPush(); }, pushRetryMs);
  });
}

// Pays an owed push now, if the socket is open (a closed one pays it on connect).
function payOwedPush() {
  if (pushOwed && lastPayload && sbWs && sbWs.readyState === WebSocket.OPEN) pushCurrent();
}

// A milestone announcement. Resolves with the send's outcome (see the send queue).
function announce(text) {
  return sendJob('Ticker Announce', 'announce', { kind: 'milestone', text, duration: '10' }, announceMatch('milestone', text));
}

// ticker:status is ephemeral (never persisted) — it rides the "Ticker Command"
// relay with the reserved command "__status"; the C# action broadcasts
// { type:'ticker:command', command:'__status', value:'<status JSON>' } and the
// control page unwraps it. dispatch() below ignores it (echo guard). Its `seq` tells
// the send queue which broadcast is this status's own. It is built when it is sent.
let statusSeq = 0;
function pushStatus() {
  const seq = ++statusSeq;
  sendJob('Ticker Command', 'status', () => ({ command: '__status', value: JSON.stringify(statusSnapshot(seq)) }), statusMatch(seq));
}

function statusSnapshot(seq) {
  return {
    type: 'ticker:status',
    seq,
    polling,
    url: shownUrl(config.tournamentUrl), // a key in it redacted (row 5.3)
    platform: platformName,
    lastPollAt,
    // When the next poll runs (ZIP-13): the interval, the backoff, a Retry-After or the
    // minimum gap, whichever is latest; null while none is armed (polling off, or a poll
    // in flight). Unlike auto.waitingUntil it does not mean the page should show WAITING.
    nextPollAt: pollDueAt !== null ? wallTime(pollDueAt) : null,
    lastError,
    commandNotice,
    // "Ticker Push not found" or "Ticker Announce not found", from the sidecar's own sends,
    // until that action answers ok (row 4.2); "" otherwise.
    actionNotice: actionNotice(),
    // A config.json that did not load, a saved logo dropped, a refused key, saves stopped
    // after a hand edit, or a failed write (rows 5.1 to 5.4); "" otherwise. A good poll
    // and setUrl never clear it.
    configWarning: configWarning(),
    // A non-fatal fact about the last good poll (row 8.3), every key redacted; "" otherwise.
    pollNotice,
    intervalMs: config.pollIntervalMs,
    maxItems: config.maxItems,
    topN: config.topN,
    // A key typed into the caps or the message redacted (R6); config keeps it as typed.
    caps: shownCaps(config.caps),
    message: shownMessage(config.message),
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
// without regard to case (ZIP-08): a Ticker Command pasted before the chat list
// lower-cases a dedicated chat command, so "!pollnow" arrives as "pollnow" (the current
// one relays the chat list's own spelling, "pollNow"). The cases below keep the camelCase
// names docs/PROTOCOL.md uses.
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
  // Redacted before it is cut (row 5.3): a key sent as a name is never shown, even in part.
  const shown = shownName(redact(command));
  // The status probe, which every open control page sends every 10 s, is not logged
  // (row 5.2: about 8,640 lines a day per page in the log file). The value is redacted
  // before it is cut to 120 characters (row 5.3), so the cut can fall only inside a
  // label, never inside a key: redact() matches a cut key only by 8 characters or more.
  if (folded !== 'status') log(`command: ${shown}${value ? ' ' + redact(value).slice(0, 120) : ''}`);
  const name = COMMANDS.get(folded);
  if (!name) { refuse(`unknown command "${shown}" — ignored`); return; }
  if (name !== 'status') commandNotice = ''; // an accepted command clears the last notice
  switch (name) {
    case 'setUrl': {
      // An address pasted without https:// is given it before anything else looks at it
      // (decision 11 d), so it is detected, compared with the URL set, saved and shown as
      // https://… . A start.gg admin page's address is kept as typed: the adapter reads
      // past its "/admin" (decision 11 c).
      const url = strOrNull(normalizeInput(value));
      // A refused URL is no URL change: the old one, its platform and its state stay,
      // nothing is saved, and no poll follows.
      const refusal = url && url.startsWith('stub:') ? stubRefusal(url) : null;
      if (refusal) { refuse(refusal); return; }
      if (url === config.tournamentUrl) {
        // The URL already set, after trimming (row 4.6, a choice left open; see the sprint
        // notes). The control page's Set button sends setUrl on every click, so this is
        // not a new tournament: the strip is not blanked (after an auto-stop on a completed
        // event no poll would come to replace a blank), the result kept and the event's
        // state stay, and a poll in flight goes on. While polling it asks for a poll, as
        // pollNow does; with polling off it does nothing but answer.
        if (polling) requestPoll(true);
        pushStatus();
        return;
      }
      config.tournamentUrl = url;
      // ZIP-02: a poll in flight now belongs to the old URL. It is stopped, and whatever
      // it brings back is thrown away; the new URL is polled once the gap allows.
      urlGeneration++;
      if (pollAbort) pollAbort.abort(new Error('the tournament URL changed'));
      cancelOneOff(); // a fetch a command asked for was for the old URL
      platformName = platformFor(config.tournamentUrl);
      // A Retry-After hold is left as it is: it belongs to its platform (see holdUntil).
      lastError = '';
      pollNotice = ''; // it was about the old URL's last good poll
      consecutiveErrors = 0; // the new URL does not inherit the old one's backoff
      eventState = null; // new tournament — forget the old one's state/schedule
      eventStartAt = null;
      autoStopped = null;
      waitingUntil = null;
      // Decision 5 (ZIP-22): the old bracket leaves the strip at once, and stays gone if
      // the new URL keeps failing. The result kept for the old URL is dropped, and a blank
      // payload, caps and message kept, is pushed and becomes the current one, so cap and
      // option changes rebuild from nothing until the new URL's first good poll.
      lastRaw = null;
      lastPayload = blankPayload();
      pushCurrent();
      lastHash = null; // the new URL's first good poll is pushed, whatever it holds
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
      persist();
      // Caps travel in the update payload: pushed at once, polling or not (showCaps).
      showCaps();
      pushStatus();
      return;
    case 'setCapsLogo': {
      const logo = checkLogo(value);
      if (logo === null) { refuse(LOGO_REFUSED_MESSAGE); return; } // the logo is kept
      config.caps.logo = logo;
      persist();
      showCaps();
      pushStatus();
      return;
    }
    case 'setTopN': {
      const n = clampInt(value, 0, 100, config.topN);
      const changed = n !== config.topN;
      config.topN = n;
      persist();
      // Rebuilt from the last result and pushed at once (applyOptions); a value already set
      // changes nothing on the strip.
      if (changed) applyOptions();
      pushStatus();
      return;
    }
    case 'setMaxItems': {
      const n = clampInt(value, 1, 100, config.maxItems);
      const changed = n !== config.maxItems;
      config.maxItems = n;
      persist();
      if (changed) applyOptions();
      pushStatus();
      return;
    }
    // Batched setters — the control page sends ONE of these per button so it
    // never fires a burst of DoActions (Streamer.bot bleeds arguments between
    // action invocations that arrive within a few ms, which corrupted the caps
    // text). Value is a JSON object. The per-field commands above stay for
    // Stream Deck, where one action fires at a time.
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
      persist();
      showCaps(); // caps travel in the update payload: pushed at once, polling or not
      pushStatus();
      return;
    }
    // Persistent message mode — batched (text + enabled in one command) for the
    // control page; the single-field variants below are for Stream Deck.
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
      // The control page sends all three on every Apply. A changed maxItems or topN
      // rebuilds the payload from the last result and pushes it at once (applyOptions),
      // as setMaxItems and setTopN do. A new interval is a reschedule through the one
      // next-delay function, so it cannot cut a longer error backoff or a Retry-After
      // short (ZIP-13).
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
      scheduleRegular();
      if (rebuild) applyOptions();
      pushStatus();
      return;
    }
    case 'status':
      // The control page asks every 10 s while it is open: a push still owed (Ticker Push
      // was missing and has been added since, with the socket kept open) is paid here.
      payOwedPush();
      pushStatus();
      return;
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────

// One line, every key in it redacted (row 5.3), to the console and appended to the log file
// (row 5.2). The file write is synchronous, so a line logged just before process.exit is in
// the file; the console write stays as Node makes it (not blocking on a console window,
// which a console in QuickEdit mode would freeze). It never throws: the log is a help,
// never a reason to stop.
let logTrims = false; // set once this instance holds the guard port, or runs without it
function log(msg) {
  try {
    const line = `[${new Date().toISOString()}] [zipper] ${redact(msg)}`;
    try { console.log(line); } catch { /* no console */ }
    try { appendFileSync(LOG_PATH, `${line}\n`, 'utf-8'); } catch { /* no log file */ }
    if (logTrims) trimLog();
  } catch { /* nothing to do */ }
}

// Only the instance that keeps running trims the log (row 5.2): at start, and after any
// line that takes it past LOG_MAX_BYTES (while Streamer.bot is down a line is logged on
// every reconnect). A second launch only adds its one line, the guard port's.
function trimLog() {
  try {
    if (statSync(LOG_PATH).size <= LOG_MAX_BYTES) return;
    renameSync(LOG_PATH, LOG_OLD_PATH);
  } catch {
    return;
  }
  log(`the log passed ${LOG_MAX_BYTES / 1048576} MB: the lines before this one are in ${basename(LOG_OLD_PATH)}`);
}

// The crash handlers (row 5.2): the error is logged, to the file too, and the sidecar exits
// non-zero, as Node would, but with a line where a hidden launch can show nothing.
function onCrash(kind) {
  return (err) => {
    log(`crash: ${kind}: ${err?.stack || err?.message || String(err)}. The sidecar stops; start it again (start-zipper.bat, or the Ticker Sidecar Start action).`);
    process.exit(1);
  };
}

function acquireGuard() {
  return new Promise((res) => {
    // A connection to the guard port (a launcher checking it, say) is closed at once, and
    // its socket's errors are ignored, so it cannot raise an unhandled error.
    const guard = createServer((socket) => { socket.on('error', () => {}); socket.destroy(); });
    guard.once('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        log(`Another Zipper sidecar is already running (guard port ${GUARD_PORT} is in use), so this one exits. If none is running, another program holds that port: set ZIPPER_GUARD_PORT to a free one.`);
        process.exit(0);
      }
      log(`guard port error: ${err.message} — continuing without the single-instance guard`);
      res();
    });
    guard.listen(GUARD_PORT, '127.0.0.1', () => { guardHeld = true; guard.on('error', () => {}); res(); });
  });
}

async function main() {
  // First, before the guard and the config (row 5.2).
  process.on('uncaughtException', onCrash('an uncaught exception'));
  process.on('unhandledRejection', onCrash('an unhandled promise rejection'));
  await acquireGuard();
  // From here this instance runs (with the guard port, or without it when the port could
  // not be opened): it trims the log (the line below is the first to, so a log already over
  // the limit is trimmed at start). Only with the guard port does it remove a temp file a
  // crash left (loadConfig).
  logTrims = true;
  log(`Zipper sidecar starting (config: ${CONFIG_PATH}; log: ${LOG_PATH})`);
  // The limits in effect: the full ones on a normal install, whatever the test settings say
  // (they apply only with ZIPPER_ALLOW_STUB=1).
  log(`limits: ${seconds(requestTimeoutMs())} per request, ${seconds(pollDeadlineMs)} per poll, ${seconds(minPollGapMs)} between polls, ${seconds(pushRetryMs)} before an owed push is retried${STUB_ALLOWED ? ' (ZIPPER_ALLOW_STUB=1: the test settings apply)' : ''}`);
  await loadConfig();
  connectSb();
  startSePolling();
  if (config.autoStart && config.tournamentUrl) startPolling();
  const shutdown = () => {
    log('shutting down');
    if (pollTimer) clearTimeout(pollTimer);
    if (seTimer) clearTimeout(seTimer);
    if (pushRetryTimer) clearTimeout(pushRetryTimer);
    try { sbWs?.close(); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main();
