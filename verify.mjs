// Zipper verify suite — no network, no Streamer.bot install, no browser.
//   1. Platform normalizer tests against recorded fixtures (pure functions).
//   2. Platform detection/parseId routing.
//   2b. The fetch wrapper (platforms/guarded-fetch.js), in-process against a local
//      server on an OS-assigned port: its timeout, size cap, Retry-After and error
//      wording; each adapter's wiring to it (signal, 429, User-Agent) against a
//      stand-in for fetch; and a static check that no bare fetch( is left in the
//      sidecar or an adapter.
//   3. Mock-SB E2E: the REAL panel-core.js runs in a browser shim against the
//      mock server — Subscribe (lowercase), sync-on-connect replay, live push.
//   4. The mock itself: its HTTP maps, DoAction replies in Streamer.bot's shape,
//      its test switches, and what it refuses (bad requests, other sites, messages
//      over 1 MiB), and the file types it serves.
//   5. The three bus actions as the mock runs them, against the case table
//      tools/cs/ticker-cases.json (verify-cs.mjs runs the same rows through the
//      compiled C#).
//   6. Sidecar E2E: the REAL zipper-sidecar.mjs runs against the mock with the
//      stub: platform — command round-trips, payload shape, diff suppression,
//      caps re-push, auto-scheduling (stop on completed / wait for start), a stub
//      poll held open and released, every command in docs/PROTOCOL.md (as named
//      there, and lower-cased), every range on the command path and the config path,
//      and which commands it accepts: stub: URLs only with ZIPPER_ALLOW_STUB=1 and
//      only inside the repo, the logo rule, the __status guard in any casing, and
//      refusals reported in commandNotice.
//   6h. The poll loop (ZIP-02, ZIP-06, ZIP-13): a URL changed mid-poll, a stale
//      failure, a result, failure or 429 that comes back after setUrl (a hold that
//      ignores the abort), the poll's deadline, the backoff, Retry-After, the minimum gap
//      between polls, re-timing by setInterval, setOptions and setAutoSchedule, the
//      next poll time in the status, and a wall clock set back (a sidecar with
//      tools/clock-step-preload.cjs).
//   7. Milestone announcements: a real sidecar against a mock StreamElements, and a
//      StreamElements request that never answers.
//   8. start-zipper.bat's Node version check (Windows only), with a stand-in node.
//
// Uses off-default ports (HTTP 7476 / WS 8082 / sidecar guards 7497, 7492 and 7487 /
// StreamElements mock 7477) so it can run next to a live Streamer.bot or sidecar,
// and stops with one line, testing nothing, when anything already answers on them.
// The stub fixtures and the test sidecars' config files are written to .verify-tmp/
// inside the repo (gitignored), which the suite clears when it starts and removes when
// it ends. Every child process is stopped when the suite ends.
// Run: npm run verify

import { spawn, spawnSync } from 'node:child_process';
import { readFile, writeFile, rm, mkdir } from 'node:fs/promises';
import { existsSync, rmSync, mkdirSync, writeFileSync, readFileSync, readdirSync } from 'node:fs';
import { resolve, dirname, join, sep, basename, win32 } from 'node:path';
import { fileURLToPath } from 'node:url';
import { connect } from 'node:net';
import WebSocket from 'ws';

import { detectPlatform, PLATFORMS } from './platforms/index.js';
import { normalizeChallongeApi, normalizeChallongeStore } from './platforms/challonge.js';
import { normalizeStartggEvent } from './platforms/startgg.js';
import { normalizeMatcherino } from './platforms/matcherino.js';
import { loadTickerCases } from './tools/cs/ticker-cases.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIX = (f) => resolve(__dirname, 'platforms', 'fixtures', f);
const HTTP_PORT = 7476;
const WS_PORT = 8082;
const GUARD_PORT = 7497;
// NOT 7498/7499 — those are the live Octet sidecar + Octet's verify.
const SE_GUARD_PORT = 7492;
// The guard port of the sidecar started WITHOUT ZIPPER_ALLOW_STUB (section 6g). Outside
// 7490-7499, which the plugin family's sidecars and suites hold, and named by no other
// plugin.
const NOSTUB_GUARD_PORT = 7487;
const SE_PORT = 7477;
const HTTP = `http://127.0.0.1:${HTTP_PORT}`;
const WS_URL = `ws://127.0.0.1:${WS_PORT}/`;
// The suite's own fixtures, inside the repo, named in stub: URLs relative to it.
const TMP_REL = '.verify-tmp';
const TMP_DIR = resolve(__dirname, TMP_REL);
// Files for the file-type checks, served by the mock through /zipper-overlay/.
const TYPES_DIR = resolve(__dirname, 'overlay', '.verify-tmp');

let pass = 0, fail = 0;
function check(name, ok, detail) {
  if (ok) { pass++; console.log(`  ✔ ${name}`); }
  else { fail++; console.log(`  ✘ ${name}${detail ? ` — ${detail}` : ''}`); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const loadFixture = async (f) => JSON.parse(await readFile(FIX(f), 'utf-8'));

// Polls a condition until it gives something truthy, or the time is up. Returns what
// it gave, or null: the caller's check then fails with its own line.
async function waitFor(fn, timeoutMs = 4000, stepMs = 25) {
  const end = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() >= end) return null;
    await sleep(stepMs);
  }
}

// ── 0. Ports and child processes ──────────────────────────────────────────────

function portAnswers(port) {
  return new Promise((res) => {
    const s = connect({ host: '127.0.0.1', port });
    const done = (v) => { s.destroy(); res(v); };
    s.once('connect', () => done(true));
    s.once('error', () => done(false));
    s.setTimeout(1500, () => done(false));
  });
}
{
  const busy = [];
  for (const port of [HTTP_PORT, WS_PORT, GUARD_PORT, SE_GUARD_PORT, NOSTUB_GUARD_PORT, SE_PORT]) if (await portAnswers(port)) busy.push(port);
  if (busy.length) {
    console.error(`verify: something already answers on 127.0.0.1 port ${busy.join(', ')} (most likely a mock or a test sidecar left by an earlier run); stop it and run again. Nothing was tested.`);
    process.exit(1);
  }
}

// Every child this suite starts is stopped when it ends: normally, on a crash, or on
// a signal. The mock also exits by itself when this process goes (MOCK_EXIT_WITH_PARENT).
const children = new Set();
function track(child) { children.add(child); child.on('exit', () => children.delete(child)); return child; }
function stopChildren() { for (const c of children) { try { c.kill(); } catch {} } }
function removeTmp() {
  try { rmSync(TMP_DIR, { recursive: true, force: true }); } catch {}
  try { rmSync(TYPES_DIR, { recursive: true, force: true }); } catch {}
}
process.on('exit', () => { stopChildren(); removeTmp(); });
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGBREAK']) process.on(sig, () => { stopChildren(); process.exit(130); });

// ── 1. Normalizers ────────────────────────────────────────────────────────────

async function testNormalizers() {
  console.log('\n[1] platform normalizers (fixtures)');

  const api = normalizeChallongeApi(await loadFixture('challonge-api-tournament.json'));
  check('challonge api: tournament name', api.tournamentName === 'BLKO Weekly #42', api.tournamentName);
  check('challonge api: 5 matches', api.matches.length === 5, String(api.matches.length));
  const done = api.matches.filter((m) => m.state === 'completed');
  check('challonge api: 2 completed', done.length === 2, String(done.length));
  check('challonge api: winner resolved', done[0].winner === 1 && done[0].p1.name === 'Flash', JSON.stringify(done[0]));
  check('challonge api: scores parsed', done[0].p1.score === 2 && done[0].p2.score === 0, JSON.stringify(done[0]));
  const losers = api.matches.find((m) => m.id === '9004');
  check('challonge api: losers round label', /^Losers/.test(losers.round), losers.round);
  const gf = api.matches.find((m) => m.id === '9005');
  check('challonge api: grand final label', gf.round === 'Grand Final', gf.round);
  check('challonge api: TBD names + null scores on upcoming', gf.p1.name === 'TBD' && gf.p1.score === null, JSON.stringify(gf));
  check('challonge api: final_rank → standings sorted',
    api.standings.length === 4 && api.standings[0].name === 'Flash' && api.standings[2].name === 'Newbie',
    JSON.stringify(api.standings));

  const store = normalizeChallongeStore(await loadFixture('challonge-module-store.json'));
  check('challonge store: tournament name', store.tournamentName === 'BLKO Weekly #42', store.tournamentName);
  check('challonge store: 4 matches', store.matches.length === 4, String(store.matches.length));
  const sDone = store.matches.filter((m) => m.state === 'completed');
  check('challonge store: 2 completed w/ winners', sDone.length === 2 && sDone.every((m) => m.winner === 1), JSON.stringify(sDone));
  check('challonge store: scores from scores_csv', sDone[1].p1.score === 2 && sDone[1].p2.score === 1, JSON.stringify(sDone[1]));
  check('challonge store: array-form scores', sDone[0].p1.score === 2 && sDone[0].p2.score === 0, JSON.stringify(sDone[0]));
  const sLive = store.matches.find((m) => m.state === 'in_progress');
  check('challonge store: open match live 1-1', sLive && sLive.p1.score === 1 && sLive.p2.score === 1, JSON.stringify(sLive));
  check('challonge store: mid-tournament → no derived standings', store.standings.length === 0, JSON.stringify(store.standings));
  const doneStore = normalizeChallongeStore(await loadFixture('challonge-module-store-complete.json'), 'Done Cup');
  check('challonge store: derived standings on finished bracket',
    doneStore.standings.length === 4
      && doneStore.standings.map((s) => `${s.placement}:${s.name}`).join(',') === '1:Alpha,2:Charlie,3:Bravo,4:Delta',
    JSON.stringify(doneStore.standings));
  const noName = await loadFixture('challonge-module-store.json');
  delete noName.tournament;
  const titled = normalizeChallongeStore(noName, 'Solution to Sunday 7');
  check('challonge store: page-title name fallback', titled.tournamentName === 'Solution to Sunday 7', titled.tournamentName);

  const gg = normalizeStartggEvent(await loadFixture('startgg-event-sets.json'));
  check('startgg: combined tournament — event name', gg.tournamentName === 'Solution to Sunday 9 — SF6 Singles', gg.tournamentName);
  check('startgg: 3 matches', gg.matches.length === 3, String(gg.matches.length));
  const ggDone = gg.matches.find((m) => m.state === 'completed');
  check('startgg: completed set winner=1 2-0', ggDone && ggDone.winner === 1 && ggDone.p1.score === 2 && ggDone.p2.score === 0, JSON.stringify(ggDone));
  const ggLive = gg.matches.find((m) => m.state === 'in_progress');
  check('startgg: live set 1-1, round text', ggLive && ggLive.p1.score === 1 && ggLive.round === 'Winners Final', JSON.stringify(ggLive));
  const ggUp = gg.matches.find((m) => m.state === 'upcoming');
  check('startgg: null entrant → TBD, null scores', ggUp && ggUp.p2.name === 'TBD' && ggUp.p1.score === null, JSON.stringify(ggUp));

  const ggCreated = normalizeStartggEvent({ name: 'E', state: 'CREATED', tournament: { name: 'T' }, sets: { nodes: [] } });
  check('startgg: string CREATED state → upcoming', ggCreated.state === 'upcoming' && ggCreated.matches.length === 0, ggCreated.state);
  check('startgg: numeric state still maps', gg.state === 'in_progress', gg.state);
  check('startgg: missing startAt → null', ggCreated.startAt === null, String(ggCreated.startAt));
  const ggTimed = normalizeStartggEvent({ name: 'E', state: 'CREATED', startAt: 1754006400, tournament: { name: 'T' }, sets: { nodes: [] } });
  check('startgg: startAt epoch-seconds → ISO', ggTimed.startAt === new Date(1754006400 * 1000).toISOString(), String(ggTimed.startAt));
  check('challonge api: missing start_at → null startAt', api.startAt === null, String(api.startAt));
  const timedApi = await loadFixture('challonge-api-tournament.json');
  timedApi.tournament.start_at = '2026-08-01T20:00:00-04:00';
  check('challonge api: start_at → ISO startAt',
    normalizeChallongeApi(timedApi).startAt === new Date('2026-08-01T20:00:00-04:00').toISOString(),
    String(normalizeChallongeApi(timedApi).startAt));

  const pendingStore = await loadFixture('challonge-module-store.json');
  for (const round of Object.values(pendingStore.matches_by_round)) {
    for (const m of round) { m.state = 'pending'; m.winner_id = null; delete m.scores; delete m.scores_csv; }
  }
  const pending = normalizeChallongeStore(pendingStore);
  check('challonge store: all-pending bracket → upcoming state',
    pending.state === 'upcoming' && pending.matches.every((m) => m.state === 'upcoming' && m.p1.score === null),
    pending.state);

  const mat = normalizeMatcherino(await loadFixture('matcherino-bounty.json'));
  check('matcherino: standings only', mat.matches.length === 0 && mat.standings.length === 3, JSON.stringify(mat.standings));
  check('matcherino: sorted by placement', mat.standings[0].placement === 1 && mat.standings[0].name === 'Flash', JSON.stringify(mat.standings[0]));
}

// ── 2. Detection ──────────────────────────────────────────────────────────────

function testDetection() {
  console.log('\n[2] platform detection');
  const cases = [
    ['https://challonge.com/blko42', 'challonge', 'blko42'],
    ['https://blko.challonge.com/weekly42', 'challonge', 'blko-weekly42'],
    ['https://www.start.gg/tournament/sol9/event/sf6-singles', 'startgg', 'sol9/sf6-singles'],
    ['https://www.start.gg/tournament/sol9/events/sf6-singles/brackets/1/2', 'startgg', 'sol9/sf6-singles'],
    ['https://matcherino.com/tournaments/424242', 'matcherino', '424242'],
    ['https://round.one/tournament/my-cup', 'roundone', 'my-cup'],
    ['https://tourneybot.gg/tourneys/1234', 'tourneybot', '1234'],
    ['blko42', 'challonge', 'blko42'],
  ];
  for (const [input, name, id] of cases) {
    const p = detectPlatform(input);
    check(`detect ${input}`, p?.name === name && p.parseId(input) === id,
      p ? `${p.name}/${p.parseId(input)}` : 'no match');
  }
  check('all platforms expose fetchTicker', PLATFORMS.every((p) => typeof p.fetchTicker === 'function'));
}

// ── 2b. The fetch wrapper, in-process (ZIP-06), and no bare fetch( left ───────
// Decision 9: the wrapper is tested in this process against a local node:http server
// that never answers (or answers too much, or says "slow down"). The server listens on
// a port the OS assigns, so it can clash with nothing. No tournament adapter is driven
// end to end through the network here; their wiring to the wrapper is checked in this
// process against a stand-in for fetch, and StreamElements end to end in section 7.

async function testFetchWrapper() {
  console.log('\n[2b] the fetch wrapper (platforms/guarded-fetch.js), in-process; no bare fetch( left');

  // The static check: every request in the sidecar and the adapters goes through the
  // wrapper. A call to fetch( anywhere else in those files fails it, line by line.
  {
    const files = ['zipper-sidecar.mjs', ...readdirSync(resolve(__dirname, 'platforms'))
      .filter((f) => f.endsWith('.js') && f !== 'guarded-fetch.js').map((f) => `platforms/${f}`)];
    const bare = [];
    for (const f of files) {
      readFileSync(resolve(__dirname, f), 'utf-8').split(/\r?\n/).forEach((line, i) => {
        if (/\bfetch\s*\(/.test(line)) bare.push(`${f}:${i + 1}`);
      });
    }
    check(`no bare fetch( in zipper-sidecar.mjs or platforms/*.js outside the wrapper (${files.length} files)`,
      files.length >= 7 && bare.length === 0, bare.join(', '));
    const uaOld = files.filter((f) => /Zipper\/0\.1/.test(readFileSync(resolve(__dirname, f), 'utf-8')));
    check('no adapter sends the old fixed User-Agent "Zipper/0.1" (the wrapper module gives the package version)', uaOld.length === 0, uaOld.join(', '));
    // The README's promise about a site's Retry-After gives the cap the sidecar applies:
    // MAX_BACKOFF_MS, or the interval when that is longer. It once said "never before".
    const sidecarSrc = readFileSync(resolve(__dirname, 'zipper-sidecar.mjs'), 'utf-8');
    const capMs = Number((/const MAX_BACKOFF_MS = (\d+);/.exec(sidecarSrc) || [])[1]);
    const readme = readFileSync(resolve(__dirname, 'README.md'), 'utf-8').replace(/\s+/g, ' ');
    const said = (/[^.]*"too many requests"[^.]*\./.exec(readme) || [''])[0];
    check('docs: the README\'s sentence on a site\'s "too many requests" wait names the cap the sidecar applies (MAX_BACKOFF_MS, 5 minutes, or the poll interval if that is longer)',
      capMs === 300000 && /up to 5 minutes, or the poll interval if that is longer/.test(said) && !/never before/.test(said), JSON.stringify({ capMs, said }));
  }

  let mod = null;
  try { mod = await import('./platforms/guarded-fetch.js'); }
  catch (err) { check('the fetch wrapper module platforms/guarded-fetch.js loads', false, err?.message || String(err)); return; }
  check('the fetch wrapper module platforms/guarded-fetch.js loads', typeof mod.guardedFetch === 'function'
    && typeof mod.statusError === 'function' && typeof mod.parseRetryAfter === 'function' && Number.isFinite(mod.MAX_RESPONSE_BYTES),
    Object.keys(mod).join(', '));
  const { guardedFetch, statusError, parseRetryAfter, MAX_RESPONSE_BYTES: CAP } = mod;
  const pkgVersion = JSON.parse(readFileSync(resolve(__dirname, 'package.json'), 'utf-8')).version;
  check('the User-Agent strings carry the package version: "Zipper/<version>", and "Mozilla/5.0 (compatible; Zipper/<version>)" for the Challonge embed page',
    mod.USER_AGENT === `Zipper/${pkgVersion}` && mod.BROWSER_USER_AGENT === `Mozilla/5.0 (compatible; Zipper/${pkgVersion})`,
    JSON.stringify({ USER_AGENT: mod.USER_AGENT, BROWSER_USER_AGENT: mod.BROWSER_USER_AGENT }));

  // The override of the 10 seconds, as the sidecar reads it: it can only shorten it.
  const envBefore = process.env.ZIPPER_REQUEST_TIMEOUT_MS;
  const setLimit = (v) => { if (v === undefined) delete process.env.ZIPPER_REQUEST_TIMEOUT_MS; else process.env.ZIPPER_REQUEST_TIMEOUT_MS = v; };
  {
    const got = [];
    for (const [v, want] of [[undefined, 10000], ['999999', 10000], ['1', 50], ['abc', 10000], ['300', 300]]) {
      setLimit(v);
      const ms = mod.requestTimeoutMs();
      if (ms !== want) got.push(`${v} → ${ms}, wanted ${want}`);
    }
    check('ZIPPER_REQUEST_TIMEOUT_MS is clamped to 50–10000 ms (a test can only shorten the 10 seconds); unset or not a number gives 10000',
      got.length === 0, got.join('; '));
    setLimit(undefined); // the checks below run on the 10 seconds unless they shorten it
  }

  const { createServer } = await import('node:http');
  const sockets = new Set();
  const held = [];
  let neverHits = 0;
  const server = createServer((req, res) => {
    const path = new URL(req.url, 'http://x').pathname;
    const stream = (status, bytes) => {
      res.writeHead(status, { 'content-type': 'application/octet-stream' });
      const chunk = Buffer.alloc(1 << 20, 0x61);
      let sent = 0;
      const pump = () => {
        while (sent < bytes) {
          const part = bytes - sent < chunk.length ? chunk.subarray(0, bytes - sent) : chunk;
          sent += part.length;
          if (!res.write(part)) { res.once('drain', pump); return; }
        }
        res.end();
      };
      pump();
    };
    switch (path) {
      case '/never': neverHits++; held.push(res); return; // accepted, never answered
      case '/trickle': { // headers at once, then a byte every 100 ms, for ever
        res.writeHead(200, { 'content-type': 'text/plain' });
        res.write('x');
        const t = setInterval(() => { try { res.write('x'); } catch {} }, 100);
        res.on('close', () => clearInterval(t));
        return;
      }
      case '/big-length': res.writeHead(200, { 'content-length': String(CAP + 1) }); res.flushHeaders(); held.push(res); return; // the body never comes
      case '/big-chunked': stream(200, CAP + 1); return;
      case '/big-500': stream(500, CAP + 1); return;
      case '/ok': res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: true, ua: req.headers['user-agent'] ?? null })); return;
      case '/429s': res.writeHead(429, { 'retry-after': '120' }); res.end('slow down'); return;
      case '/429d': res.writeHead(429, { 'retry-after': new Date(Date.now() + 120000).toUTCString() }); res.end(); return;
      case '/503s': res.writeHead(503, { 'retry-after': '7' }); res.end(); return;
      default: res.writeHead(404); res.end();
    }
  });
  server.on('connection', (s) => { sockets.add(s); s.on('close', () => sockets.delete(s)); });
  await new Promise((res, rej) => { server.once('error', rej); server.listen(0, '127.0.0.1', res); });
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}`;
  const host = `127.0.0.1:${port}`;
  // Runs one request and says how it ended and how long it took. The test's own bound
  // ends the wait, so a request the wrapper fails to stop (its timer cleared too soon, a
  // signal not joined) fails its own check with a line instead of hanging the suite:
  // /trickle and /never never end by themselves.
  const ATTEMPT_BOUND_MS = 8000;
  const attempt = async (path, opts) => {
    const t0 = Date.now();
    let bound = null;
    const outcome = await Promise.race([
      guardedFetch(`${base}${path}`, opts).then((r) => ({ r }), (err) => ({ err })),
      new Promise((res) => { bound = setTimeout(() => res({ err: new Error(`test: the request neither ended nor failed within ${ATTEMPT_BOUND_MS / 1000} s`) }), ATTEMPT_BOUND_MS); }),
    ]);
    clearTimeout(bound);
    return { ...outcome, ms: Date.now() - t0 };
  };

  try {
    {
      const { r, err } = await attempt('/ok', { headers: { 'User-Agent': mod.USER_AGENT, Accept: 'application/json' } });
      let body = null; try { body = r?.json(); } catch {}
      check('wrapper: an ordinary answer comes back with its status, its text and json(), and the headers given (the User-Agent here)',
        !err && r?.status === 200 && r.ok === true && typeof r.text === 'string' && body?.ok === true && body?.ua === mod.USER_AGENT,
        err ? err.message : JSON.stringify({ status: r?.status, body }));
    }

    setLimit('300');
    {
      const { err, ms } = await attempt('/never?api_key=SECRET-CANARY&x=1');
      const msg = err?.message || '';
      check('wrapper: a server that never answers times out at the limit (shortened to 0.3 s by its override); the message names the host and the limit, never the path or the query',
        msg === `${host} took longer than 0.3 s to answer` && ms >= 250 && ms < 3000 && !/SECRET|api_key|never|\?/.test(msg),
        JSON.stringify({ message: msg, ms }));
    }
    {
      const { err, ms } = await attempt('/trickle');
      check('wrapper: the timer stays armed until the body has been read, so a body that trickles in times out the same way',
        err?.message === `${host} took longer than 0.3 s to answer` && ms < 3000, JSON.stringify({ message: err?.message, ms }));
    }
    setLimit(undefined);

    const capText = `${host} sent more than ${CAP / 1048576} MB`;
    {
      const { err, ms } = await attempt('/big-length');
      check(`wrapper: a Content-Length over the cap (${CAP} bytes) fails at once, without waiting for the body; the message names the host and the limit`,
        err?.message === capText && ms < 3000, JSON.stringify({ message: err?.message, ms }));
    }
    {
      const { err } = await attempt('/big-chunked');
      check('wrapper: a body with no length that runs past the cap fails the same way', err?.message === capText, JSON.stringify({ message: err?.message }));
    }
    {
      const { r, err } = await attempt('/big-500');
      check('wrapper: an error answer is returned with its status (the adapter words the error); a body over the cap is cut at the cap rather than failing',
        !err && r?.status === 500 && r.ok === false && r.text.length === CAP, err ? err.message : JSON.stringify({ status: r?.status, length: r?.text?.length }));
    }

    {
      const s = await attempt('/429s');
      const d = await attempt('/429d');
      const u = await attempt('/503s');
      const e = s.r && statusError('Example 429: slow down', s.r);
      check('wrapper: a 429 with Retry-After in seconds gives retryAfterMs 120000; as an HTTP date about the same; a 503\'s Retry-After is read too',
        s.r?.status === 429 && s.r.retryAfterMs === 120000 && d.r?.status === 429 && d.r.retryAfterMs > 115000 && d.r.retryAfterMs <= 120000
          && u.r?.status === 503 && u.r.retryAfterMs === 7000,
        JSON.stringify({ seconds: s.r?.retryAfterMs ?? s.err?.message, date: d.r?.retryAfterMs ?? d.err?.message, s503: u.r?.retryAfterMs ?? u.err?.message }));
      check('wrapper: statusError puts the status code and the Retry-After on the error an adapter throws, under the adapter\'s own message',
        e instanceof Error && e.message === 'Example 429: slow down' && e.status === 429 && e.retryAfterMs === 120000,
        JSON.stringify({ message: e?.message, status: e?.status, retryAfterMs: e?.retryAfterMs }));
      const now = Date.parse('2026-01-01T00:00:00Z');
      const parsed = [['120', 120000], ['0', 0], ['Thu, 01 Jan 2026 00:02:00 GMT', 120000], ['Wed, 31 Dec 2025 23:00:00 GMT', 0], ['', null], ['soon', null], [null, null]]
        .map(([v, want]) => [v, want, parseRetryAfter(v, now)]).filter(([, want, got]) => got !== want);
      check('parseRetryAfter: seconds, an HTTP date (a past one gives 0), and nothing usable gives null', parsed.length === 0, JSON.stringify(parsed));
    }

    {
      // The poll's own signal: the deadline aborts a request with its reason, and a
      // signal that has already fired sends no request at all.
      const ac = new AbortController();
      const reason = new Error('the poll took longer than 45 s and was stopped');
      setTimeout(() => ac.abort(reason), 150);
      const { err, ms } = await attempt('/never', { signal: ac.signal });
      check('wrapper: the poll\'s signal ends a request at once with the signal\'s reason, prefixed with the host',
        err?.message === `${host}: the poll took longer than 45 s and was stopped` && ms < 3000, JSON.stringify({ message: err?.message, ms }));
      const hitsBefore = neverHits;
      const gone = new AbortController();
      gone.abort(reason);
      const early = await attempt('/never', { signal: gone.signal });
      await sleep(100);
      check('wrapper: a signal that has already fired ends the call at once and sends no request',
        early.err?.message === `${host}: the poll took longer than 45 s and was stopped` && neverHits === hitsBefore,
        JSON.stringify({ message: early.err?.message, requests: neverHits - hitsBefore }));
    }

    {
      // Node's own error here says only "Invalid URL", so the message is pinned: the check
      // must see the wrapper's words, not merely the absence of the key.
      let bad = null;
      try { await guardedFetch('not a url?api_key=SECRET-CANARY'); } catch (e) { bad = e; }
      check('wrapper: an address that cannot be parsed is refused with its fixed message ("a request address could not be read"), which does not quote it',
        bad?.message === 'a request address could not be read', bad ? bad.message : 'no error');
    }

    {
      // A request that cannot be made at all: a port nothing listens on (taken from the OS
      // and closed again). undici says only "fetch failed"; the wrapper names the host and
      // the system's code, and never the path or the query.
      const { createServer: createNetServer } = await import('node:net');
      const closedPort = await new Promise((res, rej) => {
        const s = createNetServer();
        s.once('error', rej);
        s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)); });
      });
      let e = null;
      try { await guardedFetch(`http://127.0.0.1:${closedPort}/never?api_key=SECRET-CANARY`); } catch (x) { e = x; }
      check('wrapper: a connection refused fails with the host and the system\'s code ("<host>: the request failed (ECONNREFUSED)"), not undici\'s bare "fetch failed", and never the path or the query',
        e?.message === `127.0.0.1:${closedPort}: the request failed (ECONNREFUSED)`, JSON.stringify({ message: e?.message }));
    }

    {
      // A header value the request cannot carry: undici refuses it before connecting, with
      // a message that quotes the whole value. A start.gg key or a StreamElements token
      // pasted with a line break inside would reach lastError that way.
      const token = 'TOKEN-CANARY-4321';
      const { err } = await attempt('/ok', { headers: { Authorization: `Bearer abc\n${token}` } });
      check('wrapper: a header the request cannot carry (a line break inside a key) fails with the host only; the header\'s value, and the key in it, are never quoted',
        err?.message === `${host}: the request failed` && !err.message.includes(token), JSON.stringify({ message: err?.message }));
    }
  } finally {
    setLimit(envBefore);
    for (const r of held) { try { r.destroy(); } catch {} }
    for (const s of sockets) { try { s.destroy(); } catch {} }
    await new Promise((r) => server.close(r));
  }

  // The adapters' wiring, in this process with no network and no base-URL override
  // (decision 9): for these calls only, fetch itself is replaced by a stand-in that
  // records each request and answers it, so each adapter's own request path runs through
  // the wrapper. Each must pass the poll's signal to its request (an abort ends the call
  // at once, with the host and the signal's reason), throw a 429 with its status and
  // Retry-After (statusError), and send its User-Agent.
  {
    const byName = Object.fromEntries(PLATFORMS.map((p) => [p.name, p]));
    const realFetch = globalThis.fetch;
    const limitBefore = process.env.ZIPPER_REQUEST_TIMEOUT_MS;
    let mode = '429';
    const seen = [];
    globalThis.fetch = (url, init = {}) => {
      seen.push({ host: new URL(String(url)).host, headers: { ...(init.headers || {}) } });
      if (mode === '429') return Promise.resolve(new Response('slow down', { status: 429, headers: { 'retry-after': '120' } }));
      // 'hang': no answer until the request's own signal (the wrapper's) aborts.
      return new Promise((_, rej) => {
        const s = init.signal;
        const fail = () => rej(Object.assign(new Error('This operation was aborted'), { name: 'AbortError' }));
        if (!s) return;
        if (s.aborted) fail(); else s.addEventListener('abort', fail, { once: true });
      });
    };
    // An adapter that does not pass the poll's signal leaves its request to end at the
    // wrapper's own limit, shortened here so that shows as a timeout message, quickly.
    process.env.ZIPPER_REQUEST_TIMEOUT_MS = '1500';
    const SG_URL = 'https://www.start.gg/tournament/sol9/event/sf6-singles';
    const cases = [
      // [label, platform, id, config, original URL, host, User-Agent (undefined: not checked)]
      ['start.gg (public proxy)', 'startgg', 'sol9/sf6-singles', { startggApiKey: null }, SG_URL, 'www.start.gg', mod.USER_AGENT],
      ['start.gg (API key)', 'startgg', 'sol9/sf6-singles', { startggApiKey: 'FAKE-STARTGG-KEY' }, SG_URL, 'api.start.gg', mod.USER_AGENT],
      ['Challonge (API key)', 'challonge', 'blko42', { challongeApiKey: 'FAKE-CHALLONGE-KEY' }, 'https://challonge.com/blko42', 'api.challonge.com', undefined],
      ['Challonge (embed page)', 'challonge', 'blko42', {}, 'https://challonge.com/blko42', 'challonge.com', mod.BROWSER_USER_AGENT],
      ['Matcherino', 'matcherino', '424242', {}, 'https://matcherino.com/tournaments/424242', 'api.matcherino.com', mod.USER_AGENT],
    ];
    const wrong = [];
    try {
      for (const [label, name, id, cfg, url, wantHost, wantUa] of cases) {
        mode = '429';
        seen.length = 0;
        let e429 = null;
        try { await byName[name].fetchTicker(id, cfg, url, new AbortController().signal); } catch (e) { e429 = e; }
        if (!(e429?.status === 429 && e429.retryAfterMs === 120000)) {
          wrong.push(`${label}: a 429 threw ${JSON.stringify({ message: e429?.message, status: e429?.status, retryAfterMs: e429?.retryAfterMs })}`);
        }
        if (seen.length !== 1 || seen[0].host !== wantHost) wrong.push(`${label}: requests to ${JSON.stringify(seen.map((s) => s.host))}, wanted one to ${wantHost}`);
        else if (wantUa !== undefined && seen[0].headers['User-Agent'] !== wantUa) wrong.push(`${label}: User-Agent ${JSON.stringify(seen[0].headers['User-Agent'])}, wanted ${JSON.stringify(wantUa)}`);

        mode = 'hang';
        const ac = new AbortController();
        const t0 = Date.now();
        setTimeout(() => ac.abort(new Error('the poll took longer than 45 s and was stopped')), 50);
        let eAbort = null;
        try { await byName[name].fetchTicker(id, cfg, url, ac.signal); } catch (e) { eAbort = e; }
        const ms = Date.now() - t0;
        if (!(eAbort?.message === `${wantHost}: the poll took longer than 45 s and was stopped` && ms < 1000)) {
          wrong.push(`${label}: an abort of the poll's signal gave ${JSON.stringify({ message: eAbort?.message, ms })}`);
        }
      }
    } finally {
      globalThis.fetch = realFetch;
      if (limitBefore === undefined) delete process.env.ZIPPER_REQUEST_TIMEOUT_MS; else process.env.ZIPPER_REQUEST_TIMEOUT_MS = limitBefore;
    }
    check('adapters, in-process with a stand-in for fetch (no network): start.gg and Challonge (both paths each) and Matcherino pass the poll\'s signal to their request (an abort ends it at once, with the host and the reason), throw a 429 with its status and Retry-After, and send their User-Agent',
      wrong.length === 0, wrong.join('; '));
  }
}

// ── Mock server helpers ───────────────────────────────────────────────────────

let mockProc = null;
let mockOut = '';        // the mock's stdout, its last 20000 characters
let mockOutTotal = 0;    // characters of stdout received so far, a mark for mockOutSince
// What the mock has printed since a mark taken from mockOutTotal.
const mockOutSince = (mark) => mockOut.slice(Math.max(0, mockOut.length - (mockOutTotal - mark)));
async function startMock() {
  // stdin is a pipe the mock watches (MOCK_EXIT_WITH_PARENT): if this process is
  // killed outright, the pipe closes and the mock exits by itself. MOCK_ARG_BLEED is
  // cleared so a value in the shell cannot switch bleed mode on for the whole run.
  mockProc = track(spawn(process.execPath, [resolve(__dirname, 'mock-sb-server.mjs')], {
    env: { ...process.env, SB_HTTP_PORT: String(HTTP_PORT), SB_WS_PORT: String(WS_PORT), MOCK_EXIT_WITH_PARENT: '1', MOCK_ARG_BLEED: '' },
    stdio: ['pipe', 'pipe', 'pipe'],
  }));
  mockProc.stdout.on('data', (d) => { const s = String(d); mockOutTotal += s.length; mockOut = (mockOut + s).slice(-20000); });
  mockProc.stderr.on('data', (d) => process.stderr.write(`[mock-err] ${d}`));
  let exited = null;
  mockProc.on('exit', (code) => { exited = code; });
  const up = await waitFor(async () => {
    if (exited !== null) return 'exited';
    try { return (await fetch(`${HTTP}/mock/stats`)).ok ? 'up' : null; } catch { return null; }
  }, 8000, 100);
  if (up !== 'up') throw new Error(`the mock server did not come up (${up === 'exited' ? `it exited with code ${exited}` : 'no answer in 8 s'})\n${mockOut}`);
}
const mockGet = async (path) => (await fetch(`${HTTP}${path}`)).json();
const stats = () => mockGet('/mock/stats');
const enc = encodeURIComponent;

// A raw client of the mock's WebSocket: every frame it receives, in order, and a
// DoAction that resolves with its reply frame (or null).
function sbClient({ origin, subscribe = true } = {}) {
  return new Promise((res) => {
    let settled = false;
    const finish = (c) => { if (!settled) { settled = true; res(c); } };
    const ws = new WebSocket(WS_URL, origin ? { origin } : undefined);
    const c = { ws, frames: [], closed: null, refused: null, ids: 0 };
    ws.on('message', (raw) => {
      const text = raw.toString();
      let m; try { m = JSON.parse(text); } catch { m = undefined; }
      c.frames.push({ text, m });
    });
    ws.on('close', (code) => { c.closed = code; finish(c); });
    ws.on('error', () => {});
    ws.on('unexpected-response', (req, r) => { c.refused = r.statusCode; try { req.destroy(); } catch {} finish(c); });
    ws.on('open', async () => {
      if (subscribe) {
        ws.send(JSON.stringify({ request: 'Subscribe', id: 'sub', events: { general: ['Custom'] } }));
        await waitFor(() => c.frames.some((f) => f.m && f.m.id === 'sub'), 3000);
      }
      finish(c);
    });
    c.doAction = async (name, args) => {
      const id = `v${++c.ids}`;
      const at = c.frames.length;
      c.ws.send(JSON.stringify({ request: 'DoAction', id, action: { name }, args }));
      const f = await waitFor(() => c.frames.slice(at).find((x) => x.m && x.m.id === id && !x.m.event), 3000);
      return f ? { m: f.m, index: c.frames.indexOf(f) } : null;
    };
    // The data of every General.Custom broadcast in frames [from, to).
    c.custom = (from = 0, to = c.frames.length) => c.frames.slice(from, to)
      .filter((f) => f.m && f.m.event && f.m.event.source === 'General' && f.m.event.type === 'Custom')
      .map((f) => f.m.data);
  });
}
const closeClient = (c) => { try { c.ws.close(); } catch {} };

// One DoAction, then a DoAction for an action that does not exist. The mock runs
// requests from one socket in order and answers the second only after the first has
// run, so the broadcasts between the two replies are exactly the first one's.
async function actAndCollect(c, name, args) {
  const reply = await c.doAction(name, args);
  const end = await c.doAction('Verify End Mark', {});
  return { reply, got: reply && end ? c.custom(reply.index + 1, end.index) : null };
}

// A client of the bus for the sidecar sections: the data of every General.Custom
// broadcast it receives, unwrapped. Resolves once its Subscribe is acknowledged.
function busClient() {
  return new Promise((res, rej) => {
    const ws = new WebSocket(WS_URL);
    const events = [];
    ws.on('open', () => ws.send(JSON.stringify({ request: 'Subscribe', id: 'sub', events: { general: ['Custom'] } })));
    ws.on('message', (raw) => {
      let m; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m && m.id === 'sub' && !m.event) { res({ ws, events }); return; }
      if (m?.event?.source === 'General' && m.event.type === 'Custom') {
        let d = m.data;
        if (typeof d === 'string') { try { d = JSON.parse(d); } catch { return; } }
        events.push(d);
      }
    });
    ws.on('error', rej);
  });
}

// ── 3. panel-core in a browser shim ──────────────────────────────────────────

const received = []; // every CustomEvent panel-core dispatches, for this whole run
async function testPanelCore() {
  console.log('\n[3] panel-core (real file, browser shim, mock SB)');

  const winListeners = new Map();
  globalThis.CustomEvent = class { constructor(type, opts) { this.type = type; this.detail = opts?.detail; } };
  globalThis.window = {
    __SB_WS_URL: WS_URL,
    addEventListener: (t, fn) => { (winListeners.get(t) || winListeners.set(t, []).get(t)).push(fn); },
    dispatchEvent: (ev) => { received.push(ev); (winListeners.get(ev.type) || []).forEach((fn) => fn(ev)); },
  };
  globalThis.document = {
    readyState: 'complete',
    addEventListener: () => {},
    body: { classList: { toggle: () => {} } },
  };
  globalThis.location = { search: '' };
  globalThis.WebSocket = WebSocket;

  await import('./zipper-shared/panel-core.js');

  // Sync-on-connect: panel-core Subscribes then DoActions "Ticker Push" (no
  // payload) → mock replays the stored payload (none yet → empty update).
  const sync = await waitFor(() => received.find((e) => e.type === 'ticker:update'), 4000);
  check('panel-core: sync-on-connect replay received', !!sync, JSON.stringify(received.map((e) => e.type)));
  check('panel-core: empty update shape', !!sync && Array.isArray(sync.detail.matches) && sync.detail.matches.length === 0);

  // Live push through the mock's HTTP surface (what the sidecar's DoAction becomes).
  const atPush = received.length;
  const sample = await readFile(FIX('sample-update.json'), 'utf-8');
  await fetch(`${HTTP}/mock/push`, { method: 'POST', body: sample });
  const live = await waitFor(() => received.slice(atPush).find((e) => e.type === 'ticker:update'), 4000);
  check('panel-core: live ticker:update dispatched', live && live.detail.matches.length === 4,
    live ? String(live.detail.matches.length) : 'none');
  check('panel-core: caps travel in payload', live && live.detail.caps.text === '@FlashGalatine');

  // Announcement channel: broadcast → ticker:announce CustomEvent.
  const atAnn = received.length;
  await fetch(`${HTTP}/mock/announce?text=${enc('Flash just subscribed!')}&kind=sub&duration=3`);
  const ann = await waitFor(() => received.slice(atAnn).find((e) => e.type === 'ticker:announce'), 4000);
  check('panel-core: ticker:announce dispatched', ann && ann.detail.kind === 'sub' && ann.detail.duration === 3,
    JSON.stringify(ann?.detail));
}

// ── 4. The mock ───────────────────────────────────────────────────────────────

async function testMock() {
  console.log('\n[4] the mock: HTTP maps, DoAction replies, test switches, what it refuses');
  const overlay = await fetch(`${HTTP}/zipper-overlay/ticker.html`);
  const overlayBody = await overlay.text();
  check('overlay served', overlay.status === 200);
  check('overlay references /zipper-shared/panel-core.js', overlayBody.includes('/zipper-shared/panel-core.js'));
  check('panel-core served', (await fetch(`${HTTP}/zipper-shared/panel-core.js`)).status === 200);
  check('control page served', (await fetch(`${HTTP}/zipper-shared/control.html`)).status === 200);
  check('traversal guarded', (await fetch(`${HTTP}/zipper-shared/..%2Fconfig.example.json`)).status === 404);

  const st0 = await stats();
  check('mock: bleed mode is off at the start (the suite clears MOCK_ARG_BLEED)', st0.argBleed === false, JSON.stringify(st0.argBleed));

  const a = await sbClient();
  const b = await sbClient();

  // The DoAction reply, as measured on Streamer.bot 1.0.7.
  {
    const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
    const { reply, got } = await actAndCollect(a, 'Ticker Command', { command: 'status', value: 'v1' });
    const args = reply?.m?.args || {};
    check('mock: a DoAction reply carries the request\'s own args plus actionId, actionName, queueName, runningActionId and actionQueuedAt',
      reply?.m?.status === 'ok' && args.command === 'status' && args.value === 'v1' && args.actionName === 'Ticker Command'
        && uuid.test(args.actionId || '') && args.queueName === 'Mock Blocking' && uuid.test(args.runningActionId || '')
        && !Number.isNaN(Date.parse(args.actionQueuedAt)),
      JSON.stringify(reply?.m));
    check('mock: the reply is sent when the action is queued, before its broadcast',
      !!got && got.length === 1 && got[0]?.command === 'status' && got[0]?.value === 'v1', JSON.stringify(got));
    const again = await a.doAction('Ticker Command', { command: 'status', value: 'v2' });
    check('mock: each run has its own runningActionId; the actionId is the action\'s own',
      !!again && again.m.args?.actionId === args.actionId && again.m.args?.runningActionId !== args.runningActionId,
      JSON.stringify(again?.m?.args));
    const unknown = await actAndCollect(a, 'No Such Action', { x: '1' });
    check('mock: an action that does not exist gets an error reply ("was not found") and runs nothing',
      unknown.reply?.m?.status === 'error' && /was not found/.test(unknown.reply.m.error || '') && unknown.got?.length === 0,
      JSON.stringify(unknown.reply?.m));
  }

  // The connect-sync Ticker Push broadcasts to every subscriber, as the C# does.
  {
    const atB = b.frames.length;
    const { got } = await actAndCollect(a, 'Ticker Push', { reason: 'overlay-connect' });
    const toB = await waitFor(() => b.custom(atB).find((d) => d && d.type === 'ticker:update'), 3000);
    check('mock: the connect-sync Ticker Push broadcasts to every subscriber, not only the client that asked',
      !!got && got.length === 1 && got[0]?.type === 'ticker:update' && !!toB, JSON.stringify({ toA: got, toB }));
  }

  // The switches. Each test turns its switch off again.
  {
    await mockGet(`/mock/missing?name=${enc('Ticker Push')}&on=1`);
    const off = await actAndCollect(a, 'Ticker Push', { reason: 'overlay-connect' });
    await mockGet(`/mock/missing?name=${enc('Ticker Push')}&on=0`);
    const back = await actAndCollect(a, 'Ticker Push', { reason: 'overlay-connect' });
    check('mock: /mock/missing makes a known action answer "was not found"; off, it runs again',
      off.reply?.m?.status === 'error' && /was not found/.test(off.reply.m.error || '') && off.got?.length === 0
        && back.reply?.m?.status === 'ok' && back.got?.length === 1, JSON.stringify({ off: off.reply?.m, back: back.got }));
  }
  {
    await mockGet(`/mock/silent?name=${enc('Ticker Command')}&on=1`);
    const quiet = await actAndCollect(a, 'Ticker Command', { command: 'status' });
    await mockGet(`/mock/silent?name=${enc('Ticker Command')}&on=0`);
    const loud = await actAndCollect(a, 'Ticker Command', { command: 'status' });
    check('mock: /mock/silent makes an action reply "ok" and do nothing; off, it relays again',
      quiet.reply?.m?.status === 'ok' && quiet.got?.length === 0 && loud.got?.length === 1, JSON.stringify({ quiet: quiet.got, loud: loud.got }));
  }
  {
    await mockGet(`/mock/throw?name=${enc('Ticker Announce')}&on=1`);
    const thrown = await actAndCollect(a, 'Ticker Announce', { text: 'x' });
    await mockGet(`/mock/throw?name=${enc('Ticker Announce')}&on=1&message=${enc('a given message')}`);
    const given = await actAndCollect(a, 'Ticker Announce', { text: 'x' });
    await mockGet(`/mock/throw?name=${enc('Ticker Announce')}&on=0`);
    const fine = await actAndCollect(a, 'Ticker Announce', { text: 'x' });
    check('mock: /mock/throw makes an action send ticker:error, as its catch block does (with a given message when one is given); off, it runs again',
      thrown.got?.length === 1 && thrown.got[0]?.type === 'ticker:error' && typeof thrown.got[0].message === 'string' && thrown.got[0].message.length > 0
        && given.got?.length === 1 && given.got[0]?.message === 'a given message'
        && fine.got?.length === 1 && fine.got[0]?.type === 'ticker:announce',
      JSON.stringify({ thrown: thrown.got, given: given.got, fine: fine.got }));
  }
  {
    await mockGet(`/mock/queue?name=${enc('Ticker Push')}&queue=Default`);
    const push = await a.doAction('Ticker Push', { reason: 'overlay-connect' });
    const command = await a.doAction('Ticker Command', { command: 'status' });
    await mockGet(`/mock/queue?name=${enc('Ticker Push')}&queue=`);
    const cleared = await a.doAction('Ticker Push', { reason: 'overlay-connect' });
    check('mock: /mock/queue sets the queueName of one action\'s replies; an empty queue= clears it',
      push?.m?.args?.queueName === 'Default' && command?.m?.args?.queueName === 'Mock Blocking' && cleared?.m?.args?.queueName === 'Mock Blocking',
      JSON.stringify([push?.m?.args?.queueName, command?.m?.args?.queueName, cleared?.m?.args?.queueName]));
  }
  {
    const bad = await fetch(`${HTTP}/mock/missing?name=${enc('Ticker Pusher')}&on=1`);
    check('mock: a switch given a name that is not an action answers 400', bad.status === 400, String(bad.status));
  }
  // on= takes 1 or 0 only. Each switch is turned on, then given another spelling of
  // "on", which must get a 400 and leave it on; then it is turned off again.
  {
    const wrong = [];
    const tries = [
      ['/mock/bleed?', 'true', async () => (await stats()).argBleed === true],
      [`/mock/missing?name=${enc('Ticker Push')}&`, 'true', async () => (await stats()).missing.includes('Ticker Push')],
      [`/mock/silent?name=${enc('Ticker Push')}&`, 'yes', async () => (await stats()).silent.includes('Ticker Push')],
      [`/mock/throw?name=${enc('Ticker Push')}&`, 'ON', async () => 'Ticker Push' in (await stats()).throwing],
    ];
    for (const [prefix, spelling, isOn] of tries) {
      try {
        await mockGet(`${prefix}on=1`);
        const r = await fetch(`${HTTP}${prefix}on=${spelling}`);
        const stillOn = await isOn();
        if (r.status !== 400 || !stillOn) wrong.push(`${prefix}on=${spelling}: ${r.status}, ${stillOn ? 'still on' : 'turned off'}`);
      } finally {
        await mockGet(`${prefix}on=0`);
      }
    }
    const st = await stats();
    check('mock: a switch given on= other than 1 or 0 (true, yes, ON) answers 400 and changes nothing',
      wrong.length === 0 && st.argBleed === false && st.missing.length === 0 && st.silent.length === 0 && Object.keys(st.throwing).length === 0,
      wrong.join('; ') || JSON.stringify(st));
  }

  // Bleed mode (row 1.2): two DoActions for one action within about 10 ms both run with
  // the later one's arguments. The test turns it off again however it ends.
  try {
    await mockGet('/mock/bleed?on=1');
    const at = a.frames.length;
    a.ws.send(JSON.stringify({ request: 'DoAction', id: 'b1', action: { name: 'Ticker Command' }, args: { command: 'bleedFirst' } }));
    a.ws.send(JSON.stringify({ request: 'DoAction', id: 'b2', action: { name: 'Ticker Command' }, args: { command: 'bleedSecond' } }));
    const two = await waitFor(() => { const g = a.custom(at).filter((d) => d && d.type === 'ticker:command' && /^bleed/.test(d.command)); return g.length >= 2 ? g : null; }, 3000);
    const r1 = a.frames.slice(at).find((f) => f.m && f.m.id === 'b1');
    check('mock: bleed on: two runs of one action within the window both see the later arguments, and replies say queueName "Default"',
      !!two && two.length === 2 && two.every((d) => d.command === 'bleedSecond') && r1?.m?.args?.queueName === 'Default',
      JSON.stringify({ two, queue: r1?.m?.args?.queueName }));
  } finally {
    await mockGet('/mock/bleed?on=0');
  }
  {
    const at = a.frames.length;
    a.ws.send(JSON.stringify({ request: 'DoAction', id: 'n1', action: { name: 'Ticker Command' }, args: { command: 'calmFirst' } }));
    a.ws.send(JSON.stringify({ request: 'DoAction', id: 'n2', action: { name: 'Ticker Command' }, args: { command: 'calmSecond' } }));
    const two = await waitFor(() => { const g = a.custom(at).filter((d) => d && d.type === 'ticker:command' && /^calm/.test(d.command)); return g.length >= 2 ? g : null; }, 3000);
    check('mock: bleed off again: each run keeps its own arguments (a blocking queue)',
      !!two && two[0].command === 'calmFirst' && two[1].command === 'calmSecond' && (await stats()).argBleed === false, JSON.stringify(two));
  }
  // A run that throws is logged and the mock stays up, with bleed mode off and on (on,
  // the runs happen later, in a timer). The argument here is an object whose text cannot
  // be taken; the request after it must still run.
  {
    const results = [];
    try {
      for (const on of ['0', '1']) {
        await mockGet(`/mock/bleed?on=${on}`);
        const outAt = mockOutTotal;
        const at = a.frames.length;
        const text = `after the bad one (bleed ${on})`;
        a.ws.send(JSON.stringify({ request: 'DoAction', id: `p${on}`, action: { name: 'Ticker Command' }, args: { command: { toString: 1 } } }));
        a.ws.send(JSON.stringify({ request: 'DoAction', id: `q${on}`, action: { name: 'Ticker Announce' }, args: { text } }));
        const next = await waitFor(() => mockProc.exitCode !== null || a.custom(at).some((d) => d && d.type === 'ticker:announce' && d.text === text), 3000);
        const logged = await waitFor(() => mockProc.exitCode === null && mockOutSince(outAt).includes('Ticker Command run failed'), 2000);
        results.push({ bleed: on, nextRan: !!next && mockProc.exitCode === null, logged: !!logged });
      }
    } finally {
      await mockGet('/mock/bleed?on=0').catch(() => {});
    }
    check('mock: a run that throws is logged, and the mock stays up and runs the next request, with bleed mode off and on',
      results.length === 2 && results.every((r) => r.nextRan && r.logged) && mockProc.exitCode === null,
      JSON.stringify({ results, mockExit: mockProc.exitCode }));
  }

  // What it refuses (row 1.9), and that it stays up afterwards.
  {
    const badPath = await fetch(`${HTTP}/zipper-shared/%E0%A4%A`);
    const after = await fetch(`${HTTP}/mock/stats`);
    check('mock: a path with a bad percent-escape gets a 400 and the mock stays up',
      badPath.status === 400 && after.status === 200 && mockProc.exitCode === null, `${badPath.status}, then ${after.status}`);
    const big = await fetch(`${HTTP}/mock/push`, { method: 'POST', body: 'x'.repeat(1024 * 1024 + 1) }).then((r) => r.status, (e) => `error ${e.message}`);
    const notJson = await fetch(`${HTTP}/mock/push`, { method: 'POST', body: 'not json' }).then((r) => r.status, () => 0);
    check('mock: a /mock/push body over 1 MiB gets a 413, one that is not JSON a 400, and the mock stays up',
      big === 413 && notJson === 400 && (await fetch(`${HTTP}/mock/stats`)).status === 200 && mockProc.exitCode === null, `${big}, ${notJson}`);
  }
  {
    const c = await sbClient();
    for (const frame of ['null', '42', '"text"', '[]', '{']) c.ws.send(frame);
    const r = await c.doAction('Ticker Command', { command: 'status' });
    check('mock: frames that are not a request object (null, a number, a string, an array, broken JSON) are ignored; the socket stays open',
      r?.m?.status === 'ok' && c.closed === null && mockProc.exitCode === null, JSON.stringify(r?.m));
    c.ws.send('x'.repeat(1024 * 1024 + 1));
    await waitFor(() => c.closed !== null, 3000);
    const d = await sbClient();
    const r2 = await d.doAction('Ticker Command', { command: 'status' });
    check('mock: a WebSocket message over 1 MiB closes that connection (1009); the mock stays up',
      c.closed === 1009 && r2?.m?.status === 'ok' && mockProc.exitCode === null, String(c.closed));
    closeClient(d);
  }
  {
    const tries = [
      ['https://evil.example', false], ['http://127.0.0.1.evil.example', false], ['http://localhost.evil.example:7476', false],
      ['http://127.0.0.1:7476', true], ['http://localhost:9999', true], ['http://[::1]:1234', true], ['null', true], ['file://', true], [null, true],
    ];
    const wrong = [];
    for (const [origin, allowed] of tries) {
      const c = await sbClient({ origin: origin ?? undefined, subscribe: false });
      const open = c.refused === null && c.closed === null;
      if (open !== allowed) wrong.push(`${origin ?? '(no Origin)'}: ${open ? 'let in' : `refused ${c.refused ?? c.closed}`}`);
      closeClient(c);
    }
    check('mock: a WebSocket from another site\'s page is refused (403); no Origin, null, file:// and this machine on any port are let in',
      wrong.length === 0, wrong.join('; '));
  }
  try {
    await mkdir(TYPES_DIR, { recursive: true });
    for (const f of ['a.svg', 'a.woff', 'a.woff2', 'a.png']) await writeFile(join(TYPES_DIR, f), 'x');
    const type = async (f) => (await fetch(`${HTTP}/zipper-overlay/.verify-tmp/${f}`)).headers.get('content-type');
    const got = { svg: await type('a.svg'), woff: await type('a.woff'), woff2: await type('a.woff2'), png: await type('a.png') };
    check('mock: .svg, .woff and .woff2 are served as application/octet-stream, as Streamer.bot 1.0.7 does (.png keeps image/png)',
      got.svg === 'application/octet-stream' && got.woff === 'application/octet-stream' && got.woff2 === 'application/octet-stream' && got.png === 'image/png',
      JSON.stringify(got));
  } finally {
    await rm(TYPES_DIR, { recursive: true, force: true });
  }

  // /mock/kick closes every open socket. panel-core's (section 3) is one of them: wait
  // until it has reconnected and synced again, so no late sync lands in later sections.
  {
    const atRecv = received.length;
    const kicked = await mockGet('/mock/kick');
    await waitFor(() => a.closed !== null && b.closed !== null, 3000);
    const resynced = await waitFor(() => received.slice(atRecv).find((e) => e.type === 'ticker:update'), 8000, 50);
    const fresh = await sbClient();
    const r = await fresh.doAction('Ticker Command', { command: 'status' });
    check('mock: /mock/kick closes every open connection; panel-core reconnects and syncs again; a new connection is let in',
      kicked?.kicked >= 3 && a.closed !== null && b.closed !== null && !!resynced && r?.m?.status === 'ok',
      JSON.stringify({ kicked, a: a.closed, b: b.closed, resynced: !!resynced }));
    closeClient(fresh);
  }
  closeClient(a);
  closeClient(b);
}

// ── 5. The bus actions as the mock runs them: the case table ─────────────────

async function testCaseTable() {
  console.log('\n[5] the mock\'s Ticker Push, Ticker Command and Ticker Announce against tools/cs/ticker-cases.json');
  const table = loadTickerCases();
  const c = await sbClient();
  const asData = (text) => { try { return JSON.parse(text); } catch { return text; } };
  const same = (x, y) => JSON.stringify(x) === JSON.stringify(y);
  for (const action of ['Ticker Push', 'Ticker Command', 'Ticker Announce']) {
    const rows = table.rows.filter((r) => r.action === action);
    const wrong = [];
    for (const row of rows) {
      if (action === 'Ticker Push') {
        if (row.start === null) await mockGet('/mock/reset');
        else await fetch(`${HTTP}/mock/push`, { method: 'POST', body: row.start });
      }
      if (row.throw) await mockGet(`/mock/throw?name=${enc(action)}&on=1&message=${enc(table.throwMessage)}`);
      let res;
      try { res = await actAndCollect(c, action, row.args); }
      finally { if (row.throw) await mockGet(`/mock/throw?name=${enc(action)}&on=0`); }
      const want = row.broadcast === null ? [] : [asData(row.broadcast)];
      let ok = res.reply?.m?.status === 'ok' && !!res.got && same(res.got, want);
      if (action === 'Ticker Push') ok = ok && (await stats()).storedPayload === row.stored;
      if (!ok) wrong.push(`"${row.name}": wanted ${JSON.stringify(want)}${action === 'Ticker Push' ? ` stored ${JSON.stringify(row.stored)}` : ''}; got ${JSON.stringify(res.got)}`);
    }
    check(`${action}: the mock answers every one of the table's ${rows.length} rows as the table says (verify:cs runs the same rows through the C#)`,
      rows.length > 0 && wrong.length === 0, wrong.slice(0, 6).join(' || '));
  }
  // Leave the store as section 6 found it before this table existed: the sample.
  await fetch(`${HTTP}/mock/push`, { method: 'POST', body: await readFile(FIX('sample-update.json'), 'utf-8') });
  closeClient(c);
}

// ── Sidecar helpers ───────────────────────────────────────────────────────────

const sidecars = [];
// Starts a test sidecar with ZIPPER_ALLOW_STUB=1, or with no ZIPPER_ALLOW_STUB at all
// when allowStub is false (removed in any casing, since Windows matches environment
// names without regard to case, so a value in the developer's shell cannot turn it on).
// The poll loop's three limit overrides are removed the same way, so only the values a
// test passes apply.
// If it exits before the suite stops it, the run fails at once with a line, and its
// stdout and stderr are printed.
// nodeArgs go before the script (section 6h preloads tools/clock-step-preload.cjs).
const SHELL_ENV_DROPPED = ['ZIPPER_ALLOW_STUB', 'ZIPPER_REQUEST_TIMEOUT_MS', 'ZIPPER_POLL_DEADLINE_MS', 'ZIPPER_MIN_POLL_GAP_MS', 'ZIPPER_TEST_CLOCK_STEP_FILE'];
function startSidecar(label, env, { allowStub = true, nodeArgs = [] } = {}) {
  const sc = { label, out: '', err: '', stopping: false, exited: false, code: null };
  const base = { ...process.env };
  for (const k of Object.keys(base)) if (SHELL_ENV_DROPPED.includes(k.toUpperCase())) delete base[k];
  if (allowStub) base.ZIPPER_ALLOW_STUB = '1';
  sc.proc = track(spawn(process.execPath, [...nodeArgs, resolve(__dirname, 'zipper-sidecar.mjs')], {
    env: { ...base, SB_WS_URL: WS_URL, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  }));
  sc.proc.stdout.on('data', (d) => { sc.out += d; });
  sc.proc.stderr.on('data', (d) => { sc.err += d; process.stderr.write(`[${label}-err] ${d}`); });
  sc.proc.on('exit', (code) => {
    sc.exited = true;
    sc.code = code;
    if (!sc.stopping) {
      check(`${label} kept running until the suite stopped it`, false, `it exited by itself with code ${code}`);
      console.log(`----- ${label}: stdout -----\n${sc.out}----- ${label}: stderr -----\n${sc.err}----- end of ${label} -----`);
    }
  });
  sc.gone = () => sc.exited;
  sc.stop = async () => {
    if (sc.exited) return;
    sc.stopping = true;
    try { sc.proc.kill(); } catch {}
    await waitFor(() => sc.exited, 5000);
  };
  sidecars.push(sc);
  return sc;
}

const isStatus = (d) => d && d.type === 'ticker:command' && d.command === '__status';
const parseStatus = (d) => { try { return JSON.parse(d.value); } catch { return {}; } };
const isUpdate = (d) => d && d.type === 'ticker:update';

// A scan of only the events that arrived after a recorded mark, so an earlier event
// that also matches cannot answer for the one under test. Gives up early when the
// sidecar under test has gone.
function sinceOn(client, sc) {
  return async (at, pred, timeoutMs = 4000) => {
    const end = Date.now() + timeoutMs;
    for (;;) {
      const hit = client.events.slice(at).filter(pred).pop();
      if (hit) return hit;
      if (Date.now() >= end || (sc && sc.gone())) return null;
      await sleep(25);
    }
  };
}
const readJsonFile = async (path) => { try { return JSON.parse(await readFile(path, 'utf-8')); } catch { return null; } };
// A bounded wait for a saved config file to show what a check expects. On timeout it
// returns the last value read (or null), so the check fails with its own line.
async function savedConfig(path, pred, timeoutMs = 4000) {
  let last = null;
  const hit = await waitFor(async () => { last = await readJsonFile(path); return last && pred(last) ? last : null; }, timeoutMs, 50);
  return hit || last;
}

// ── 6. Sidecar E2E ────────────────────────────────────────────────────────────

async function testSidecar() {
  console.log('\n[6] sidecar (real process, stub platform, mock SB)');

  const cfgPath = join(TMP_DIR, 'config-sidecar.json');
  await writeFile(cfgPath, JSON.stringify({
    tournamentUrl: null, pollIntervalMs: 15000, maxItems: 20,
    caps: { text: '', logo: '' }, autoStart: false,
  }), 'utf-8');

  const client = await busClient();
  // The minimum gap between polls is shortened for this sidecar (its override), so the
  // checks below that send a command and wait a few seconds for the poll it starts keep
  // their timing. Section 6h checks the gap itself.
  const sc = startSidecar('test sidecar [6]', { ZIPPER_CONFIG: cfgPath, ZIPPER_GUARD_PORT: String(GUARD_PORT), ZIPPER_MIN_POLL_GAP_MS: '200' });
  const since = sinceOn(client, sc);
  const mark = () => client.events.length;
  const cmd = (command, value) => client.ws.send(JSON.stringify({
    request: 'DoAction', id: 'c', action: { name: 'Ticker Command' },
    args: { command, value: value ?? '' },
  }));
  const saved = (pred) => savedConfig(cfgPath, pred);

  // Startup status announces the connection.
  const s0e = await since(0, isStatus);
  const s0 = s0e && parseStatus(s0e);
  check('sidecar: startup status broadcast', !!s0 && s0.polling === false && s0.url === null, JSON.stringify(s0));

  // setUrl → status echo carries the stub platform.
  const atUrl = mark();
  cmd('setUrl', 'stub:platforms/fixtures/stub-result.json');
  const s1e = await since(atUrl, (d) => isStatus(d) && parseStatus(d).url?.startsWith('stub:'));
  const s1 = s1e && parseStatus(s1e);
  check('sidecar: setUrl round-trip', s1?.platform === 'stub', JSON.stringify(s1));

  // start → poll → ticker:update pushed through the Ticker Push action.
  const atStart = mark();
  cmd('start');
  const upd = await since(atStart, isUpdate);
  check('sidecar: ticker:update pushed', upd?.matches?.length === 3, String(upd?.matches?.length));
  check('sidecar: sorted completed → live → upcoming',
    upd?.matches?.length === 3 && upd.matches[0].state === 'completed' && upd.matches[1].state === 'in_progress' && upd.matches[2].state === 'upcoming',
    upd?.matches?.map((m) => m.state).join(','));
  check('sidecar: tournament block', upd?.tournament?.name === 'Stub Invitational' && upd.tournament.platform === 'stub');

  // Second poll with identical data → diff suppression (no new push). The first poll's
  // closing status is read first, so the pollNow's own poll can be told apart from it;
  // the pushes are counted only once that poll has finished.
  const firstPolled = await since(atStart, (d) => isStatus(d) && !!parseStatus(d).lastPollAt);
  const firstPollAt = firstPolled && parseStatus(firstPolled).lastPollAt;
  const statsMid = await stats();
  const atPoll = mark();
  cmd('pollNow');
  const polled = await since(atPoll, (d) => isStatus(d) && !!parseStatus(d).lastPollAt && parseStatus(d).lastPollAt !== firstPollAt);
  const statsAfter = await stats();
  check('sidecar: identical poll suppressed', !!firstPollAt && !!polled && statsAfter.pushCount === statsMid.pushCount,
    polled ? `pushCount ${statsMid.pushCount} → ${statsAfter.pushCount}` : 'the pollNow poll was not seen to finish');

  // Caps change → forced re-push with the new caps text.
  const atCaps = mark();
  cmd('setCapsText', '@FlashGalatine');
  const upd2 = await since(atCaps, (d) => isUpdate(d) && d.caps?.text === '@FlashGalatine');
  check('sidecar: caps change forces re-push', !!upd2, 'no update with the new caps text after setCapsText');

  // Chat "!ticker <cmd> …" exactly as a real SB Command trigger fires it: SB's
  // OWN command arg ("!ticker") + commandId + rawInput — the action must parse
  // rawInput instead of relaying "!ticker" verbatim.
  const atChat = mark();
  client.ws.send(JSON.stringify({
    request: 'DoAction', id: 'c', action: { name: 'Ticker Command' },
    args: { command: '!ticker', commandId: 'trig-1', rawInput: 'setCapsText @ChatSet' },
  }));
  const updChat = await since(atChat, (d) => isUpdate(d) && d.caps?.text === '@ChatSet');
  check('sidecar: chat !ticker maps through the trigger shape', !!updChat, 'no update with the caps text sent from chat');

  // Batched setCaps (one command, JSON value — the control page's burst-safe
  // path). Sets both text and logo at once; '@'-leading text must survive.
  const atSetCaps = mark();
  cmd('setCaps', JSON.stringify({ text: '@NewbieFightClub', logo: '/zipper-overlay/x.png' }));
  const updCaps = await since(atSetCaps, (d) => isUpdate(d) && d.caps?.text === '@NewbieFightClub');
  check('sidecar: batched setCaps sets text + logo',
    updCaps?.caps?.text === '@NewbieFightClub' && updCaps.caps.logo === '/zipper-overlay/x.png', JSON.stringify(updCaps?.caps));

  // Batched setOptions (one command, JSON value) — interval + maxItems + topN.
  const atOpts = mark();
  cmd('setOptions', JSON.stringify({ intervalMs: 20000, maxItems: 5, topN: 0 }));
  const sOptsE = await since(atOpts, (d) => isStatus(d) && parseStatus(d).intervalMs === 20000);
  const sOpts = sOptsE && parseStatus(sOptsE);
  check('sidecar: batched setOptions applies all three', !!sOpts && sOpts.intervalMs === 20000 && sOpts.maxItems === 5 && sOpts.topN === 0,
    JSON.stringify({ intervalMs: sOpts?.intervalMs, maxItems: sOpts?.maxItems, topN: sOpts?.topN }));

  // topN filter: only matches involving the top-2 placed (Flash, Rival) remain.
  const atTop = mark();
  cmd('setTopN', '2');
  const upd3 = await since(atTop, (d) => isUpdate(d) && d.matches.length === 2);
  check('sidecar: topN filters to top-placed participants',
    !!upd3 && upd3.matches.every((m) => ['Flash', 'Rival'].includes(m.p1.name) || ['Flash', 'Rival'].includes(m.p2.name)),
    JSON.stringify(upd3?.matches?.map((m) => `${m.p1.name} vs ${m.p2.name}`)));
  check('sidecar: topN trims standings crawl', upd3?.standings?.length === 2, String(upd3?.standings?.length));
  const atReset = mark();
  cmd('setTopN', '0');
  const updReset = await since(atReset, (d) => isUpdate(d) && d.matches.length === 3);
  check('sidecar: topN 0 restores every match', !!updReset, 'no re-push after setTopN 0');

  // Persistent message mode: takes the strip over, results ride along underneath.
  const atMsg = mark();
  cmd('setMessage', JSON.stringify({ text: 'Bracket starts at 8pm ET\n\n!discord for the lobby ', enabled: true }));
  const updMsg = await since(atMsg, (d) => isUpdate(d) && !!d.message);
  check('sidecar: setMessage splits + trims lines, drops blanks',
    JSON.stringify(updMsg?.message?.lines) === JSON.stringify(['Bracket starts at 8pm ET', '!discord for the lobby']),
    JSON.stringify(updMsg?.message));
  check('sidecar: results still ride under the message', updMsg?.matches?.length === 3, String(updMsg?.matches?.length));
  const sMsgE = await since(atMsg, (d) => isStatus(d) && parseStatus(d).message?.enabled);
  const sMsg = sMsgE && parseStatus(sMsgE);
  check('sidecar: message state in status', !!sMsg && sMsg.message.text.startsWith('Bracket starts'), JSON.stringify(sMsg?.message));

  // Enabled + blank text === off (no message key → results show).
  const atBlank = mark();
  cmd('setMessageText', '   ');
  const updBlank = await since(atBlank, (d) => isUpdate(d) && !d.message);
  check('sidecar: enabled but blank text falls back to results',
    updBlank?.matches?.length === 3, JSON.stringify(updBlank?.message ?? updBlank?.matches?.length));

  // Toggle off → message key gone even with the text still set.
  const atBack = mark();
  cmd('setMessageText', 'Back in 5');
  await since(atBack, (d) => isUpdate(d) && d.message?.lines?.[0] === 'Back in 5');
  const atOff = mark();
  cmd('setMessageEnabled', 'off'); // chat spelling, not a JSON boolean
  const updOff = await since(atOff, (d) => isUpdate(d) && !d.message);
  check('sidecar: setMessageEnabled off restores results',
    updOff?.matches?.length === 3, JSON.stringify(updOff?.message ?? updOff?.matches?.length));

  // Persisted for a restart (text survives the toggle).
  const persistedMsg = await saved((c) => c.message?.text === 'Back in 5' && c.message?.enabled === false);
  check('sidecar: message persisted',
    persistedMsg?.message?.text === 'Back in 5' && persistedMsg?.message?.enabled === false,
    JSON.stringify(persistedMsg?.message));

  // stop → status shows polling off; config persisted autoStart=false.
  const atStopCmd = mark();
  cmd('stop');
  const s2 = await since(atStopCmd, (d) => isStatus(d) && parseStatus(d).polling === false);
  check('sidecar: stop round-trip', !!s2, 'no status with polling off after stop');

  // The headline case: message mode with polling off. Nothing polls, so the
  // command itself has to put it on the strip.
  const atStopped = mark();
  cmd('setMessage', JSON.stringify({ text: 'Back in 5', enabled: true }));
  const updStopped = await since(atStopped, isUpdate);
  check('sidecar: message pushes immediately while polling is off',
    updStopped?.message?.lines?.[0] === 'Back in 5', JSON.stringify(updStopped?.message));

  // Same for end caps while stopped — they'd otherwise wait for a poll that
  // message-only setups never run.
  const atCapsStopped = mark();
  cmd('setCapsText', '@Stopped');
  const updCapsStopped = await since(atCapsStopped, isUpdate);
  check('sidecar: caps push immediately while polling is off',
    updCapsStopped?.caps?.text === '@Stopped', JSON.stringify(updCapsStopped?.caps));

  cmd('setMessageEnabled', 'off');
  const persisted = await saved((c) => c.message?.enabled === false && c.caps?.text === '@Stopped');
  check('sidecar: config persisted (url + autoStart off)',
    persisted?.tournamentUrl?.startsWith('stub:') && persisted?.autoStart === false, JSON.stringify(persisted));

  // ── Auto-scheduling ─────────────────────────────────────────────────────────
  // The fixtures live in .verify-tmp/ inside the repo and are named relative to it.
  // A poll that returns `completed` pushes the final payload, then stops
  // polling by itself.
  await writeFile(join(TMP_DIR, 'done.json'), JSON.stringify({
    tournamentName: 'Done Cup', state: 'completed',
    matches: [{ id: 'd1', round: 'Grand Final', state: 'completed',
      p1: { name: 'Flash', score: 3 }, p2: { name: 'Rival', score: 1 }, winner: 1, order: 1 }],
    standings: [{ placement: 1, name: 'Flash' }, { placement: 2, name: 'Rival' }],
  }), 'utf-8');
  cmd('setUrl', `stub:${TMP_REL}/done.json`);
  const atDone = mark();
  cmd('start');
  const evDone = await since(atDone, (d) => isStatus(d) && parseStatus(d).auto?.stopped === 'completed', 5000);
  const sDone = evDone && parseStatus(evDone);
  check('sidecar: completed event auto-stops polling',
    sDone && sDone.polling === false && sDone.auto.eventState === 'completed',
    JSON.stringify(sDone?.auto ?? 'no auto-stop status seen'));
  const updDone = await since(atDone, (d) => isUpdate(d) && d.tournament?.state === 'completed');
  check('sidecar: final payload still pushed before the auto-stop', !!updDone, 'no completed ticker:update seen');

  // A poll that returns `upcoming` sleeps until the platform's start time.
  const upStartAt = new Date(Date.now() + 90_000).toISOString();
  await writeFile(join(TMP_DIR, 'upcoming.json'), JSON.stringify({
    tournamentName: 'Tomorrow Cup', state: 'upcoming', startAt: upStartAt, matches: [], standings: [],
  }), 'utf-8');
  cmd('setUrl', `stub:${TMP_REL}/upcoming.json`);
  const atUp = mark();
  cmd('start');
  const evWait = await since(atUp, (d) => isStatus(d) && !!parseStatus(d).auto?.waitingUntil, 5000);
  const sWait = evWait && parseStatus(evWait);
  check('sidecar: upcoming event waits for the start time',
    sWait && sWait.polling === true && sWait.auto.eventState === 'upcoming' && sWait.auto.startAt === upStartAt,
    JSON.stringify(sWait?.auto ?? 'no waiting status seen'));
  check('sidecar: waitingUntil lands on the start time',
    sWait && Math.abs(Date.parse(sWait.auto.waitingUntil) - Date.parse(upStartAt)) < 5000,
    `waitingUntil ${sWait?.auto?.waitingUntil} vs startAt ${upStartAt}`);

  // …and resumes by itself once the start time arrives and the event is live.
  const soonFix = join(TMP_DIR, 'soon.json');
  await writeFile(soonFix, JSON.stringify({
    tournamentName: 'Soon Cup', state: 'upcoming',
    startAt: new Date(Date.now() + 1500).toISOString(), matches: [], standings: [],
  }), 'utf-8');
  cmd('setUrl', `stub:${TMP_REL}/soon.json`);
  const atSoon = mark();
  cmd('start');
  await since(atSoon, (d) => isStatus(d) && !!parseStatus(d).auto?.waitingUntil, 5000);
  // The event "goes live" while the sidecar sleeps; the wake-up poll at the
  // start time must pick it up and resume the normal cadence.
  await writeFile(soonFix, JSON.stringify({
    tournamentName: 'Soon Cup', state: 'in_progress',
    matches: [{ id: 'l1', round: 'Round 1', state: 'in_progress',
      p1: { name: 'Flash', score: 1 }, p2: { name: 'Rival', score: 0 }, winner: 0, order: 1 }],
    standings: [],
  }), 'utf-8');
  const evLive = await since(atSoon, (d) => isStatus(d) && parseStatus(d).auto?.eventState === 'in_progress', 8000);
  const sLive2 = evLive && parseStatus(evLive);
  check('sidecar: polling resumes when the start time arrives',
    sLive2 && sLive2.polling === true && !sLive2.auto.waitingUntil,
    JSON.stringify(sLive2?.auto ?? 'no resumed status seen'));

  // setAutoSchedule off → a completed event keeps polling (manual mode).
  const atAutoOff = mark();
  cmd('setAutoSchedule', 'off');
  await since(atAutoOff, (d) => isStatus(d) && parseStatus(d).auto?.enabled === false, 4000);
  // Read back before any other command: setUrl, start and stop below each save the whole
  // config too, so a read after them would pass even if setAutoSchedule saved nothing.
  // Nothing else saves here: the event is live, so a poll saves nothing, and the next
  // one is about 20 s away.
  const persistedAuto = await saved((c) => c.autoSchedule === false);
  check('sidecar: autoSchedule persisted (setAutoSchedule saves it itself)', persistedAuto?.autoSchedule === false, JSON.stringify(persistedAuto?.autoSchedule));
  cmd('setUrl', `stub:${TMP_REL}/done.json`);
  const atManual = mark();
  cmd('start');
  const evManual = await since(atManual, (d) => {
    if (!isStatus(d)) return false;
    const s = parseStatus(d);
    return s.auto?.eventState === 'completed' && !!s.lastPollAt;
  }, 5000);
  const sManual = evManual && parseStatus(evManual);
  check('sidecar: setAutoSchedule off keeps polling a completed event',
    sManual && sManual.polling === true && sManual.auto.stopped === null,
    JSON.stringify(sManual?.auto ?? 'no manual-mode status seen'));
  const atStop = mark();
  cmd('stop');
  await since(atStop, (d) => isStatus(d) && parseStatus(d).polling === false, 4000);

  await testStubHold(client, sc, since, mark, cmd);
  await testEveryCommand(client, sc, since, mark, cmd);
  await testCommandRanges(since, mark, cmd);
  await testEveryCommand(client, sc, since, mark, cmd, { lower: true });
  await testAccepts(client, sc, since, mark, cmd, cfgPath);

  // This sidecar is stopped here, so only the milestone sidecar of section 7 can
  // answer there.
  await sc.stop();
  await testConfigRanges(client);
  await testStubAtTheRead(client);
  await testPollLoop(client);

  client.ws.close();
  await rm(cfgPath, { force: true });
}

// ── 6a. A slow source: a stub poll held open while its hold file exists ──────

async function testStubHold(client, sc, since, mark, cmd) {
  console.log('\n[6a] a stub poll held while its hold file exists (the test releases it)');

  // The hold, run on its own in-process: the block is taken from zipper-sidecar.mjs as
  // it is, and given stand-ins for the file check and the timers, so nothing sleeps.
  const src = await readFile(resolve(__dirname, 'zipper-sidecar.mjs'), 'utf-8');
  const block = (/\/\/ ── stub hold: BEGIN[^\n]*\n([\s\S]*?)\/\/ ── stub hold: END/.exec(src) || [])[1];
  check('stub hold: zipper-sidecar.mjs has the stub hold block', !!block);
  if (block) {
    const env = (exists) => {
      const timers = new Map();
      const delays = new Map();
      let seq = 0;
      const e = { calls: 0, logs: [] };
      e.existsSync = (p) => { e.calls++; return exists(p); };
      e.setTimeout = (fn, ms) => { const id = ++seq; timers.set(id, fn); delays.set(id, ms); return id; };
      e.clearTimeout = (id) => { timers.delete(id); delays.delete(id); };
      e.fire = () => { const first = timers.entries().next().value; if (first) { timers.delete(first[0]); delays.delete(first[0]); first[1](); } };
      // Fires the first pending timer set for `ms`; false when there is none.
      e.fireAfter = (ms) => {
        for (const [id, fn] of timers) if (delays.get(id) === ms) { timers.delete(id); delays.delete(id); fn(); return true; }
        return false;
      };
      e.pending = () => timers.size;
      e.delays = () => [...timers.keys()].map((id) => delays.get(id));
      return e;
    };
    // The poll's deadline as the block sees it. Not the 50 ms of a file check, so the two
    // timers can be told apart by their delays.
    const HOLD_DEADLINE_MS = 4500;
    const make = (allowed, e) => new Function('resolve', 'sep', 'existsSync', '__dirname', 'STUB_ALLOWED', 'log', 'setTimeout', 'clearTimeout', 'pollDeadlineMs',
      `${block}\nreturn stubHold;`)(resolve, sep, e.existsSync, __dirname, allowed, (m) => e.logs.push(m), e.setTimeout, e.clearTimeout, HOLD_DEADLINE_MS);
    const state = (p) => { const s = { done: false, err: null }; p.then(() => { s.done = true; }, (err) => { s.err = err; }); return s; };
    const turn = () => new Promise((r) => setImmediate(r));
    const held = { stubHoldFile: `${TMP_REL}/hold-x` };

    {
      const e = env(() => true);
      const s = state(make(false, e)(held));
      await turn();
      check('stub hold: without ZIPPER_ALLOW_STUB=1 it does nothing (no file check, no wait)', s.done && e.calls === 0 && e.pending() === 0, JSON.stringify({ ...s, calls: e.calls }));
    }
    {
      let there = true;
      const e = env(() => there);
      const s = state(make(true, e)(held));
      await turn();
      const waiting = !s.done && !s.err && e.pending() === 1 && e.logs.length === 1;
      e.fire(); await turn();
      const stillWaiting = !s.done && e.pending() === 1 && e.calls === 2;
      there = false;
      e.fire(); await turn();
      check('stub hold: with the variable it waits while the hold file exists, and goes on once it is gone (no fixed sleep)',
        waiting && stillWaiting && s.done && e.pending() === 0, JSON.stringify({ waiting, stillWaiting, ...s, pending: e.pending(), calls: e.calls }));
    }
    {
      const e = env(() => true);
      const ac = new AbortController();
      const s = state(make(true, e)(held, ac.signal));
      await turn();
      const waiting = !s.done && e.pending() === 1;
      ac.abort(new Error('deadline'));
      await turn();
      check('stub hold: an abort signal ends the wait at once with the signal\'s reason, and clears its timer',
        waiting && !s.done && s.err?.message === 'deadline' && e.pending() === 0, JSON.stringify({ waiting, done: s.done, err: s.err?.message, pending: e.pending() }));
      const e2 = env(() => true);
      const ac2 = new AbortController();
      ac2.abort(new Error('already'));
      const s2 = state(make(true, e2)(held, ac2.signal));
      await turn();
      check('stub hold: a signal aborted before the call ends it at once, with no file check',
        s2.err?.message === 'already' && e2.calls === 0 && e2.pending() === 0, JSON.stringify({ err: s2.err?.message, calls: e2.calls }));
    }
    {
      const e = env(() => true);
      const none = state(make(true, e)({ tournamentName: 'x' }));
      const outside = state(make(true, e)({ stubHoldFile: `..${sep}elsewhere-hold` }));
      await turn();
      check('stub hold: a fixture with no hold file, or one whose hold file is not inside the repo folder, is not held (no file check)',
        none.done && outside.done && e.calls === 0, JSON.stringify({ none, outside, calls: e.calls }));
    }

    // "stubHoldIgnoresAbort": true. The hold goes on when the poll is aborted, so a poll's
    // result can come back after setUrl has moved on (section 6h); it is still bounded.
    const ignoring = { ...held, stubHoldIgnoresAbort: true };
    const LOOK_MS = 50; // STUB_HOLD_CHECK_MS, the file check's own timer
    {
      const e = env(() => true);
      const s = state(make(false, e)(ignoring, new AbortController().signal));
      await turn();
      check('stubHoldIgnoresAbort: without ZIPPER_ALLOW_STUB=1 it does nothing either (no file check, no wait, no timer)',
        s.done && e.calls === 0 && e.pending() === 0, JSON.stringify({ ...s, calls: e.calls, pending: e.pending() }));
    }
    {
      let there = true;
      const e = env(() => there);
      const ac = new AbortController();
      const s = state(make(true, e)(ignoring, ac.signal));
      await turn();
      ac.abort(new Error('the tournament URL changed'));
      await turn();
      const afterAbort = !s.done && !s.err && e.delays().includes(LOOK_MS);
      e.fireAfter(LOOK_MS); await turn();
      const stillWaiting = !s.done && !s.err && e.delays().includes(LOOK_MS);
      there = false;
      e.fireAfter(LOOK_MS); await turn();
      check('stubHoldIgnoresAbort: with the variable an abort does not end the wait; the hold goes on while its file exists, and once the file is gone the poll goes on (its result comes back late), with no timer left',
        afterAbort && stillWaiting && s.done && !s.err && e.pending() === 0,
        JSON.stringify({ afterAbort, stillWaiting, done: s.done, err: s.err?.message ?? null, pending: e.delays() }));
    }
    {
      const e = env(() => true);
      const ac = new AbortController();
      const s = state(make(true, e)(ignoring, ac.signal));
      await turn();
      const bounded = e.delays().includes(HOLD_DEADLINE_MS);
      ac.abort(new Error('the tournament URL changed'));
      await turn();
      const fired = e.fireAfter(HOLD_DEADLINE_MS);
      await turn();
      check('stubHoldIgnoresAbort: bounded: from the start of the hold a timer of the poll\'s deadline runs, and a hold file that is never removed ends there, failing with the signal\'s reason, with no timer left',
        bounded && fired && !s.done && s.err?.message === 'the tournament URL changed' && e.pending() === 0,
        JSON.stringify({ bounded, fired, done: s.done, err: s.err?.message ?? null, pending: e.delays() }));
    }
    {
      const e = env(() => true);
      const ac = new AbortController();
      const s = state(make(true, e)({ ...held, stubHoldIgnoresAbort: 'yes' }, ac.signal));
      await turn();
      ac.abort(new Error('deadline'));
      await turn();
      check('stubHoldIgnoresAbort: only the JSON value true turns it on ("yes" does not: the abort ends the wait at once)',
        s.err?.message === 'deadline' && e.pending() === 0, JSON.stringify({ err: s.err?.message ?? null, pending: e.pending() }));
    }
  }

  // End to end: the real sidecar, started with ZIPPER_ALLOW_STUB=1, holds a poll.
  const holdFile = join(TMP_DIR, 'hold-1');
  await writeFile(holdFile, 'hold', 'utf-8');
  await writeFile(join(TMP_DIR, 'held.json'), JSON.stringify({
    tournamentName: 'Held Cup', state: 'in_progress', stubHoldFile: `${TMP_REL}/hold-1`,
    matches: [{ id: 'h1', round: 'Round 1', state: 'in_progress',
      p1: { name: 'Flash', score: 0 }, p2: { name: 'Rival', score: 0 }, winner: 0, order: 1 }],
    standings: [],
  }), 'utf-8');
  const atHeldUrl = mark();
  cmd('setUrl', `stub:${TMP_REL}/held.json`);
  const urlSet = await since(atHeldUrl, (d) => isStatus(d) && parseStatus(d).url === `stub:${TMP_REL}/held.json`);
  const pollAtBefore = urlSet && parseStatus(urlSet).lastPollAt;
  const outAt = sc.out.length;
  const atHeld = mark();
  cmd('pollNow');
  const holding = await waitFor(() => sc.out.slice(outAt).includes('stub: holding the poll'), 4000);
  const atProbe = mark();
  cmd('status');
  const probe = await since(atProbe, isStatus);
  const pushedEarly = client.events.slice(atHeld).some((d) => isUpdate(d) && d.tournament?.name === 'Held Cup');
  check('sidecar: a stub poll is held while its hold file exists (it logs the hold; the poll has not finished; nothing is pushed)',
    !!urlSet && !!holding && !!probe && parseStatus(probe).lastPollAt === pollAtBefore && !pushedEarly,
    JSON.stringify({ holding: !!holding, lastPollAt: probe && parseStatus(probe).lastPollAt, before: pollAtBefore, pushedEarly }));
  await rm(holdFile, { force: true });
  const released = await since(atHeld, (d) => isUpdate(d) && d.tournament?.name === 'Held Cup', 4000);
  check('sidecar: deleting the hold file releases the poll, which then pushes its result', !!released, 'no Held Cup update after the hold file was deleted');
}

// ── 6b. Every command, on the control page's path ────────────────────────────
// ── 6e. The same pass with every name lower-cased (ZIP-08) ──────────────────

async function testEveryCommand(client, sc, since, mark, cmd, { lower = false } = {}) {
  const tag = lower ? 'every command, lower-cased' : 'every command';
  console.log(lower
    ? '\n[6e] every command again, each name lower-cased (as chat relays a dedicated command), on the control page\'s path'
    : '\n[6b] every command in docs/PROTOCOL.md, sent as the control page sends it');
  const protocol = (await readFile(resolve(__dirname, 'docs', 'PROTOCOL.md'), 'utf-8')).replace(/\r\n/g, '\n');
  const section = protocol.slice(protocol.indexOf('## ticker:command'), protocol.indexOf('\n## ', protocol.indexOf('## ticker:command') + 5));
  const tableNames = [...section.matchAll(/^\| `([A-Za-z_]+)`\s*\|/gm)].map((m) => m[1]);
  if (!lower) {
    const src = await readFile(resolve(__dirname, 'zipper-sidecar.mjs'), 'utf-8');
    const body = src.slice(src.indexOf('function dispatch('), src.indexOf('// ── Entry point'));
    const caseNames = [...body.matchAll(/case '([A-Za-z_]+)':/g)].map((m) => m[1]);
    const sameSet = tableNames.length === new Set(tableNames).size && tableNames.length === caseNames.length
      && tableNames.every((n) => caseNames.includes(n));
    check(`every command: docs/PROTOCOL.md's table and dispatch's case labels name the same ${tableNames.length} commands`,
      sameSet && tableNames.length === 16, `table: ${tableNames.join(', ')} | dispatch: ${caseNames.join(', ')}`);
  }

  let lastPollAt = null;
  const statusOf = (d) => (isStatus(d) ? parseStatus(d) : null);
  const capsOf = (d) => (isUpdate(d) ? d.caps : statusOf(d)?.caps);
  // Each command, with a value, and what must follow it: a status or an update that
  // shows the command took effect, so an earlier event cannot stand in for it.
  const plan = [
    ['stop', '', (d) => statusOf(d)?.polling === false],
    ['setUrl', 'stub:platforms/fixtures/stub-result.json', (d) => statusOf(d)?.url === 'stub:platforms/fixtures/stub-result.json'],
    ['setInterval', '25000', (d) => statusOf(d)?.intervalMs === 25000],
    ['setCaps', JSON.stringify({ text: '@EveryCommand', logo: '' }), (d) => capsOf(d)?.text === '@EveryCommand'],
    ['setOptions', JSON.stringify({ intervalMs: 30000, maxItems: 19, topN: 0 }), (d) => statusOf(d)?.intervalMs === 30000 && statusOf(d)?.maxItems === 19],
    ['setMessage', JSON.stringify({ text: 'Every command', enabled: true }), (d) => isUpdate(d) && d.message?.lines?.[0] === 'Every command'],
    ['setMessageText', 'Every command, again', (d) => isUpdate(d) && d.message?.lines?.[0] === 'Every command, again'],
    ['setMessageEnabled', 'off', (d) => statusOf(d)?.message?.enabled === false],
    ['setCapsText', '@EveryCommand2', (d) => capsOf(d)?.text === '@EveryCommand2'],
    ['setCapsLogo', '/zipper-overlay/every.png', (d) => capsOf(d)?.logo === '/zipper-overlay/every.png'],
    ['setMaxItems', '18', (d) => statusOf(d)?.maxItems === 18],
    ['setTopN', '1', (d) => statusOf(d)?.topN === 1],
    ['setAutoSchedule', 'on', (d) => statusOf(d)?.auto?.enabled === true],
    ['start', '', (d) => statusOf(d)?.polling === true && !!statusOf(d)?.lastPollAt && statusOf(d).lastPollAt !== lastPollAt],
    ['pollNow', '', (d) => !!statusOf(d)?.lastPollAt && statusOf(d).lastPollAt !== lastPollAt],
    ['status', '', (d) => !!statusOf(d)],
  ];
  const sent = new Set();
  for (const [name, value, pred] of plan) {
    sent.add(name);
    const as = lower ? name.toLowerCase() : name;
    const at = mark(), outAt = sc.out.length;
    cmd(as, value);
    // A status counts only when it carries no commandNotice. An unknown or refused
    // command always leaves one, and its status can repeat a value already in place (in
    // the lower-cased pass the URL and autoSchedule on are), so without this a refused
    // name could pass as taken.
    const hit = await since(at, (d) => pred(d) && (!isStatus(d) || parseStatus(d).commandNotice === ''));
    const logged = await waitFor(() => sc.out.slice(outAt).includes(`command: ${as}`), 2000);
    const unknown = sc.out.slice(outAt).includes(`unknown command "${as}"`);
    if (statusOf(hit)?.lastPollAt) lastPollAt = statusOf(hit).lastPollAt;
    else { const last = client.events.filter(isStatus).pop(); if (last) lastPollAt = parseStatus(last).lastPollAt; }
    check(`${tag}: ${as} is followed by its status or update, and the sidecar does not call it unknown`,
      !!hit && !!logged && !unknown,
      [!hit && 'no status or update showing it took effect', unknown && 'the sidecar logged it as an unknown command', !logged && 'the sidecar did not log it']
        .filter(Boolean).join('; '));
  }
  check(`${tag}: each name in the table was sent`, tableNames.every((n) => sent.has(n)), tableNames.filter((n) => !sent.has(n)).join(', '));
  // Polling off for the ranges below.
  const atOff = mark();
  cmd('stop');
  await since(atOff, (d) => statusOf(d)?.polling === false);
}

// ── 6c. Every range: the command path, read back from the status ─────────────

async function testCommandRanges(since, mark, cmd) {
  console.log('\n[6c] every range on the command path (at and past both ends)');
  const fieldOf = (d, f) => (isStatus(d) ? parseStatus(d)[f] : undefined);
  const single = [
    ['setInterval', 'intervalMs', [['3600001', 3600000], ['14999', 15000], ['3600000', 3600000], ['15000', 15000]]],
    ['setMaxItems', 'maxItems', [['101', 100], ['0', 1], ['100', 100], ['1', 1]]],
    ['setTopN', 'topN', [['101', 100], ['-1', 0], ['100', 100], ['0', 0]]],
  ];
  for (const [name, field, tries] of single) {
    const wrong = [];
    for (const [value, want] of tries) {
      const at = mark();
      cmd(name, value);
      const hit = await since(at, (d) => fieldOf(d, field) === want);
      if (!hit) wrong.push(`${value} → wanted ${want}`);
    }
    check(`range: ${name} clamps at and past both ends (${tries.map(([v, w]) => `${v}→${w}`).join(', ')})`, wrong.length === 0, wrong.join('; '));
  }
  const opts = [
    [{ intervalMs: 3600001, maxItems: 101, topN: 101 }, { intervalMs: 3600000, maxItems: 100, topN: 100 }],
    [{ intervalMs: 14999, maxItems: 0, topN: -1 }, { intervalMs: 15000, maxItems: 1, topN: 0 }],
    [{ intervalMs: 3600000, maxItems: 100, topN: 100 }, { intervalMs: 3600000, maxItems: 100, topN: 100 }],
    [{ intervalMs: 15000, maxItems: 1, topN: 0 }, { intervalMs: 15000, maxItems: 1, topN: 0 }],
  ];
  const wrong = [];
  for (const [value, want] of opts) {
    const at = mark();
    cmd('setOptions', JSON.stringify(value));
    const hit = await since(at, (d) => isStatus(d) && Object.entries(want).every(([k, v]) => parseStatus(d)[k] === v));
    if (!hit) wrong.push(`${JSON.stringify(value)} → wanted ${JSON.stringify(want)}`);
  }
  check('range: setOptions clamps intervalMs, maxItems and topN at and past both ends', wrong.length === 0, wrong.join('; '));
}

// ── 6d. Every range: the config path, read back from the saved config.json ───

async function testConfigRanges(client) {
  console.log('\n[6d] every range on the config path (at and past both ends), read back from the saved config');
  // The status does not carry pollSeconds or the milestones, so each value is read back
  // from config.json once the sidecar has saved it (setAutoSchedule makes it save).
  const fields = {
    pollIntervalMs: [(c) => c.pollIntervalMs, 15000, 3600000],
    maxItems: [(c) => c.maxItems, 1, 100],
    topN: [(c) => c.topN, 0, 100],
    'streamElements.pollSeconds': [(c) => c.streamElements?.pollSeconds, 5, 3600],
    'milestones.followers': [(c) => c.milestones?.followers, 0, 1000000],
    'milestones.subs': [(c) => c.milestones?.subs, 0, 1000000],
    'milestones.tips': [(c) => c.milestones?.tips, 0, 1000000],
    'milestones.cheers': [(c) => c.milestones?.cheers, 0, 1000000],
  };
  const positions = [
    ['one below the bottom', (lo) => lo - 1, (lo) => lo],
    ['at the bottom', (lo) => lo, (lo) => lo],
    ['at the top', (lo, hi) => hi, (lo, hi) => hi],
    ['one above the top', (lo, hi) => hi + 1, (lo, hi) => hi],
  ];
  const wrong = Object.fromEntries(Object.keys(fields).map((k) => [k, []]));
  for (const [where, give, want] of positions) {
    const v = (k) => give(fields[k][1], fields[k][2]);
    const cfgPath = join(TMP_DIR, 'config-range.json');
    await writeFile(cfgPath, JSON.stringify({
      tournamentUrl: null, autoStart: false, autoSchedule: false, caps: { text: '', logo: '' },
      pollIntervalMs: v('pollIntervalMs'), maxItems: v('maxItems'), topN: v('topN'),
      streamElements: { jwtToken: null, pollSeconds: v('streamElements.pollSeconds') },
      milestones: { followers: v('milestones.followers'), subs: v('milestones.subs'), tips: v('milestones.tips'), cheers: v('milestones.cheers') },
    }), 'utf-8');
    const at = client.events.length;
    const sc = startSidecar(`range sidecar (${where})`, { ZIPPER_CONFIG: cfgPath, ZIPPER_GUARD_PORT: String(GUARD_PORT) });
    const up = await sinceOn(client, sc)(at, isStatus, 5000);
    if (up) {
      client.ws.send(JSON.stringify({ request: 'DoAction', id: 'r', action: { name: 'Ticker Command' }, args: { command: 'setAutoSchedule', value: 'on' } }));
    }
    const got = up ? await savedConfig(cfgPath, (c) => c.autoSchedule === true, 4000) : null;
    for (const [k, [read, lo, hi]] of Object.entries(fields)) {
      const value = got && got.autoSchedule === true ? read(got) : undefined;
      if (value !== want(lo, hi)) wrong[k].push(`${where}: ${give(lo, hi)} → wanted ${want(lo, hi)}, saved ${value === undefined ? '(nothing saved)' : value}`);
    }
    await sc.stop();
    await rm(cfgPath, { force: true });
  }
  for (const [k, [, lo, hi]] of Object.entries(fields)) {
    check(`range: config.json ${k} is clamped to ${lo}–${hi} at and past both ends`, wrong[k].length === 0, wrong[k].join('; '));
  }
}

// ── 6f. Which commands the sidecar accepts (ZIP-01, ZIP-08, ZIP-12) ─────────

// The sidecar's fixed messages. None quotes a file's content, a path or a value sent.
const MSG = {
  stubOff: 'stub: URLs are for the tests and work only with ZIPPER_ALLOW_STUB=1',
  stubOutside: 'stub: URLs may name only a file inside the Zipper folder',
  stubUnreadable: 'stub: the test file could not be read',
  stubNotObject: 'stub: the test file is not a JSON object',
  logo: 'Logo not changed: use an http(s) URL or a path on the overlay\'s server, such as /zipper-overlay/assets/logo.png',
  noUrl: 'No tournament URL set',
  // A stub fixture's stubFail (section 6h): a poll that fails as a site answering 429 would.
  stub429: 'stub: the test file answered as HTTP 429',
};
// An in-progress fixture inside the repo, which every sidecar here may read.
const SAFE_STUB = 'stub:platforms/fixtures/stub-result.json';

async function testAccepts(client, sc, since, mark, cmd, cfgPath) {
  console.log('\n[6f] which commands the sidecar accepts: names in any case, __status, stub: URLs, the logo, an empty URL; refusals in commandNotice');
  const statusOf = (d) => (isStatus(d) ? parseStatus(d) : null);
  // A chat Command trigger, as the mock's copy of Ticker Command takes it.
  const chat = (command, rawInput) => client.ws.send(JSON.stringify({
    request: 'DoAction', id: 'chat', action: { name: 'Ticker Command' },
    args: { command, commandId: 'verify-chat', rawInput },
  }));
  const firstAfter = (at, pred) => { const i = client.events.findIndex((d, j) => j >= at && pred(d)); return i < 0 ? null : i; };
  // Polling off and the URL a fixture inside the repo, sent with the exact names every
  // sidecar knows. Nothing below polls a URL the sidecar should refuse, so even a
  // sidecar that accepted one would open no file outside the repo.
  const ready = async () => {
    let at = mark();
    cmd('stop');
    await since(at, (d) => statusOf(d)?.polling === false);
    at = mark();
    cmd('setUrl', SAFE_STUB);
    return statusOf(await since(at, (d) => statusOf(d)?.url === SAFE_STUB));
  };

  // __status in any casing: our own status relay echoing back, ignored the same way.
  {
    const base = await ready();
    const outAt = sc.out.length;
    const at = mark();
    const forged = JSON.stringify({ type: 'ticker:status', url: 'stub:C:\\Windows\\win.ini', topN: 77, polling: true });
    cmd('__status', forged);
    cmd('__STATUS', forged);
    cmd('__Status', forged);
    // A marker: the first status any of the three could be followed by is this one's.
    const topN = (base?.topN ?? 0) === 3 ? 4 : 3;
    cmd('setTopN', String(topN));
    await since(at, (d) => statusOf(d)?.topN === topN);
    const markerAt = firstAfter(at, (d) => statusOf(d)?.topN === topN);
    // The mock relays this test's own __status send as well; only a status the sidecar
    // sent counts as drawn.
    const drawn = markerAt === null ? null : client.events.slice(at, markerAt).filter((d) => isStatus(d) && d.value !== forged).length;
    const m = markerAt === null ? null : statusOf(client.events[markerAt]);
    const keys = ['polling', 'url', 'platform', 'lastError', 'intervalMs', 'maxItems', 'caps', 'message'];
    const changed = m && base ? keys.filter((k) => JSON.stringify(m[k]) !== JSON.stringify(base[k])) : ['(no status)'];
    const savedNow = await savedConfig(cfgPath, (c) => c.topN === topN);
    const out = sc.out.slice(outAt);
    const logged = /command: __status|unknown command/i.test(out);
    check('__status, __STATUS and __Status are all ignored as the sidecar\'s own echo: no status in reply, no status field or saved setting changed, nothing logged',
      drawn === 0 && changed.length === 0 && savedNow?.tournamentUrl === SAFE_STUB && savedNow?.topN === topN && !logged,
      JSON.stringify({ statusesDrawn: drawn, changed, savedUrl: savedNow?.tournamentUrl, logged: logged ? out.split('\n').filter((l) => /__status/i.test(l)) : false }));
  }

  // An unknown command: reported by name only, cut to 40 characters, in commandNotice.
  {
    const name = 'Zq' + 'x'.repeat(38) + 'TAIL' + 'y'.repeat(456); // 500 characters
    const value = 'VALUE-CANARY';
    const outAt = sc.out.length;
    let at = mark();
    cmd(name, value);
    const hit = await since(at, (d) => !!statusOf(d)?.commandNotice);
    const notice = statusOf(hit)?.commandNotice || '';
    const out = sc.out.slice(outAt);
    check('an unknown 500-character command is reported in commandNotice by name only, cut to 40 characters, never with its value; the console lines cut it too',
      notice === `unknown command "${name.slice(0, 40)}…" — ignored` && !notice.includes(value)
        && out.includes(`unknown command "${name.slice(0, 40)}…"`) && out.includes(`command: ${name.slice(0, 40)}…`) && !out.includes('TAIL'),
      JSON.stringify({ notice, out: out.slice(0, 400) }));
    at = mark();
    cmd('status');
    const kept = statusOf(await since(at, isStatus));
    at = mark();
    cmd('setTopN', '0');
    const cleared = statusOf(await since(at, (d) => statusOf(d)?.topN === 0));
    check('commandNotice outlasts a status request, and the next accepted command clears it',
      notice !== '' && kept?.commandNotice === notice && cleared?.commandNotice === '',
      JSON.stringify({ afterStatus: kept?.commandNotice, afterSetTopN: cleared?.commandNotice }));

    // The cut is by whole characters: an emoji as the 40th character is kept whole, never
    // split into half a surrogate pair.
    const emoji = '\u{1F600}';
    const emojiName = 'e'.repeat(39) + emoji + 'tail';
    at = mark();
    cmd(emojiName, '');
    const eh = statusOf(await since(at, (d) => !!statusOf(d)?.commandNotice));
    check('an unknown name is cut by whole characters: an emoji as the 40th character is kept whole, then "…"',
      eh?.commandNotice === `unknown command "${'e'.repeat(39)}${emoji}…" — ignored`,
      JSON.stringify({ commandNotice: eh?.commandNotice }));
  }

  // The chat path: names arrive lower-cased (the C# lower-cases a dedicated command).
  {
    let at = mark();
    cmd('status');
    const before = statusOf(await since(at, isStatus));
    at = mark();
    chat('!pollnow', '');
    const polled = await since(at, (d) => !!statusOf(d)?.lastPollAt && statusOf(d).lastPollAt !== before?.lastPollAt);
    check('chat: !pollnow (relayed as "pollnow") polls', !!polled && statusOf(polled).lastError === '',
      polled ? JSON.stringify({ lastError: statusOf(polled).lastError }) : 'no poll after !pollnow');
    at = mark();
    cmd('stop');
    await since(at, (d) => statusOf(d)?.polling === false);

    const inRepo = 'stub:./platforms/fixtures/stub-result.json';
    at = mark();
    chat('!ticker', `seturl ${inRepo}`);
    const set = statusOf(await since(at, (d) => statusOf(d)?.url === inRepo));
    check('chat: !ticker seturl (relayed as "seturl") sets a stub: URL inside the repo', set?.url === inRepo && set?.commandNotice === '',
      JSON.stringify({ url: set?.url, commandNotice: set?.commandNotice }));
    at = mark();
    chat('!ticker', 'seturl stub:C:\\Windows\\win.ini');
    const refused = statusOf(await since(at, (d) => !!statusOf(d)?.commandNotice));
    check('chat: !ticker seturl stub:C:\\Windows\\win.ini is refused by the sidecar with the confinement message; the URL is kept',
      refused?.commandNotice === MSG.stubOutside && refused?.url === inRepo && refused?.lastError === '',
      JSON.stringify({ commandNotice: refused?.commandNotice, url: refused?.url, lastError: refused?.lastError }));

    // setCapsLogo javascript:… through chat too, lower-cased as a chatter may type it.
    at = mark();
    chat('!ticker', 'setcapslogo javascript:alert(1)');
    const jsChat = statusOf(await since(at, (d) => statusOf(d)?.commandNotice === MSG.logo));
    const jsChatPushed = client.events.slice(at).some((d) => isUpdate(d) && /javascript/i.test(d.caps?.logo || ''));
    check('chat: !ticker setcapslogo javascript:… is refused by the sidecar; the logo is kept, nothing is pushed with it, and commandNotice says why',
      !!jsChat && jsChat.caps?.logo === set?.caps?.logo && !jsChatPushed,
      JSON.stringify({ logo: jsChat?.caps?.logo, before: set?.caps?.logo, commandNotice: jsChat?.commandNotice, jsChatPushed }));

    // An empty seturl through chat while polling. The poll that start begins is waited
    // out first (its closing status carries a new lastPollAt), so none is in flight.
    at = mark();
    cmd('start');
    const chatRunning = statusOf(await since(at, (d) => statusOf(d)?.polling === true && !!statusOf(d).lastPollAt && statusOf(d).lastPollAt !== set?.lastPollAt));
    at = mark();
    chat('!ticker', 'seturl');
    const emptied = statusOf(await since(at, (d) => isStatus(d) && statusOf(d).url === null));
    check('chat: !ticker seturl with no value while polling clears the URL and stops polling',
      !!chatRunning && emptied?.url === null && emptied?.polling === false && emptied?.commandNotice === '',
      JSON.stringify({ running: !!chatRunning, url: emptied?.url, polling: emptied?.polling, commandNotice: emptied?.commandNotice }));
  }

  // Confinement (ZIPPER_ALLOW_STUB=1 here), checked as text before any file call.
  {
    // The check itself, in-process, against a stand-in folder path with a "k" in its
    // name, as the published folder has: a sibling spelled with a Kelvin sign (U+212A,
    // which JavaScript's toLowerCase folds to "k" and NTFS keeps apart), and the network
    // forms of the folder's own path. Windows path arithmetic only: nothing is opened.
    const src = await readFile(resolve(__dirname, 'zipper-sidecar.mjs'), 'utf-8');
    const fn = (/\nfunction stubRefusal\(url\) \{\r?\n[\s\S]*?\r?\n\}\r?\n/.exec(src) || [])[0];
    let inproc = 'zipper-sidecar.mjs has no stubRefusal(url) function';
    if (fn) {
      const root = 'C:\\dev\\StreamerBot\\zipper-ticker';
      const refusal = new Function('resolve', 'relative', 'isAbsolute', 'sep', '__dirname', 'STUB_ALLOWED', 'STUB_OFF_MESSAGE', 'STUB_OUTSIDE_MESSAGE',
        `${fn}\nreturn stubRefusal;`)(win32.resolve, win32.relative, win32.isAbsolute, win32.sep, root, true, MSG.stubOff, MSG.stubOutside);
      const cases = [
        ['a sibling spelled with a Kelvin sign', 'stub:..\\zipper-tic\u212Aer\\x.json', MSG.stubOutside],
        ['UNC naming the folder\'s own path', `stub:\\\\${root}\\x.json`, MSG.stubOutside],
        ['its forward-slash form', `stub://${root.replace(/\\/g, '/')}/x.json`, MSG.stubOutside],
        ['the same in lower case', `stub:\\\\${root.toLowerCase()}\\x.json`, MSG.stubOutside],
        ['a sibling whose name starts with the folder\'s', 'stub:..\\zipper-ticker-x\\x.json', MSG.stubOutside],
        ['.. traversal', 'stub:platforms\\..\\..\\x.json', MSG.stubOutside],
        ['a relative path inside', 'stub:platforms\\fixtures\\stub-result.json', null],
        ['an absolute path inside', `stub:${root}\\platforms\\x.json`, null],
      ];
      inproc = cases.filter(([, url, want]) => refusal(url) !== want).map(([what, url]) => `${what} (${url}): ${JSON.stringify(refusal(url))}`).join(' || ');
    }
    check('confinement, in-process: a sibling spelled with a Kelvin sign and \\\\ or // forms of the folder\'s own path are refused; paths inside are accepted',
      !!fn && inproc === '', inproc);

    // The state a refused setUrl must leave alone is made non-default first: a fixture
    // inside the repo is polled (event state, start time, platform), then made unreadable
    // and polled again (lastError), then polling stops.
    await ready();
    const flip = `stub:${TMP_REL}/flip.json`;
    const flipStart = '2026-01-02T03:04:05.000Z';
    await writeFile(join(TMP_DIR, 'flip.json'), JSON.stringify({ tournamentName: 'Flip Cup', state: 'in_progress', startAt: flipStart, matches: [], standings: [] }), 'utf-8');
    let at = mark();
    cmd('setUrl', flip);
    const flipSet = statusOf(await since(at, (d) => statusOf(d)?.url === flip));
    at = mark();
    cmd('pollNow');
    const live = statusOf(await since(at, (d) => statusOf(d)?.auto?.eventState === 'in_progress' && !!statusOf(d).lastPollAt && statusOf(d).lastPollAt !== flipSet?.lastPollAt));
    await writeFile(join(TMP_DIR, 'flip.json'), 'not JSON', 'utf-8');
    at = mark();
    cmd('pollNow');
    const failed = statusOf(await since(at, (d) => statusOf(d)?.lastError === MSG.stubNotObject));
    at = mark();
    cmd('stop');
    await since(at, (d) => statusOf(d)?.polling === false);
    const want = { url: flip, platform: 'stub', lastError: MSG.stubNotObject, eventState: 'in_progress', startAt: flipStart, polling: false };
    const stateOf = (s) => s && { url: s.url, platform: s.platform, lastError: s.lastError, eventState: s.auto?.eventState, startAt: s.auto?.startAt, polling: s.polling };
    check('confinement, set-up: the URL is an in-repo fixture with an event state and a start time, and its last poll failed',
      !!live && JSON.stringify(stateOf(failed)) === JSON.stringify({ ...want, polling: true }), JSON.stringify(stateOf(failed)));
    // The baseline is taken once stop's save has landed, and two reads 100 ms apart
    // agree, so a save still on its way cannot land after it.
    await savedConfig(cfgPath, (c) => c.tournamentUrl === flip && c.autoStart === false);
    let bytesBefore = null;
    for (let i = 0; i < 30; i++) {
      let cur = null;
      try { cur = await readFile(cfgPath); } catch {}
      if (cur && bytesBefore && cur.equals(bytesBefore)) break;
      bytesBefore = cur;
      await sleep(100);
    }

    const at0 = mark();
    const outside = [
      ['drive-absolute, outside the repo', 'stub:C:\\Windows\\win.ini'],
      ['.. traversal', 'stub:..\\x.json'],
      ['a sibling folder whose name starts with this folder\'s name', `stub:..\\${basename(__dirname)}-x\\x.json`],
      ['a sibling folder whose name starts with the published folder\'s name', 'stub:..\\zipper-ticker-x\\x.json'],
      ['a UNC path on a host that cannot resolve', 'stub:\\\\zipper-verify.invalid\\share\\x.json'],
      ['its forward-slash form', 'stub://zipper-verify.invalid/share/x.json'],
      // Polling is off, so even a sidecar that took these would open nothing.
      ['a UNC path whose text is this folder\'s own path', `stub:\\\\${__dirname}\\x.json`],
      ['its forward-slash form', `stub://${__dirname.replace(/\\/g, '/')}/x.json`],
      ['nothing after stub:', 'stub:'],
    ];
    const wrong = [];
    for (const [what, url] of outside) {
      at = mark();
      cmd('setUrl', url);
      const s = statusOf(await since(at, isStatus));
      if (s?.commandNotice !== MSG.stubOutside || JSON.stringify(stateOf(s)) !== JSON.stringify(want)) {
        wrong.push(`${what} (${url}): ${s ? JSON.stringify({ commandNotice: s.commandNotice, ...stateOf(s) }) : 'no status'}`);
      }
    }
    check('confinement: setUrl refuses a stub: path outside the repo (drive-absolute, .., a sibling folder named like the repo, UNC both ways, UNC spelling this folder\'s path, empty) with the confinement message, never ENOENT or a network error; the URL, platform, event state, start time and last poll error are kept',
      wrong.length === 0, wrong.join(' || '));
    // A save starts before the status that follows it is sent: watch the file for a
    // change for a while after the last refusal's status. Polling is off here, so an
    // accepted setUrl pushes nothing either: the update count can fail only from row 4.6
    // on (an accepted URL blanks the strip). The check while polling, below, covers it now.
    const changed = await waitFor(async () => { try { return !(await readFile(cfgPath)).equals(bytesBefore); } catch { return false; } }, 1000, 50);
    const pushed = client.events.slice(at0).filter(isUpdate).length;
    check('confinement: the refused setUrls leave config.json byte-identical and push no ticker:update (the update count is live from row 4.6)',
      !!bytesBefore && !changed && pushed === 0, JSON.stringify({ configChanged: !!changed, updatesPushed: pushed }));

    // While polling, a refused setUrl starts no poll and pushes nothing (an accepted one
    // polls at once), and the URL and polling stay as they were.
    at = mark();
    cmd('setUrl', SAFE_STUB);
    const safeSet = statusOf(await since(at, (d) => statusOf(d)?.url === SAFE_STUB));
    at = mark();
    cmd('start');
    const running = statusOf(await since(at, (d) => statusOf(d)?.polling === true && !!statusOf(d).lastPollAt && statusOf(d).lastPollAt !== safeSet?.lastPollAt));
    at = mark();
    cmd('setUrl', 'stub:..\\zipper-verify-outside.json');
    const whileOn = statusOf(await since(at, (d) => statusOf(d)?.commandNotice === MSG.stubOutside));
    await sleep(500);
    const polledAfter = client.events.slice(at).filter(isStatus).map(parseStatus).some((s) => s.lastPollAt !== running?.lastPollAt);
    const pushedAfter = client.events.slice(at).filter(isUpdate).length;
    check('confinement: while polling, a refused setUrl starts no poll and pushes no ticker:update; the URL is kept and polling stays on',
      !!running && whileOn?.url === SAFE_STUB && whileOn?.polling === true && !polledAfter && pushedAfter === 0,
      JSON.stringify({ running: !!running, url: whileOn?.url, polling: whileOn?.polling, polledAfter, pushedAfter }));
    at = mark();
    cmd('stop');
    await since(at, (d) => statusOf(d)?.polling === false);

    const abs = `stub:${resolve(__dirname, 'platforms', 'fixtures', 'stub-result.json')}`;
    at = mark();
    cmd('setUrl', abs);
    const acc = statusOf(await since(at, (d) => statusOf(d)?.url === abs));
    at = mark();
    cmd('pollNow');
    const upd = await since(at, (d) => isUpdate(d) && d.tournament?.url === abs);
    check('confinement: an absolute stub: path inside the repo is accepted and read',
      acc?.url === abs && !acc.commandNotice && acc.platform === 'stub' && upd?.tournament?.name === 'Stub Invitational',
      JSON.stringify({ url: acc?.url, commandNotice: acc?.commandNotice, pushed: upd?.tournament?.name }));
  }

  // Read and parse failures: fixed messages that quote no file content and no path.
  {
    await ready();
    await writeFile(join(TMP_DIR, 'canary.txt'), 'sk_live_CANARY is not JSON', 'utf-8');
    await writeFile(join(TMP_DIR, 'null.json'), 'null', 'utf-8');
    const tries = [
      ['a file that is not JSON', `stub:${TMP_REL}/canary.txt`, MSG.stubNotObject],
      ['a file that holds JSON null', `stub:${TMP_REL}/null.json`, MSG.stubNotObject],
      ['a file that is not there', `stub:${TMP_REL}/missing.json`, MSG.stubUnreadable],
    ];
    const wrong = [];
    const outAt = sc.out.length;
    for (const [what, url, want] of tries) {
      let at = mark();
      cmd('setUrl', url);
      await since(at, (d) => statusOf(d)?.url === url);
      at = mark();
      cmd('pollNow');
      const s = statusOf(await since(at, (d) => statusOf(d)?.url === url && !!statusOf(d)?.lastError));
      if (s?.lastError !== want) wrong.push(`${what}: ${JSON.stringify(s?.lastError ?? 'no failed poll seen')}`);
      at = mark();
      cmd('stop');
      await since(at, (d) => statusOf(d)?.polling === false);
    }
    const out = sc.out.slice(outAt);
    // The log line of each failed poll carries the same fixed message, and nothing else.
    const logged = out.split(/\r?\n/).filter((l) => l.includes('poll failed')).map((l) => l.replace(/^.*poll failed \(\d+x\): /, ''));
    check('stub: read and parse failures put a fixed message in lastError and in the log line, quoting no file content and no path',
      wrong.length === 0 && JSON.stringify(logged) === JSON.stringify(tries.map(([, , want]) => want)) && !out.includes('sk_live'),
      wrong.join(' || ') || JSON.stringify({ logged, leaked: out.includes('sk_live') }));
  }

  // The logo: an http(s) URL or a path on the overlay's own server, on every way in.
  {
    await ready(); // polling off: each caps command pushes its update at once
    const keep = '/zipper-overlay/x.png';
    let at = mark();
    cmd('setCaps', JSON.stringify({ text: '@LogoBase', logo: keep }));
    await since(at, (d) => isUpdate(d) && d.caps?.text === '@LogoBase' && d.caps?.logo === keep);
    at = mark();
    cmd('setCapsLogo', 'javascript:alert(1)');
    const js = statusOf(await since(at, isStatus));
    const jsPushed = client.events.slice(at).some((d) => isUpdate(d) && /javascript/i.test(d.caps?.logo || ''));
    check('logo: setCapsLogo javascript:… is refused; the logo is kept, nothing is pushed with it, and commandNotice says why',
      js?.caps?.logo === keep && js?.commandNotice === MSG.logo && !jsPushed, JSON.stringify({ logo: js?.caps?.logo, commandNotice: js?.commandNotice, jsPushed }));

    const refused = [
      ['//host', '//zipper-verify.invalid/x.png'],
      ['\\\\host', '\\\\zipper-verify.invalid\\x.png'],
      ['/\\host', '/\\zipper-verify.invalid/x.png'],
      ['data:', 'data:image/png;base64,iVBORw0KGgo='],
      ['file:', 'file:///C:/Windows/win.ini'],
      ['javascript:', 'javascript:alert(1)'],
      ['blob:', 'blob:http://127.0.0.1:7474/x'],
      ['a tab inside //host', '/\t/zipper-verify.invalid/x.png'],
      ['a newline inside \\\\host', '\\\n\\zipper-verify.invalid\\x.png'],
      ['a newline inside javascript:', 'java\nscript:alert(1)'],
    ];
    const wrong = [];
    for (const [i, [what, logo]] of refused.entries()) {
      const text = `@Refused${i}`;
      at = mark();
      cmd('setCaps', JSON.stringify({ text, logo }));
      const upd = await since(at, (d) => isUpdate(d) && d.caps?.text === text);
      const s = statusOf(await since(at, (d) => statusOf(d)?.caps?.text === text));
      if (upd?.caps?.logo !== keep || s?.caps?.logo !== keep || s?.commandNotice !== MSG.logo) {
        wrong.push(`${what}: ${JSON.stringify({ pushedLogo: upd?.caps?.logo, statusLogo: s?.caps?.logo, commandNotice: s?.commandNotice })}`);
      }
    }
    check('logo: setCaps with a valid text and a refused logo (//host, \\\\host, /\\host, data:, file:, javascript:, blob:, tab and newline variants): the text applies, the logo is kept, commandNotice says why',
      wrong.length === 0, wrong.join(' || '));

    const accepted = ['https://zipper-verify.invalid/logo.png', 'HTTP://zipper-verify.invalid/logo.png', 'assets/logo.png', ' /zipper-overlay/x.png ', ''];
    const bad = [];
    for (const [i, logo] of accepted.entries()) {
      const text = `@Accepted${i}`;
      at = mark();
      cmd('setCaps', JSON.stringify({ text, logo }));
      const upd = await since(at, (d) => isUpdate(d) && d.caps?.text === text);
      const s = statusOf(await since(at, (d) => statusOf(d)?.caps?.text === text));
      if (upd?.caps?.logo !== logo.trim() || s?.commandNotice !== '') bad.push(`${JSON.stringify(logo)}: ${JSON.stringify({ pushedLogo: upd?.caps?.logo, commandNotice: s?.commandNotice })}`);
    }
    check('logo: an http(s) URL (remote hosts allowed), a relative path, a path on the overlay\'s server (trimmed) and an empty value (clears the cap) are accepted',
      bad.length === 0, bad.join(' || '));

    // The notice outlasts the next poll, which row 6.1 relies on: with polling on, a
    // refused logo in setCaps sets it, setCaps starts a poll, and the status that closes
    // that poll still carries it.
    at = mark();
    cmd('setCaps', JSON.stringify({ text: '@PollBase', logo: keep }));
    const pollBase = statusOf(await since(at, (d) => statusOf(d)?.caps?.text === '@PollBase'));
    at = mark();
    cmd('start');
    const on = statusOf(await since(at, (d) => statusOf(d)?.polling === true && !!statusOf(d).lastPollAt && statusOf(d).lastPollAt !== pollBase?.lastPollAt));
    at = mark();
    cmd('setCaps', JSON.stringify({ text: '@DuringPoll', logo: 'javascript:alert(1)' }));
    const afterPoll = statusOf(await since(at, (d) => statusOf(d)?.caps?.text === '@DuringPoll' && !!statusOf(d).lastPollAt && statusOf(d).lastPollAt !== on?.lastPollAt));
    check('logo: commandNotice outlasts the next poll: with polling on, the status that closes the poll setCaps starts still says why the logo was refused',
      !!on && afterPoll?.commandNotice === MSG.logo && afterPoll?.caps?.logo === keep && afterPoll?.lastError === '',
      JSON.stringify({ polled: !!afterPoll, commandNotice: afterPoll?.commandNotice, logo: afterPoll?.caps?.logo, lastError: afterPoll?.lastError }));
    at = mark();
    cmd('stop');
    await since(at, (d) => statusOf(d)?.polling === false);
  }

  // ZIP-12: clearing the URL while polling stops polling; start and pollNow with no URL
  // are refused in commandNotice, not lastError.
  {
    const base = await ready();
    let at = mark();
    cmd('start');
    // The status pollOnce sends before its fetch also shows polling on, with the last
    // poll's time: wait for the one that closes this poll (a new lastPollAt), so no poll
    // is in flight when the URL is cleared and none of its statuses can arrive later.
    await since(at, (d) => statusOf(d)?.polling === true && !!statusOf(d)?.lastPollAt && statusOf(d).lastPollAt !== base?.lastPollAt);
    at = mark();
    cmd('setUrl', '');
    const cleared = statusOf(await since(at, (d) => isStatus(d) && statusOf(d).url === null));
    const savedCleared = await savedConfig(cfgPath, (c) => c.tournamentUrl === null && c.autoStart === false);
    check('setUrl with an empty value while polling clears the URL and stops polling (saved: no URL, autoStart off)',
      cleared?.url === null && cleared?.polling === false && cleared?.lastError === '' && savedCleared?.tournamentUrl === null && savedCleared?.autoStart === false,
      JSON.stringify({ url: cleared?.url, polling: cleared?.polling, lastError: cleared?.lastError, saved: savedCleared && { tournamentUrl: savedCleared.tournamentUrl, autoStart: savedCleared.autoStart } }));
    // Each refusal is read from the status that carries its notice, not the first status
    // after the command.
    at = mark();
    cmd('start');
    const s1 = statusOf(await since(at, (d) => statusOf(d)?.commandNotice === MSG.noUrl));
    at = mark();
    cmd('pollNow');
    const s2 = statusOf(await since(at, (d) => statusOf(d)?.commandNotice === MSG.noUrl));
    check('start and pollNow with no URL are refused in commandNotice ("No tournament URL set"), not in lastError; polling stays off',
      [s1, s2].every((s) => s?.commandNotice === MSG.noUrl && s.lastError === '' && s.polling === false),
      JSON.stringify([s1, s2].map((s) => s && { commandNotice: s.commandNotice, lastError: s.lastError, polling: s.polling })));
  }

  // ZIP-12 while waiting for an event's start time: an empty setUrl also clears
  // waitingUntil and the pending wake-up poll, which would otherwise still run.
  {
    await ready();
    const later = `stub:${TMP_REL}/later.json`;
    const startAt = new Date(Date.now() + 2500).toISOString();
    await writeFile(join(TMP_DIR, 'later.json'), JSON.stringify({ tournamentName: 'Later Cup', state: 'upcoming', startAt, matches: [], standings: [] }), 'utf-8');
    let at = mark();
    cmd('setUrl', later);
    await since(at, (d) => statusOf(d)?.url === later);
    at = mark();
    cmd('start');
    const waiting = statusOf(await since(at, (d) => !!statusOf(d)?.auto?.waitingUntil));
    at = mark();
    cmd('setUrl', '');
    const cleared = statusOf(await since(at, (d) => isStatus(d) && statusOf(d).url === null));
    // Wait past the start time: by then a wake-up poll left pending would have run, and
    // with no URL it would show as a new lastPollAt and a lastError.
    await sleep(Math.max(0, Date.parse(startAt) - Date.now()) + 1000);
    const woke = client.events.slice(at).filter(isStatus).map(parseStatus).filter((s) => s.lastPollAt !== cleared?.lastPollAt || s.lastError);
    check('setUrl with an empty value while waiting for the start time clears waitingUntil, and the pending wake-up poll never runs',
      !!waiting && cleared?.polling === false && cleared?.auto?.waitingUntil === null && woke.length === 0,
      JSON.stringify({ waited: waiting?.auto?.waitingUntil ?? null, polling: cleared?.polling, waitingUntil: cleared?.auto?.waitingUntil, woke: woke.map((s) => ({ lastPollAt: s.lastPollAt, lastError: s.lastError })) }));
  }
  await ready();
}

// ── 6g. stub: without ZIPPER_ALLOW_STUB, and outside the repo, at the read ──

async function testStubAtTheRead(client) {
  console.log('\n[6g] a stub: URL from config.json: without ZIPPER_ALLOW_STUB, and outside the repo, refused at the poll; a saved logo that fails the rule');
  let stubOffSeen = null; // lastError of the sidecar without the variable, as it reported it

  // Without the variable: its own spawn, config file and guard port.
  {
    const cfgPath = join(TMP_DIR, 'config-nostub.json');
    await writeFile(cfgPath, JSON.stringify({ tournamentUrl: SAFE_STUB, autoStart: true, pollIntervalMs: 15000, caps: { text: '', logo: '' } }), 'utf-8');
    const at = client.events.length;
    const sc = startSidecar('no-stub sidecar [6g]', { ZIPPER_CONFIG: cfgPath, ZIPPER_GUARD_PORT: String(NOSTUB_GUARD_PORT) }, { allowStub: false });
    const since = sinceOn(client, sc);
    const polled = await since(at, (d) => isStatus(d) && !!parseStatus(d).lastPollAt, 6000);
    const p = polled && parseStatus(polled);
    // Read, the fixture would set the event state and push its results.
    const read = p?.auto?.eventState != null || client.events.slice(at).some((d) => isUpdate(d) && d.tournament?.name === 'Stub Invitational');
    // The platform label follows the gate: a refused URL is never labelled "stub".
    check('without ZIPPER_ALLOW_STUB: a stub: URL inside the repo from config.json (autoStart) is refused at the poll with the variable\'s own message, nothing is read, and no platform is shown',
      p?.lastError === MSG.stubOff && p?.url === SAFE_STUB && p?.platform === null && !read,
      JSON.stringify({ lastError: p?.lastError, url: p?.url, platform: p?.platform, eventState: p?.auto?.eventState, read }));
    stubOffSeen = p?.lastError ?? null;
    const at2 = client.events.length;
    const other = 'stub:./platforms/fixtures/stub-result.json';
    client.ws.send(JSON.stringify({ request: 'DoAction', id: 'n', action: { name: 'Ticker Command' }, args: { command: 'setUrl', value: other } }));
    const r = await since(at2, (d) => isStatus(d) && (!!parseStatus(d).commandNotice || parseStatus(d).url === other), 4000);
    const rs = r && parseStatus(r);
    check('without ZIPPER_ALLOW_STUB: setUrl with a stub: URL inside the repo is refused with the same message in commandNotice; the URL is kept and no platform is shown',
      rs?.commandNotice === MSG.stubOff && rs?.url === SAFE_STUB && rs?.platform === null,
      JSON.stringify({ commandNotice: rs?.commandNotice, url: rs?.url, platform: rs?.platform }));
    await sc.stop();
    await rm(cfgPath, { force: true });
  }

  // With the variable: a saved stub: URL outside the repo, and a saved logo that fails
  // the rule. The path names a file that does not exist, in the folder above the repo.
  {
    const cfgPath = join(TMP_DIR, 'config-outside.json');
    await writeFile(cfgPath, JSON.stringify({
      tournamentUrl: 'stub:..\\zipper-verify-outside.json', autoStart: true, autoSchedule: true, pollIntervalMs: 15000,
      caps: { text: '@Saved', logo: 'javascript:alert(1)' },
    }), 'utf-8');
    const at = client.events.length;
    const sc = startSidecar('outside-stub sidecar [6g]', { ZIPPER_CONFIG: cfgPath, ZIPPER_GUARD_PORT: String(GUARD_PORT) });
    const since = sinceOn(client, sc);
    const polled = await since(at, (d) => isStatus(d) && !!parseStatus(d).lastPollAt, 6000);
    const p = polled && parseStatus(polled);
    check('a stub: URL outside the repo from config.json (autoStart) is refused at the poll with the confinement message, never ENOENT',
      p?.lastError === MSG.stubOutside, JSON.stringify({ lastError: p?.lastError }));
    // As the sidecar reports them, not as this file spells them: the two refusals differ.
    check('the two stub: refusals at the poll read differently (without the variable; outside the repo)',
      !!stubOffSeen && !!p?.lastError && stubOffSeen !== p.lastError, JSON.stringify({ withoutVariable: stubOffSeen, outside: p?.lastError }));
    const dropped = (sc.out.match(/the saved logo was dropped/g) || []).length;
    client.ws.send(JSON.stringify({ request: 'DoAction', id: 'o', action: { name: 'Ticker Command' }, args: { command: 'setAutoSchedule', value: 'off' } }));
    const saved = await savedConfig(cfgPath, (c) => c.autoSchedule === false);
    check('a saved logo that fails the rule is dropped to "" in memory with one log line, and written out on the next save; the text stays',
      p?.caps?.logo === '' && p?.caps?.text === '@Saved' && dropped === 1 && saved?.autoSchedule === false && saved?.caps?.logo === '' && saved?.caps?.text === '@Saved',
      JSON.stringify({ statusCaps: p?.caps, logLines: dropped, savedCaps: saved?.caps }));
    await sc.stop();
    await rm(cfgPath, { force: true });
  }
}

// ── 6h. The poll loop (ZIP-02, ZIP-06, ZIP-13) ──────────────────────────────
// Its own sidecar, with the poll's deadline shortened to 4 s and the minimum gap
// between polls to 1.5 s by their overrides. The slow source is the stub hold, and a
// stub fixture's stubFail makes a poll fail as a rate-limited site would. A fixture's
// stubHoldIgnoresAbort keeps its hold going when setUrl aborts the poll, so the poll's
// result or failure comes back after setUrl has moved on (a late result). A site's
// Retry-After holds its platform back whatever URL is set, for as long as the sidecar
// runs, so the checks that need a fresh one start the sidecar again (the hold lives in
// memory only). The last one runs with a preload that sets its wall clock back.

const POLL_LOOP_DEADLINE_MS = 4000;
const POLL_LOOP_GAP_MS = 1500;
// How long a command's own status may take, where a check must tell it apart from the
// status that closes a poll.
const REPLY_MS = 1000;
// How far the clock-step sidecar's wall clock is set (back an hour).
const CLOCK_STEP_MS = -3600000;

async function testPollLoop(client) {
  console.log('\n[6h] the poll loop: a URL changed mid-poll, a stale failure, a late result, the deadline, the minimum gap, re-timing, the backoff, Retry-After, a clock set back');
  const statusOf = (d) => (isStatus(d) ? parseStatus(d) : null);
  const T = (name) => `stub:${TMP_REL}/${name}`;
  const fixture = (name, body) => writeFile(join(TMP_DIR, name), typeof body === 'string' ? body : JSON.stringify(body), 'utf-8');
  const holdFor = (name) => writeFile(join(TMP_DIR, name), 'hold', 'utf-8');
  const release = (name) => rm(join(TMP_DIR, name), { force: true });
  const due = (s) => (s && s.nextPollAt && s.lastPollAt ? Date.parse(s.nextPollAt) - Date.parse(s.lastPollAt) : NaN);
  const cfgPath = join(TMP_DIR, 'config-poll-loop.json');
  const stepFile = join(TMP_DIR, 'clock-step');
  // The first poll after a start fails (Round.one is recognised but not implemented, so
  // its adapter throws without any network call): the status must still name the platform.
  const roundOne = 'https://round.one/tournament/zipper-verify';
  await writeFile(cfgPath, JSON.stringify({ tournamentUrl: roundOne, autoStart: true, autoSchedule: true, pollIntervalMs: 15000, caps: { text: '', logo: '' } }), 'utf-8');
  const env = {
    ZIPPER_CONFIG: cfgPath, ZIPPER_GUARD_PORT: String(GUARD_PORT),
    ZIPPER_POLL_DEADLINE_MS: String(POLL_LOOP_DEADLINE_MS), ZIPPER_MIN_POLL_GAP_MS: String(POLL_LOOP_GAP_MS),
  };
  const at0 = client.events.length;
  let runs = 1;
  let sc = startSidecar('poll-loop sidecar [6h]', env);
  let since = sinceOn(client, sc);
  // Stops this sidecar and starts another on the same config file, so no Retry-After hold
  // is left. Polling was stopped and saved as off, so the new one waits to be told.
  // Resolves with the status it sends once it has connected.
  const restart = async (label = `poll-loop sidecar [6h] #${++runs}`, extraEnv = {}, opts = {}) => {
    await sc.stop();
    const at = client.events.length;
    sc = startSidecar(label, { ...env, ...extraEnv }, opts);
    since = sinceOn(client, sc);
    return statusOf(await since(at, isStatus, 8000));
  };
  const mark = () => client.events.length;
  const cmd = (command, value) => client.ws.send(JSON.stringify({
    request: 'DoAction', id: 'p', action: { name: 'Ticker Command' }, args: { command, value: value ?? '' },
  }));
  // The status that closes the next poll: one with a lastPollAt other than `before`.
  const closed = (at, before, pred = () => true, timeoutMs = 6000) =>
    since(at, (d) => !!statusOf(d)?.lastPollAt && statusOf(d).lastPollAt !== before && pred(statusOf(d)), timeoutMs).then(statusOf);
  // How many polls closed among the statuses from `at` on, besides the one at `before`.
  const pollsSince = (at, before) => new Set(client.events.slice(at).filter(isStatus).map(parseStatus).map((s) => s.lastPollAt).filter((t) => t && t !== before)).size;
  const stopNow = async () => { const at = mark(); cmd('stop'); return statusOf(await since(at, (d) => statusOf(d)?.polling === false)); };
  const holdLogged = (name, outAt) => waitFor(() => sc.out.slice(outAt).includes(`stub: holding the poll while ${TMP_REL}/${name} exists`), 6000);

  try {
    {
      const first = statusOf(await since(at0, (d) => !!statusOf(d)?.lastPollAt, 6000));
      check('a first poll after a start that fails still shows its platform in the status (platformName is set on a failure too)',
        first?.platform === 'roundone' && /Round\.one/.test(first?.lastError || ''), JSON.stringify({ platform: first?.platform, lastError: first?.lastError }));
      await stopNow();
    }

    // ZIP-02: setUrl to B while a poll of A (completed) is in flight.
    {
      await fixture('old.json', { tournamentName: 'Old Cup', state: 'completed', stubHoldFile: `${TMP_REL}/hold-old`,
        matches: [{ id: 'o1', round: 'Grand Final', state: 'completed', p1: { name: 'Flash', score: 3 }, p2: { name: 'Rival', score: 0 }, winner: 1, order: 1 }],
        standings: [{ placement: 1, name: 'Flash' }, { placement: 2, name: 'Rival' }] });
      await fixture('new.json', { tournamentName: 'New Cup', state: 'in_progress', standings: [],
        matches: [{ id: 'n1', round: 'Round 1', state: 'in_progress', p1: { name: 'Ann', score: 1 }, p2: { name: 'Bo', score: 0 }, winner: 0, order: 1 },
          { id: 'n2', round: 'Round 1', state: 'upcoming', p1: { name: 'Cy', score: null }, p2: { name: 'Di', score: null }, winner: 0, order: 2 }] });
      await holdFor('hold-old');
      let at = mark();
      cmd('setUrl', T('old.json'));
      await since(at, (d) => statusOf(d)?.url === T('old.json'));
      const outAt = sc.out.length;
      cmd('start');
      const holding = await holdLogged('hold-old', outAt);
      at = mark();
      const tSet = Date.now();
      cmd('setUrl', T('new.json'));
      // The old hold is not released until B has been seen: setUrl itself must stop the
      // poll in flight (it obeys the poll's signal), or B waits for A's deadline.
      const upd = await since(at, (d) => isUpdate(d) && d.tournament?.url === T('new.json') && d.tournament?.name === 'New Cup', 9000);
      const tUpd = Date.now();
      await release('hold-old'); // the old poll may go on now, as it would on its own
      await sleep(300);
      const oldPushed = client.events.slice(at).filter((d) => isUpdate(d) && d.tournament?.name === 'Old Cup').length;
      const last = client.events.slice(at).filter(isStatus).map(parseStatus).pop();
      check('ZIP-02: setUrl to B during a held poll of a completed A ends with B polled and B\'s results under B\'s URL and platform; A\'s result is never pushed, and A\'s "completed" does not stop polling',
        !!holding && upd?.matches?.length === 2 && upd.tournament.platform === 'stub' && oldPushed === 0
          && last?.polling === true && last?.auto?.stopped === null && last?.url === T('new.json'),
        JSON.stringify({ holding: !!holding, newPushed: !!upd, oldPushed, polling: last?.polling, stopped: last?.auto?.stopped, url: last?.url }));
      // Without the abort, B would come after A's deadline and then the gap: about 5.5 s.
      check(`setUrl stops the poll in flight at once: B is pushed within the gap (${POLL_LOOP_GAP_MS / 1000} s) and 2 s of the setUrl, with A's hold never released, not after A's ${POLL_LOOP_DEADLINE_MS / 1000} s deadline`,
        !!upd && tUpd - tSet < POLL_LOOP_GAP_MS + 2000, JSON.stringify({ pushedAfterMs: upd ? tUpd - tSet : null }));
    }

    // setUrl clears waitingUntil: a new URL is not left sleeping for the old event's start.
    {
      await stopNow();
      await fixture('later2.json', { tournamentName: 'Later Cup', state: 'upcoming', startAt: new Date(Date.now() + 600000).toISOString(), matches: [], standings: [] });
      let at = mark();
      cmd('setUrl', T('later2.json'));
      await since(at, (d) => statusOf(d)?.url === T('later2.json'));
      at = mark();
      cmd('start');
      const waiting = statusOf(await since(at, (d) => !!statusOf(d)?.auto?.waitingUntil));
      at = mark();
      cmd('setUrl', T('new.json'));
      // setUrl's own reply: the first status with the new URL, not a later one from a poll.
      await since(at, (d) => statusOf(d)?.url === T('new.json'));
      const reply = statusOf(client.events.slice(at).find((d) => statusOf(d)?.url === T('new.json')));
      check('setUrl clears waitingUntil: its own status no longer shows the old event\'s start-time wait',
        !!waiting && reply?.auto?.waitingUntil === null, JSON.stringify({ waited: waiting?.auto?.waitingUntil ?? null, after: reply?.auto?.waitingUntil }));
      await stopNow();
    }

    // A stale failure: A has failed twice (so it is on a backoff), a slow poll of A is in
    // flight, and the URL changes to B, which fails on its own.
    {
      await fixture('fail-a.json', 'not JSON');
      let at = mark();
      cmd('setUrl', T('fail-a.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('fail-a.json')));
      at = mark();
      cmd('start');
      const f1 = await closed(at, set?.lastPollAt, (s) => s.lastError === MSG.stubNotObject);
      at = mark();
      cmd('pollNow');
      const f2 = await closed(at, f1?.lastPollAt, (s) => s.lastError === MSG.stubNotObject);
      // Two failures in a row: the backoff is four times the interval. setInterval goes
      // through the one next-delay function and keeps it (ZIP-13: today it cut it short).
      at = mark();
      cmd('setInterval', '15000');
      const iv = statusOf(await since(at, isStatus, 3000));
      check('ZIP-13: setInterval does not cut a longer error backoff short: after two failures in a row the next poll stays four intervals after the last (nextPollAt unchanged)',
        !!f2 && Math.abs(due(f2) - 60000) < 1500 && iv?.nextPollAt === f2.nextPollAt, JSON.stringify({ backoffMs: due(f2), before: f2?.nextPollAt, after: iv?.nextPollAt }));
      // The control page changes the interval only through setOptions, which carries all
      // three values on every Apply. With maxItems and topN unchanged it is a reschedule,
      // through the same function: four of the new intervals, not the gap.
      at = mark();
      cmd('setOptions', JSON.stringify({ intervalMs: 20000, maxItems: 20, topN: 0 }));
      const op = statusOf(await since(at, (d) => statusOf(d)?.intervalMs === 20000, REPLY_MS));
      check('ZIP-13: setOptions that changes only the interval (the control page\'s Apply, all three values sent) does not cut the backoff short either: the next poll is four of the new intervals after the last, not the gap',
        !!f2 && Math.abs(due(op) - 80000) < 1500 && op?.lastPollAt === f2.lastPollAt, JSON.stringify({ inMs: due(op), lastPollAt: op?.lastPollAt }));
      at = mark();
      cmd('setOptions', JSON.stringify({ intervalMs: 15000, maxItems: 20, topN: 0 }));
      await since(at, (d) => statusOf(d)?.intervalMs === 15000, REPLY_MS);
      await fixture('fail-a.json', { tournamentName: 'Slow A', state: 'in_progress', stubHoldFile: `${TMP_REL}/hold-a`, matches: [], standings: [] });
      await holdFor('hold-a');
      const outAt = sc.out.length;
      cmd('pollNow');
      const holding = await holdLogged('hold-a', outAt);
      at = mark();
      const tSet = Date.now();
      cmd('setUrl', T('missing-b.json'));
      await sleep(300);
      await release('hold-a');
      const b = await closed(at, f2?.lastPollAt, (s) => s.url === T('missing-b.json') && s.lastError === MSG.stubUnreadable, 8000);
      const errorsSeen = [...new Set(client.events.slice(at).filter(isStatus).map(parseStatus).map((s) => s.lastError))];
      check('a stale failure: after setUrl to B, no status carries an error from A\'s poll (A\'s old error, or the one its poll in flight ended with), only B\'s own',
        !!f2 && !!holding && !!b && errorsSeen.every((e) => e === '' || e === MSG.stubUnreadable),
        JSON.stringify({ failedTwice: !!f2, holding: !!holding, bPolled: !!b, errorsSeen }));
      check('a stale failure: B is polled once the gap allows, without A\'s backoff, and B\'s own backoff starts from one error (twice the interval)',
        !!b && Date.parse(b.lastPollAt) - tSet < POLL_LOOP_GAP_MS + 3000 && Math.abs(due(b) - 30000) < 1500,
        JSON.stringify({ bPolledAfterMs: b ? Date.parse(b.lastPollAt) - tSet : null, nextPollInMs: due(b) }));
      await stopNow();
    }

    // ZIP-02, a late result. A's hold ignores the abort setUrl sends (stubHoldIgnoresAbort),
    // so A's poll is still under way when B is set and its result comes back afterwards,
    // as an answer already on its way would. Only the URL check in pollOnce stops it then.
    // A is a completed event, so a result taken as B's would also stop polling.
    const lateLog = 'a poll of the previous tournament URL came back after setUrl; its result is thrown away';
    const lateFailLog = 'a poll of the previous tournament URL failed after setUrl; its error is thrown away';
    // Asked after setUrl to `url`: is A's poll still under way? Then no poll is armed, nothing
    // has been pushed since `at`, and no poll has ended after the setUrl (the sidecar logs
    // that, from `outAt` on). Had the abort ended A's poll, B would be armed by now for the
    // end of the gap.
    const stillUnderWay = async (url, at, outAt) => {
      const atProbe = mark();
      cmd('status');
      const probe = statusOf(await since(atProbe, (d) => statusOf(d)?.url === url, REPLY_MS));
      const ok = !!probe && probe.polling === true && probe.nextPollAt === null
        && !client.events.slice(at).some((d) => isUpdate(d)) && !sc.out.slice(outAt).includes('after setUrl');
      return { ok, nextPollAt: probe ? probe.nextPollAt : 'no status' };
    };
    {
      await fixture('late-old.json', { tournamentName: 'Late Old Cup', state: 'completed', stubHoldFile: `${TMP_REL}/hold-late`, stubHoldIgnoresAbort: true,
        matches: [{ id: 'lo1', round: 'Grand Final', state: 'completed', p1: { name: 'Flash', score: 3 }, p2: { name: 'Rival', score: 1 }, winner: 1, order: 1 }],
        standings: [{ placement: 1, name: 'Flash' }, { placement: 2, name: 'Rival' }] });
      await holdFor('hold-late');
      let at = mark();
      cmd('setUrl', T('late-old.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('late-old.json')));
      const outAt = sc.out.length;
      cmd('start');
      const holding = await holdLogged('hold-late', outAt);
      at = mark();
      cmd('setUrl', T('new.json'));
      await since(at, (d) => statusOf(d)?.url === T('new.json'), REPLY_MS);
      await sleep(300);
      const underWay = await stillUnderWay(T('new.json'), at, outAt);
      const tRel = Date.now();
      await release('hold-late');
      const lateLogged = await waitFor(() => sc.out.slice(outAt).includes(lateLog), 3000);
      const upd = await since(at, (d) => isUpdate(d) && d.tournament?.url === T('new.json') && d.tournament?.name === 'New Cup', POLL_LOOP_GAP_MS + 4000);
      const tUpd = Date.now();
      const good = await closed(at, set?.lastPollAt, (s) => s.url === T('new.json') && s.auto?.eventState === 'in_progress', 3000);
      await sleep(300);
      const evs = client.events.slice(at);
      const statuses = evs.filter(isStatus).map(parseStatus);
      const oldPushed = evs.filter((d) => isUpdate(d) && (d.tournament?.name === 'Late Old Cup' || d.tournament?.url === T('late-old.json'))).length;
      const otherUnderB = evs.filter((d) => isUpdate(d) && d.tournament?.url === T('new.json') && d.tournament?.name !== 'New Cup').length;
      // Under B's URL a status may carry only B's own poll: before it, the lastPollAt from
      // before A's poll and no event state; never A's "completed".
      const showsA = statuses.filter((s) => s.auto?.eventState === 'completed' || s.auto?.stopped === 'completed'
        || (s.lastPollAt !== set?.lastPollAt && s.auto?.eventState !== 'in_progress')).length;
      const stopped = statuses.filter((s) => s.polling !== true).length;
      check('ZIP-02, a late result: with stubHoldIgnoresAbort the poll of A is still under way after setUrl to B (no poll armed, B not polled, nothing pushed), and once released its result comes back after the setUrl and is thrown away (logged)',
        !!holding && underWay.ok && !!lateLogged,
        JSON.stringify({ holding: !!holding, underWay, lateLogged: !!lateLogged }));
      check('ZIP-02, a late result of a completed A: it is not pushed, not shown under B\'s URL (no update or status there carries A\'s tournament, poll time or state), and does not stop polling',
        !!lateLogged && oldPushed === 0 && otherUnderB === 0 && showsA === 0 && stopped === 0 && statuses.length > 0,
        JSON.stringify({ oldPushed, otherUnderB, statusesShowingA: showsA, statusesNotPolling: stopped, statuses: statuses.length }));
      check('ZIP-02, a late result: B is then polled once the gap after A\'s late end allows, and pushed under B\'s URL and platform with B\'s results',
        underWay.ok && !!upd && upd.matches?.length === 2 && upd.tournament.platform === 'stub' && tUpd - tRel < POLL_LOOP_GAP_MS + 2000
          && !!good && good.lastError === '' && good.platform === 'stub' && good.polling === true,
        JSON.stringify({ pushed: !!upd, pushedAfterReleaseMs: upd ? tUpd - tRel : null, matches: upd?.matches?.length ?? null, closedWith: good && { lastError: good.lastError, platform: good.platform } }));
      await stopNow();
    }

    // ZIP-02, a late failure: A's poll fails after setUrl to B (HTTP 500, no Retry-After).
    // B fails on its own too, so its backoff shows the error count it started from.
    {
      await fixture('late-fail.json', { tournamentName: 'Late Fail', state: 'in_progress', matches: [], standings: [],
        stubHoldFile: `${TMP_REL}/hold-late-f`, stubHoldIgnoresAbort: true, stubFail: { status: 500 } });
      await holdFor('hold-late-f');
      let at = mark();
      cmd('setUrl', T('late-fail.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('late-fail.json')));
      const outAt = sc.out.length;
      cmd('start');
      const holding = await holdLogged('hold-late-f', outAt);
      at = mark();
      cmd('setUrl', T('missing-late.json'));
      await since(at, (d) => statusOf(d)?.url === T('missing-late.json'), REPLY_MS);
      await sleep(300);
      const underWay = await stillUnderWay(T('missing-late.json'), at, outAt);
      await release('hold-late-f');
      const lateLogged = await waitFor(() => sc.out.slice(outAt).includes(lateFailLog), 3000);
      const b = await closed(at, set?.lastPollAt, (s) => s.url === T('missing-late.json') && s.lastError === MSG.stubUnreadable, POLL_LOOP_GAP_MS + 4000);
      const errorsSeen = [...new Set(client.events.slice(at).filter(isStatus).map(parseStatus).map((s) => s.lastError))];
      check('ZIP-02, a late failure: a poll of A still under way after setUrl to B that then fails (HTTP 500) leaves lastError and the error count alone: no status shows A\'s error, and B\'s own failure starts its backoff from one error (twice the interval)',
        !!holding && underWay.ok && !!lateLogged && !!b && errorsSeen.every((e) => e === '' || e === MSG.stubUnreadable) && Math.abs(due(b) - 30000) < 1500,
        JSON.stringify({ holding: !!holding, underWay, lateLogged: !!lateLogged, bPolled: !!b, errorsSeen, bNextPollInMs: due(b) }));
      await stopNow();
    }

    // The bound: a hold that ignores the abort, never released, still ends at the poll's
    // deadline, so the poll loop goes on to B.
    {
      await fixture('late-stuck.json', { tournamentName: 'Stuck Cup', state: 'in_progress', matches: [], standings: [],
        stubHoldFile: `${TMP_REL}/hold-stuck`, stubHoldIgnoresAbort: true });
      await holdFor('hold-stuck');
      let at = mark();
      cmd('setUrl', T('late-stuck.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('late-stuck.json')));
      const outAt = sc.out.length;
      cmd('start');
      const holding = await holdLogged('hold-stuck', outAt);
      const tHold = Date.now();
      at = mark();
      cmd('setUrl', SAFE_STUB);
      const b = await closed(at, set?.lastPollAt, (s) => s.url === SAFE_STUB && s.lastError === '', POLL_LOOP_DEADLINE_MS + POLL_LOOP_GAP_MS + 5000);
      const tB = Date.now();
      const endedLate = sc.out.slice(outAt).includes(lateFailLog);
      const errorsSeen = [...new Set(client.events.slice(at).filter(isStatus).map(parseStatus).map((s) => s.lastError))];
      check(`stubHoldIgnoresAbort is bounded: a hold that ignores the abort and is never released still ends at the poll's deadline (${POLL_LOOP_DEADLINE_MS / 1000} s here), thrown away as a late failure, and B is polled after it and the gap`,
        !!holding && !!b && endedLate && tB - tHold >= POLL_LOOP_DEADLINE_MS - 500 && tB - tHold < POLL_LOOP_DEADLINE_MS + POLL_LOOP_GAP_MS + 2500
          && errorsSeen.every((e) => e === ''),
        JSON.stringify({ holding: !!holding, bPolled: !!b, bPolledAfterHoldMs: b ? tB - tHold : null, endedLate, errorsSeen }));
      await stopNow();
      await release('hold-stuck');
    }

    // A stalled poll: the hold never ends by itself. The poll's deadline aborts it, and a
    // pollNow sent meanwhile is kept and runs afterwards.
    {
      await fixture('stalled.json', { tournamentName: 'Stalled Cup', state: 'in_progress', stubHoldFile: `${TMP_REL}/hold-s`, standings: [],
        matches: [{ id: 's1', round: 'Round 1', state: 'in_progress', p1: { name: 'Ann', score: 0 }, p2: { name: 'Bo', score: 0 }, winner: 0, order: 1 }] });
      await holdFor('hold-s');
      let at = mark();
      cmd('setUrl', T('stalled.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('stalled.json')));
      const outAt = sc.out.length;
      cmd('start');
      const holding = await holdLogged('hold-s', outAt);
      const tHold = Date.now();
      at = mark();
      cmd('pollNow');
      // pollNow's own status: sent while the held poll runs, so it still carries the
      // lastPollAt from before that poll, with no poll armed (nextPollAt null). The status
      // that closes the held poll has a new lastPollAt, so it cannot stand in for it.
      const replied = statusOf(await since(at, (d) => !!set?.lastPollAt && statusOf(d)?.lastPollAt === set.lastPollAt && statusOf(d)?.nextPollAt === null, REPLY_MS));
      const deadline = await closed(at, set?.lastPollAt, (s) => s.lastError === `the poll took longer than ${POLL_LOOP_DEADLINE_MS / 1000} s and was stopped`, 8000);
      const tDeadline = Date.now();
      await release('hold-s'); // so the poll that pollNow asked for can finish
      const upd = await since(at, (d) => isUpdate(d) && d.tournament?.name === 'Stalled Cup', 6000);
      const good = await closed(at, deadline?.lastPollAt ?? set?.lastPollAt, (s) => s.lastError === '', 6000);
      check(`the poll's deadline (shortened to ${POLL_LOOP_DEADLINE_MS / 1000} s) aborts a held stub delay: the poll fails with "the poll took longer than ${POLL_LOOP_DEADLINE_MS / 1000} s and was stopped" while the hold file still exists`,
        !!holding && !!deadline && tDeadline - tHold < POLL_LOOP_DEADLINE_MS + 2000, JSON.stringify({ holding: !!holding, deadline: deadline?.lastError ?? null, afterMs: tDeadline - tHold }));
      check(`a pollNow sent while a poll is held replies at once with its own status (within ${REPLY_MS / 1000} s, before the held poll ends, nextPollAt null), and is kept: a second poll runs after the held one ends, with no further command`,
        !!replied && !!upd && !!good && pollsSince(at, set?.lastPollAt) >= 2,
        JSON.stringify({ replied: !!replied, pushed: !!upd, polls: pollsSince(at, set?.lastPollAt) }));
      await stopNow();
    }

    // The minimum gap: a burst of one command, sent inside the gap after a poll, costs one
    // fetch, and every command still replies at once.
    {
      let at = mark();
      cmd('setUrl', SAFE_STUB);
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === SAFE_STUB));
      at = mark();
      cmd('start');
      let last = await closed(at, set?.lastPollAt);
      const other = 'stub:./platforms/fixtures/stub-result.json';
      const bursts = [
        ['pollNow', () => cmd('pollNow')],
        ['setUrl', (i) => cmd('setUrl', i % 2 ? SAFE_STUB : other)],
        ['start', () => cmd('start')],
      ];
      for (const [name, send] of bursts) {
        // Each burst starts right after a poll has closed, so all five fall inside the
        // gap that follows it. start is sent with polling off; the gap still runs from
        // the last poll.
        at = mark();
        cmd('pollNow');
        last = await closed(at, last?.lastPollAt);
        if (name === 'start') await stopNow();
        at = mark();
        for (let i = 0; i < 5; i++) { send(i); await sleep(40); }
        // The commands' own statuses: those sent before the burst's poll closed still carry
        // the lastPollAt from before the burst. The status that closes it is not counted.
        const own = () => client.events.slice(at).filter(isStatus).map(parseStatus).filter((s) => s.lastPollAt === last?.lastPollAt);
        const replies = await waitFor(() => own().length >= 5, 3000);
        const polled = await closed(at, last?.lastPollAt, () => true, POLL_LOOP_GAP_MS + 4000);
        await sleep(POLL_LOOP_GAP_MS + 800);
        const n = pollsSince(at, last?.lastPollAt);
        // A reply sent while the poll ran (nextPollAt null) means a command reached the
        // sidecar after the gap had ended: this run's timing, which a second poll would
        // then follow from, so the detail says so.
        const late = own().filter((s) => s.nextPollAt === null).length;
        check(`the gap: a burst of five ${name} commands 40 ms apart, inside the gap after a poll, costs one fetch, and each command replies at once with its own status`,
          !!replies && !!polled && n === 1 && late === 0, JSON.stringify({ ownReplies: own().length, lateReplies: late, polls: n }));
        last = polled || last;
      }
      await stopNow();
    }

    // ZIP-13: setInterval and setAutoSchedule off re-time the next poll through the one
    // next-delay function, counted from the end of the last poll. nextPollAt is null while
    // polling is off.
    {
      let at = mark();
      cmd('setUrl', SAFE_STUB);
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === SAFE_STUB));
      at = mark();
      cmd('start');
      const s = await closed(at, set?.lastPollAt, (x) => x.lastError === '', POLL_LOOP_GAP_MS + 4000);
      at = mark();
      cmd('setInterval', '60000');
      const longer = statusOf(await since(at, (d) => statusOf(d)?.intervalMs === 60000, REPLY_MS));
      at = mark();
      cmd('setInterval', '15000');
      const shorter = statusOf(await since(at, (d) => statusOf(d)?.intervalMs === 15000, REPLY_MS));
      check('ZIP-13: setInterval re-times the next poll from the end of the last one: one interval after a good poll, 60 s after it with setInterval 60000, and 15 s again with 15000',
        !!s && Math.abs(due(s) - 15000) < 1500 && Math.abs(due(longer) - 60000) < 1500 && Math.abs(due(shorter) - 15000) < 1500
          && longer?.lastPollAt === s.lastPollAt && shorter?.lastPollAt === s.lastPollAt,
        JSON.stringify({ afterPoll: due(s), after60000: due(longer), after15000: due(shorter) }));
      const off = await stopNow();
      check('nextPollAt: with polling off it is null', off?.nextPollAt === null, JSON.stringify({ whenOff: off?.nextPollAt }));

      await fixture('later3.json', { tournamentName: 'Later Cup', state: 'upcoming', startAt: new Date(Date.now() + 600000).toISOString(), matches: [], standings: [] });
      at = mark();
      cmd('setUrl', T('later3.json'));
      const set2 = statusOf(await since(at, (d) => statusOf(d)?.url === T('later3.json')));
      at = mark();
      cmd('start');
      const w = await closed(at, set2?.lastPollAt, (x) => !!x.auto?.waitingUntil, POLL_LOOP_GAP_MS + 4000);
      at = mark();
      cmd('setAutoSchedule', 'off');
      const manual = statusOf(await since(at, (d) => statusOf(d)?.auto?.enabled === false, REPLY_MS));
      check('setAutoSchedule off ends a wait for the event\'s start at once: its own status shows no wait, and the next poll one interval after the last, not at the start time 10 minutes away',
        !!w && Math.abs(due(w) - 600000) < 4000 && manual?.auto?.waitingUntil === null && Math.abs(due(manual) - 15000) < 1500 && manual?.lastPollAt === w.lastPollAt,
        JSON.stringify({ waitBeforeMs: due(w), waitingUntil: manual?.auto?.waitingUntil, nextAfterMs: due(manual) }));
      at = mark();
      cmd('setAutoSchedule', 'on');
      await since(at, (d) => statusOf(d)?.auto?.enabled === true, REPLY_MS);
      await stopNow();
    }

    // ZIP-13: the backoff never goes below the interval, and is not doubled past it.
    {
      let at = mark();
      cmd('setInterval', '3600000');
      await since(at, (d) => statusOf(d)?.intervalMs === 3600000);
      at = mark();
      cmd('setUrl', T('missing-backoff.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('missing-backoff.json')));
      at = mark();
      cmd('start');
      const s = await closed(at, set?.lastPollAt, (x) => x.lastError === MSG.stubUnreadable);
      check('ZIP-13: with a one-hour interval, a failed poll waits one hour (read from nextPollAt): not the five-minute backoff cap, and not the doubled two hours',
        !!s && due(s) >= 3600000 - 1000 && due(s) <= 3600000 + 1500, JSON.stringify({ lastPollAt: s?.lastPollAt, nextPollAt: s?.nextPollAt, inMs: due(s) }));
      await stopNow();
      at = mark();
      cmd('setInterval', '15000');
      await since(at, (d) => statusOf(d)?.intervalMs === 15000);
    }

    // Retry-After, through a stub fixture that answers like a rate-limited site.
    const ra = (name, retryAfter) => fixture(name, { tournamentName: 'Busy Cup', state: 'in_progress', matches: [], standings: [], stubFail: { status: 429, retryAfter } });
    {
      await ra('ra-s.json', '120');
      await ra('ra-s2.json', '120');
      let at = mark();
      cmd('setUrl', T('ra-s.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('ra-s.json')));
      at = mark();
      cmd('start');
      const s = await closed(at, set?.lastPollAt, (x) => x.lastError === MSG.stub429, POLL_LOOP_GAP_MS + 4000);
      check('Retry-After: a 429 with Retry-After 120 (seconds) puts the next poll 120 s after the failure (more than the interval and the backoff), in nextPollAt',
        !!s && Math.abs(due(s) - 120000) < 1500, JSON.stringify({ lastError: s?.lastError, inMs: due(s) }));
      // setInterval reschedules through the one next-delay function, which keeps the wait
      // (today's code armed the new interval from now, cutting a longer wait short).
      at = mark();
      cmd('setInterval', '15000');
      const iv = statusOf(await since(at, isStatus, 3000));
      check('Retry-After: setInterval, rescheduled through the one next-delay function, does not cut the wait short (nextPollAt unchanged)',
        !!s?.nextPollAt && iv?.nextPollAt === s.nextPollAt, JSON.stringify({ before: s?.nextPollAt, after: iv?.nextPollAt }));
      // No poll can close during the wait, so every status here is a command's own reply.
      at = mark();
      cmd('pollNow');
      const p = statusOf(await since(at, isStatus, REPLY_MS));
      check('Retry-After: pollNow replies at once and does not cut the wait short (nextPollAt unchanged)',
        !!s?.nextPollAt && p?.nextPollAt === s.nextPollAt, JSON.stringify({ before: s?.nextPollAt, afterPollNow: p?.nextPollAt }));
      const atOpts = mark();
      const optionWrong = [];
      for (const [command, value, pred] of [
        ['setTopN', '1', (x) => x.topN === 1],
        ['setCapsText', 'held-a', (x) => x.caps?.text === 'held-a'],
        ['setCapsLogo', '/zipper-overlay/held.png', (x) => x.caps?.logo === '/zipper-overlay/held.png'],
        ['setCaps', JSON.stringify({ text: 'held-b', logo: '' }), (x) => x.caps?.text === 'held-b' && x.caps?.logo === ''],
        ['setMaxItems', '19', (x) => x.maxItems === 19],
        ['setOptions', JSON.stringify({ intervalMs: 15000, maxItems: 20, topN: 0 }), (x) => x.maxItems === 20 && x.topN === 0],
      ]) {
        const atC = mark();
        cmd(command, value);
        const r = statusOf(await since(atC, (d) => { const x = statusOf(d); return !!x && pred(x); }, REPLY_MS));
        if (!r) optionWrong.push(`${command}: no status within ${REPLY_MS / 1000} s`);
        else if (r.nextPollAt !== s?.nextPollAt) optionWrong.push(`${command}: nextPollAt ${r.nextPollAt}, was ${s?.nextPollAt}`);
      }
      await sleep(POLL_LOOP_GAP_MS + 800);
      const polledAnyway = pollsSince(atOpts, s?.lastPollAt);
      check(`Retry-After: each option command (setTopN, setCapsText, setCapsLogo, setCaps, setMaxItems, setOptions) replies with its own status within ${REPLY_MS / 1000} s, and none cuts the wait short (nextPollAt unchanged, no poll)`,
        !!s?.nextPollAt && optionWrong.length === 0 && polledAnyway === 0, JSON.stringify({ wrong: optionWrong, polls: polledAnyway }));
      at = mark();
      cmd('setUrl', T('ra-s2.json'));
      const same = statusOf(await since(at, (d) => statusOf(d)?.url === T('ra-s2.json')));
      check('Retry-After: setUrl to another URL on the same platform keeps the wait (nextPollAt unchanged)',
        !!s?.nextPollAt && same?.nextPollAt === s.nextPollAt, JSON.stringify({ before: s?.nextPollAt, after: same?.nextPollAt }));
      at = mark();
      const tOther = Date.now();
      cmd('setUrl', roundOne);
      const elsewhere = await closed(at, s?.lastPollAt, (x) => x.url === roundOne, POLL_LOOP_GAP_MS + 4000);
      check('Retry-After: the wait is its platform\'s: a URL on another platform is polled as usual, once the gap allows',
        !!elsewhere && Date.parse(elsewhere.lastPollAt) - tOther < POLL_LOOP_GAP_MS + 3000, JSON.stringify({ polledAfterMs: elsewhere ? Date.parse(elsewhere.lastPollAt) - tOther : null }));
      // Going back: the other platform's poll armed its own next poll, sooner than the
      // wait's end; the URL set back must not inherit it.
      at = mark();
      cmd('setUrl', T('ra-s2.json'));
      const back = statusOf(await since(at, (d) => statusOf(d)?.url === T('ra-s2.json'), REPLY_MS));
      check('Retry-After: setting a URL on the waiting platform again, after one on another platform, does not cut the wait short (nextPollAt is still its end)',
        !!s?.nextPollAt && back?.nextPollAt === s.nextPollAt, JSON.stringify({ before: s?.nextPollAt, after: back?.nextPollAt }));
      at = mark();
      cmd('setUrl', '');
      const cleared = statusOf(await since(at, (d) => statusOf(d)?.url === null && statusOf(d)?.polling === false, REPLY_MS));
      at = mark();
      cmd('setUrl', T('ra-s2.json'));
      await since(at, (d) => statusOf(d)?.url === T('ra-s2.json'), REPLY_MS);
      at = mark();
      cmd('start');
      const again = statusOf(await since(at, (d) => statusOf(d)?.polling === true, REPLY_MS));
      check('Retry-After: clearing the URL does not drop the wait either: set again and started, the waiting platform still waits for its end (nextPollAt unchanged)',
        !!s?.nextPollAt && !!cleared && again?.nextPollAt === s.nextPollAt, JSON.stringify({ cleared: !!cleared, before: s?.nextPollAt, after: again?.nextPollAt }));
      await stopNow();
    }

    // ZIP-02, a late 429: A's poll answers 429 with Retry-After 120 after setUrl to B on
    // another platform (stubHoldIgnoresAbort). Its error is thrown away like any late
    // failure, but the wait is the platform's, so it is kept for A's platform (stub) and
    // only for it. A fresh sidecar, so no wait is left from above.
    {
      await restart();
      await fixture('late-ra.json', { tournamentName: 'Busy Cup', state: 'in_progress', matches: [], standings: [],
        stubHoldFile: `${TMP_REL}/hold-late-ra`, stubHoldIgnoresAbort: true, stubFail: { status: 429, retryAfter: '120' } });
      await holdFor('hold-late-ra');
      let at = mark();
      cmd('setUrl', T('late-ra.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('late-ra.json')));
      const outAt = sc.out.length;
      cmd('start');
      const holding = await holdLogged('hold-late-ra', outAt);
      at = mark();
      cmd('setUrl', roundOne);
      await since(at, (d) => statusOf(d)?.url === roundOne, REPLY_MS);
      await sleep(300);
      const underWay = await stillUnderWay(roundOne, at, outAt);
      const tRel = Date.now();
      await release('hold-late-ra');
      const lateLogged = await waitFor(() => sc.out.slice(outAt).includes(lateFailLog), 3000);
      const other = await closed(at, set?.lastPollAt, (s) => s.url === roundOne, POLL_LOOP_GAP_MS + 4000);
      const tOther = Date.now();
      const errorsSeen = [...new Set(client.events.slice(at).filter(isStatus).map(parseStatus).map((s) => s.lastError))];
      check('ZIP-02, a late 429: a poll of A still under way after setUrl to B on another platform that then answers 429 does not hold B: B is polled as usual once the gap after A\'s late end allows, and no status shows A\'s error',
        !!holding && underWay.ok && !!lateLogged && !!other && tOther - tRel < POLL_LOOP_GAP_MS + 2500 && /Round\.one/.test(other.lastError || '')
          && errorsSeen.every((e) => e !== MSG.stub429),
        JSON.stringify({ holding: !!holding, underWay, lateLogged: !!lateLogged, bPolledAfterReleaseMs: other ? tOther - tRel : null, bError: other?.lastError ?? null, errorsSeen }));
      // Back to a URL on A's platform while polling: it waits for the end of A's 120 s.
      at = mark();
      cmd('setUrl', SAFE_STUB);
      const back = statusOf(await since(at, (d) => statusOf(d)?.url === SAFE_STUB, REPLY_MS));
      const want = tRel + 120000;
      check('ZIP-02, a late 429: its Retry-After is kept for A\'s platform although the poll was thrown away: a URL on that platform set afterwards waits for the end of the 120 s (nextPollAt)',
        underWay.ok && !!lateLogged && !!back && back.polling === true && Math.abs(Date.parse(back.nextPollAt) - want) < 1500,
        JSON.stringify({ underWay, lateLogged: !!lateLogged, nextPollAt: back?.nextPollAt ?? null, want: new Date(want).toISOString() }));
      await stopNow();
    }

    // As an HTTP date. A fresh sidecar, so no wait is left from above.
    {
      await restart();
      const raDate = new Date(Date.now() + 125000).toUTCString(); // an HTTP date counts whole seconds
      await ra('ra-d.json', raDate);
      let at = mark();
      cmd('setUrl', T('ra-d.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('ra-d.json')));
      at = mark();
      cmd('start');
      const d = await closed(at, set?.lastPollAt, (x) => x.url === T('ra-d.json') && x.lastError === MSG.stub429, POLL_LOOP_GAP_MS + 4000);
      check('Retry-After: as an HTTP date, the next poll is at that date',
        !!d && Math.abs(Date.parse(d.nextPollAt) - Date.parse(raDate)) < 1500, JSON.stringify({ nextPollAt: d?.nextPollAt, retryAfter: raDate }));
      await stopNow();
    }

    // The cap. A fresh sidecar again.
    {
      await restart();
      await ra('ra-cap.json', '86400');
      let at = mark();
      cmd('setUrl', T('ra-cap.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('ra-cap.json')));
      at = mark();
      cmd('start');
      const c = await closed(at, set?.lastPollAt, (x) => x.url === T('ra-cap.json') && x.lastError === MSG.stub429, POLL_LOOP_GAP_MS + 4000);
      check('Retry-After: a wait longer than the cap is cut to the cap, five minutes with a 15-second interval',
        !!c && Math.abs(due(c) - 300000) < 1500, JSON.stringify({ inMs: due(c) }));
      await stopNow();
    }

    // The cap is never below the interval. With a 10-minute interval the next regular poll
    // after a failure is 10 minutes away anyway, so the cap shows only on a poll a command
    // asks for: pollNow.
    {
      await restart();
      let at = mark();
      cmd('setInterval', '600000');
      await since(at, (d) => statusOf(d)?.intervalMs === 600000);
      await ra('ra-floor.json', '86400');
      at = mark();
      cmd('setUrl', T('ra-floor.json'));
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === T('ra-floor.json')));
      at = mark();
      cmd('start');
      const f = await closed(at, set?.lastPollAt, (x) => x.url === T('ra-floor.json') && x.lastError === MSG.stub429, POLL_LOOP_GAP_MS + 4000);
      at = mark();
      cmd('pollNow');
      const pn = statusOf(await since(at, isStatus, REPLY_MS));
      check('Retry-After: with a 10-minute interval the cap on the wait is the interval, not five minutes: after a 429 asking for a day, pollNow still waits the full 10 minutes (nextPollAt the same to within 5 ms)',
        !!f && Math.abs(due(f) - 600000) < 1500 && !!pn?.nextPollAt && Math.abs(Date.parse(pn.nextPollAt) - Date.parse(f.nextPollAt)) <= 5, JSON.stringify({ failedInMs: due(f), afterPollNowInMs: due(pn) }));
      await stopNow();
      at = mark();
      cmd('setInterval', '15000');
      await since(at, (d) => statusOf(d)?.intervalMs === 15000);
    }

    // A wall clock set back an hour after a poll. Polls are timed on the monotonic clock,
    // so neither a re-timed regular poll nor a command's poll waits out the step. This
    // sidecar runs with a preload (tools/clock-step-preload.cjs, development only) that
    // sets its wall clock back when the test writes the step file.
    {
      const first = await restart('clock-step sidecar [6h]', { ZIPPER_TEST_CLOCK_STEP_FILE: stepFile },
        { nodeArgs: ['--require', resolve(__dirname, 'tools', 'clock-step-preload.cjs')] });
      let at = mark();
      cmd('setUrl', SAFE_STUB);
      const set = statusOf(await since(at, (d) => statusOf(d)?.url === SAFE_STUB));
      at = mark();
      cmd('start');
      const g = await closed(at, set?.lastPollAt, (x) => x.lastError === '', POLL_LOOP_GAP_MS + 4000);
      const outAt = sc.out.length;
      await writeFile(stepFile, String(CLOCK_STEP_MS), 'utf-8');
      const stepped = await waitFor(() => sc.out.slice(outAt).includes('[clock-step] the wall clock was set by'), 3000);
      at = mark();
      cmd('setInterval', '20000');
      const iv = statusOf(await since(at, (d) => statusOf(d)?.intervalMs === 20000, REPLY_MS));
      // 20 s after the poll, read on the clock as it now is: an hour before the poll's own
      // (unstepped) lastPollAt, plus 20 s.
      const want = g ? Date.parse(g.lastPollAt) + CLOCK_STEP_MS + 20000 : NaN;
      check('a wall clock set back an hour after a poll: setInterval re-times the next poll 20 s after that poll, and nextPollAt shows it on the clock as it now is, not an hour later',
        !!first && !!g && !!stepped && Math.abs(Date.parse(iv?.nextPollAt) - want) < 1500,
        JSON.stringify({ stepped: !!stepped, nextPollAt: iv?.nextPollAt ?? null, want: Number.isFinite(want) ? new Date(want).toISOString() : null }));
      await sleep(POLL_LOOP_GAP_MS + 200); // past the gap after that poll, by the real clock
      at = mark();
      const tAsk = Date.now();
      cmd('pollNow');
      const p = await closed(at, g?.lastPollAt, () => true, 4000);
      const tPolled = Date.now();
      check('a wall clock set back an hour after a poll: pollNow, once the gap has passed, still polls at once, not an hour later',
        !!g && !!stepped && !!p && tPolled - tAsk < 2500, JSON.stringify({ polled: !!p, afterMs: tPolled - tAsk }));
      await stopNow();
    }
  } finally {
    for (const f of ['hold-old', 'hold-a', 'hold-s', 'hold-late', 'hold-late-f', 'hold-stuck', 'hold-late-ra']) await release(f);
    await rm(stepFile, { force: true });
    await sc.stop();
    await rm(cfgPath, { force: true });
  }
}

// ── 7. Milestone announcements (SE mock + real sidecar) ──────────────────────

let seServer = null;
async function testMilestones() {
  console.log('\n[7] milestones (real sidecar, mock StreamElements)');
  const { createServer } = await import('node:http');

  // Tiny StreamElements sessions mock with a mutable follower counter. It answers
  // only once the sidecar is connected to the mock bus, so an announcement from the
  // first poll could not be lost for want of a connection.
  let followers = 250;
  let seOpen = false;
  // The switch that makes it stop answering (row 3.2): a request that arrives while it
  // is on is accepted and never answered.
  let seStall = false;
  const seStalled = [];
  seServer = createServer(async (req, res) => {
    if (req.url?.startsWith('/kappa/v2/sessions/')) {
      if (seStall) { seStalled.push(res); return; }
      await waitFor(() => seOpen, 8000);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ data: { 'follower-total': { count: followers } } }));
    } else {
      res.writeHead(404); res.end();
    }
  });
  await new Promise((res, rej) => {
    seServer.once('error', (err) => rej(new Error(`the StreamElements mock could not listen on ${SE_PORT}: ${err.message}`)));
    seServer.listen(SE_PORT, '127.0.0.1', res);
  });

  // JWT whose payload carries the channel claim, like the real one.
  const jwt = `x.${Buffer.from(JSON.stringify({ channel: 'testchan' })).toString('base64url')}.x`;
  const cfgPath = join(TMP_DIR, 'config-milestones.json');
  await writeFile(cfgPath, JSON.stringify({
    tournamentUrl: null, autoStart: false,
    streamElements: { jwtToken: jwt, pollSeconds: 5 },
    milestones: { followers: 100 },
  }), 'utf-8');

  const client = await busClient();
  const before = (await stats()).announceCount;

  // The 10-second request limit is shortened to 2 s (its override) for the check below
  // with a StreamElements that never answers.
  const SE_LIMIT_MS = 2000;
  const sc = startSidecar('milestone test sidecar [7]', {
    ZIPPER_CONFIG: cfgPath,
    ZIPPER_GUARD_PORT: String(SE_GUARD_PORT),
    ZIPPER_SE_BASE: `http://127.0.0.1:${SE_PORT}`,
    ZIPPER_REQUEST_TIMEOUT_MS: String(SE_LIMIT_MS),
  });
  const since = sinceOn(client, sc);

  // Only this sidecar's status has se.configured (the section 6 sidecar has stopped).
  const seStatus = await since(0, (d) => isStatus(d) && parseStatus(d).se?.configured === true, 5000);
  seOpen = true;

  // First poll records the 200 threshold silently: wait until it is saved, then look.
  const persisted1 = await savedConfig(cfgPath, (c) => c.milestoneState?.followers === 200, 8000);
  const st = await stats();
  check('milestones: first poll records silently (no announce)', persisted1?.milestoneState?.followers === 200 && st.announceCount === before,
    `threshold ${JSON.stringify(persisted1?.milestoneState)}, announcements ${st.announceCount - before}`);
  check('milestones: threshold persisted', persisted1?.milestoneState?.followers === 200, JSON.stringify(persisted1?.milestoneState));
  check('milestones: JWT stays out of status', !!seStatus && !seStatus.value.includes(jwt), seStatus ? 'the JWT is in the status' : 'no status with se.configured seen');

  // Cross 300 → next poll announces.
  const atCross = client.events.length;
  followers = 312;
  const ann = await since(atCross, (d) => d.type === 'ticker:announce', 8000);
  check('milestones: crossing announces', ann?.kind === 'milestone' && /300 followers/.test(ann?.text), JSON.stringify(ann));
  const persisted2 = await savedConfig(cfgPath, (c) => c.milestoneState?.followers === 300);
  check('milestones: new threshold persisted', persisted2?.milestoneState?.followers === 300, JSON.stringify(persisted2?.milestoneState));

  // StreamElements end to end through ZIPPER_SE_BASE (decision 9): the mock stops
  // answering, the request times out, and the next poll still runs once it answers again.
  // A StreamElements poll pushes no status, so the test asks for one.
  {
    const probe = async () => {
      const at = client.events.length;
      client.ws.send(JSON.stringify({ request: 'DoAction', id: 's', action: { name: 'Ticker Command' }, args: { command: 'status', value: '' } }));
      const s = await since(at, (d) => isStatus(d) && parseStatus(d).se?.configured === true, 2000);
      return s ? parseStatus(s).se : null;
    };
    const want = `127.0.0.1:${SE_PORT} took longer than ${SE_LIMIT_MS / 1000} s to answer`;
    seStall = true;
    let seen = null;
    const timedOut = await waitFor(async () => { seen = await probe(); return seen?.lastError === want ? seen : null; }, 16000, 300);
    seStall = false;
    check('StreamElements: with the server not answering, the StreamElements error reports a timeout that names the host and the limit (shortened to 2 s), never the address or the token',
      !!timedOut && !timedOut.lastError.includes(jwt) && !timedOut.lastError.includes('/kappa'), JSON.stringify({ lastError: seen?.lastError ?? null, stalledRequests: seStalled.length }));
    const before = timedOut?.lastPollAt ?? seen?.lastPollAt ?? null;
    const recovered = await waitFor(async () => { const s = await probe(); return s && s.lastError === '' && s.lastPollAt && s.lastPollAt !== before ? s : null; }, 16000, 300);
    check('StreamElements: once the server answers again, the next StreamElements poll runs and clears the error',
      !!recovered, JSON.stringify({ recovered: !!recovered }));
    for (const r of seStalled) { try { r.destroy(); } catch {} }
  }

  await sc.stop();
  client.ws.close();
  await rm(cfgPath, { force: true });
}

// ── 8. start-zipper.bat: the Node version check ──────────────────────────────

// The launcher run in progress, so the exit handler can stop it with its whole tree.
let launcherProc = null;
function killTree(proc) {
  if (proc && proc.exitCode === null && proc.signalCode === null) {
    try { spawnSync('taskkill', ['/PID', String(proc.pid), '/T', '/F'], { stdio: 'ignore' }); } catch {}
  }
}
process.on('exit', () => killTree(launcherProc));

async function testLauncher() {
  console.log('\n[8] start-zipper.bat stops before npm and the sidecar when Node is missing or older than 20');
  if (process.platform !== 'win32') { console.log('  (skipped: start-zipper.bat runs only on Windows)'); return; }
  // A stand-in `node` (and `npm`) first on PATH. It prints the version it is told to and
  // writes every call to a log. Where the launcher goes past its check, it runs the
  // stand-in in place of the sidecar, so the real sidecar is never started.
  // Each run is fenced in as well, in case some other `node` ever runs in its place: the
  // environment points a sidecar at the suite's own side ports (the mock, guard port
  // GUARD_PORT, the StreamElements mock) and a throwaway config in the stand-in folder;
  // a run that has not ended after 20 s is stopped with its whole process tree; and after
  // each run nothing may answer on GUARD_PORT.
  const dir = join(TMP_DIR, 'launcher');
  const bat = resolve(__dirname, 'start-zipper.bat');
  // A copy of the launcher in a folder whose name has a space, & and parentheses (as in
  // "Program Files (x86)"). Its empty node_modules lets it go straight on to the sidecar.
  const oddDir = join(TMP_DIR, 'launcher copy & (x86)');
  mkdirSync(join(oddDir, 'node_modules'), { recursive: true });
  writeFileSync(join(oddDir, 'start-zipper.bat'), readFileSync(bat));
  const sandbox = new Set(['path', 'sb_ws_url', 'sb_ws_port', 'zipper_config', 'zipper_guard_port', 'zipper_se_base', 'zipper_allow_stub']);
  const runWith = async (version, batPath = bat) => {
    rmSync(dir, { recursive: true, force: true });
    mkdirSync(dir, { recursive: true });
    const log = join(dir, 'calls.log');
    const lines = ['@echo off', `>>"${log}" echo %~n0 %*`];
    writeFileSync(join(dir, 'npm.cmd'), lines.join('\r\n') + '\r\n');
    if (version) writeFileSync(join(dir, 'node.cmd'), [...lines, `echo ${version}`].join('\r\n') + '\r\n');
    const cfg = join(dir, 'config.json');
    writeFileSync(cfg, JSON.stringify({ tournamentUrl: null, autoStart: false }));
    const env = {};
    for (const [k, v] of Object.entries(process.env)) if (!sandbox.has(k.toLowerCase())) env[k] = v;
    // With no version there is no stand-in node, and the real one is left off PATH.
    env.Path = version ? `${dir};${process.env.PATH || process.env.Path || ''}` : `${dir};${process.env.SystemRoot || 'C:\\Windows'}\\System32`;
    env.SB_WS_URL = WS_URL;
    env.ZIPPER_CONFIG = cfg;
    env.ZIPPER_GUARD_PORT = String(GUARD_PORT);
    env.ZIPPER_SE_BASE = `http://127.0.0.1:${SE_PORT}`;
    // With /s, cmd takes off the first and the last quote of the line, so the path gets
    // a second pair: it stays quoted, spaces, & and parentheses included.
    const proc = launcherProc = spawn(process.env.ComSpec || 'cmd.exe', ['/d', '/s', '/c', `""${batPath}""`],
      { env, windowsVerbatimArguments: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let out = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { out += d; });
    proc.stdin.end();
    let timedOut = false;
    const code = await new Promise((res) => {
      let settled = false, late = null;
      const finish = (c) => { if (!settled) { settled = true; clearTimeout(timer); clearTimeout(late); res(c); } };
      const timer = setTimeout(() => {
        timedOut = true;
        killTree(proc);
        // A process the run started may still hold the pipes: do not wait on it for ever.
        late = setTimeout(() => finish(proc.exitCode), 5000);
      }, 20000);
      proc.on('close', (c) => finish(c));
      proc.on('error', () => finish(null));
    });
    launcherProc = null;
    const stray = await portAnswers(GUARD_PORT);
    const calls = existsSync(log) ? readFileSync(log, 'utf8') : '';
    return { code, out, calls: calls.split(/\r?\n/).filter(Boolean), timedOut, stray };
  };
  const stopped = (r, found) => r.code === 1 && !r.timedOut && !r.stray
    && (r.out.match(/needs Node\.js 20 or later/g) || []).length === 1 && r.out.includes('nodejs.org') && r.out.includes(`(found: ${found})`)
    && !r.calls.some((c) => /zipper-sidecar\.mjs/.test(c) || /^npm\b/.test(c));
  const went = (r) => !r.timedOut && !r.stray && !/needs Node\.js 20 or later/.test(r.out) && r.calls.some((c) => /^node zipper-sidecar\.mjs/.test(c));
  const v18 = await runWith('v18.20.4');
  check('launcher: Node v18 stops with one line naming Node.js 20 and nodejs.org; neither npm nor the sidecar runs',
    stopped(v18, 'v18.20.4'), JSON.stringify(v18));
  const v9 = await runWith('v9.11.2'), v19 = await runWith('v19.9.0');
  check('launcher: v9 and v19 stop too (the major version is compared as a number, not as text)',
    stopped(v9, 'v9.11.2') && stopped(v19, 'v19.9.0'), JSON.stringify({ v9, v19 }));
  const none = await runWith(null);
  check('launcher: no node on PATH stops with the same line', stopped(none, 'none'), JSON.stringify(none));
  const v20 = await runWith('v20.0.0'), v22 = await runWith('v22.21.0');
  check('launcher: v20 and v22 go on as before, to `node zipper-sidecar.mjs` (here the stand-in); each run ends by itself and leaves nothing on the guard port',
    went(v20) && went(v22), JSON.stringify({ v20, v22 }));
  const oddBat = join(oddDir, 'start-zipper.bat');
  const odd18 = await runWith('v18.20.4', oddBat), odd22 = await runWith('v22.21.0', oddBat);
  check('launcher: from a folder whose name has a space, & and parentheses, v18 stops and v22 goes on, as from the repo folder',
    stopped(odd18, 'v18.20.4') && went(odd22), JSON.stringify({ odd18, odd22 }));
  rmSync(dir, { recursive: true, force: true });
  rmSync(oddDir, { recursive: true, force: true });
}

// ── Run ───────────────────────────────────────────────────────────────────────

try {
  await rm(TMP_DIR, { recursive: true, force: true });
  await mkdir(TMP_DIR, { recursive: true });
  await testNormalizers();
  testDetection();
  await testFetchWrapper();
  await startMock();
  await testPanelCore();
  await testMock();
  await testCaseTable();
  await testSidecar();
  await testMilestones();
  await testLauncher();
} catch (err) {
  fail++;
  console.error(`\nFATAL: ${err.stack || err}`);
} finally {
  for (const sc of sidecars) await sc.stop();
  try { seServer?.close(); } catch {}
  try { mockProc?.kill(); } catch {}
  await rm(TMP_DIR, { recursive: true, force: true }).catch(() => {});
  await rm(TYPES_DIR, { recursive: true, force: true }).catch(() => {});
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : 'FAILURES'} — ${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
