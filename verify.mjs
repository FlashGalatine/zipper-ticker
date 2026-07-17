// Zipper verify suite — no network, no Streamer.bot install, no browser.
//   1. Platform normalizer tests against recorded fixtures (pure functions).
//   2. Platform detection/parseId routing.
//   3. Mock-SB E2E: the REAL panel-core.js runs in a browser shim against the
//      mock server — Subscribe (lowercase), sync-on-connect replay, live push.
//   4. Sidecar E2E: the REAL zipper-sidecar.mjs runs against the mock with the
//      stub: platform — command round-trips, payload shape, diff suppression,
//      caps re-push.
//
// Uses off-default ports (HTTP 7476 / WS 8082 / guard 7497) so it can run next
// to a live Streamer.bot or sidecar. Run: npm run verify

import { spawn } from 'node:child_process';
import { readFile, writeFile, rm } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import WebSocket from 'ws';

import { detectPlatform, PLATFORMS } from './platforms/index.js';
import { normalizeChallongeApi, normalizeChallongeStore } from './platforms/challonge.js';
import { normalizeStartggEvent } from './platforms/startgg.js';
import { normalizeMatcherino } from './platforms/matcherino.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIX = (f) => resolve(__dirname, 'platforms', 'fixtures', f);
const HTTP_PORT = 7476;
const WS_PORT = 8082;
const GUARD_PORT = 7497;
const HTTP = `http://127.0.0.1:${HTTP_PORT}`;
const WS_URL = `ws://127.0.0.1:${WS_PORT}/`;

let pass = 0, fail = 0;
function check(name, ok, detail) {
  if (ok) { pass++; console.log(`  ✔ ${name}`); }
  else { fail++; console.log(`  ✘ ${name}${detail ? ` — ${detail}` : ''}`); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const loadFixture = async (f) => JSON.parse(await readFile(FIX(f), 'utf-8'));

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

// ── Mock server helpers ───────────────────────────────────────────────────────

let mockProc = null;
async function startMock() {
  mockProc = spawn(process.execPath, [resolve(__dirname, 'mock-sb-server.mjs')], {
    env: { ...process.env, SB_HTTP_PORT: String(HTTP_PORT), SB_WS_PORT: String(WS_PORT) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  mockProc.stderr.on('data', (d) => process.stderr.write(`[mock-err] ${d}`));
  for (let i = 0; i < 50; i++) {
    try {
      const res = await fetch(`${HTTP}/mock/stats`);
      if (res.ok) return;
    } catch { /* not up yet */ }
    await sleep(100);
  }
  throw new Error('mock server did not come up');
}

function wsClient() {
  return new Promise((res, rej) => {
    const ws = new WebSocket(WS_URL);
    const events = [];
    const waiters = [];
    ws.on('open', () => {
      ws.send(JSON.stringify({ request: 'Subscribe', id: 'sub', events: { general: ['Custom'] } }));
      setTimeout(() => res({ ws, events, next }), 100);
    });
    ws.on('message', (raw) => {
      let m; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m?.event?.source === 'General' && m.event.type === 'Custom') {
        let d = m.data;
        if (typeof d === 'string') { try { d = JSON.parse(d); } catch { return; } }
        events.push(d);
        for (const w of waiters.splice(0)) w();
      }
    });
    ws.on('error', rej);
    function next(pred, timeoutMs = 5000) {
      return new Promise((resolve2, reject2) => {
        const scan = () => {
          const hit = events.find(pred);
          if (hit) { resolve2(hit); return true; }
          return false;
        };
        if (scan()) return;
        const t = setTimeout(() => reject2(new Error('timeout waiting for event')), timeoutMs);
        const w = () => { if (scan()) clearTimeout(t); else waiters.push(w); };
        waiters.push(w);
      });
    }
  });
}

// ── 3. panel-core in a browser shim ──────────────────────────────────────────

async function testPanelCore() {
  console.log('\n[3] panel-core (real file, browser shim, mock SB)');

  const winListeners = new Map();
  const received = [];
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
  await sleep(800);
  const sync = received.find((e) => e.type === 'ticker:update');
  check('panel-core: sync-on-connect replay received', !!sync, JSON.stringify(received.map((e) => e.type)));
  check('panel-core: empty update shape', sync && Array.isArray(sync.detail.matches) && sync.detail.matches.length === 0);

  // Live push through the mock's HTTP surface (what the sidecar's DoAction becomes).
  const sample = await readFile(FIX('sample-update.json'), 'utf-8');
  await fetch(`${HTTP}/mock/push`, { method: 'POST', body: sample });
  await sleep(300);
  const live = received.filter((e) => e.type === 'ticker:update').pop();
  check('panel-core: live ticker:update dispatched', live && live.detail.matches.length === 4,
    live ? String(live.detail.matches.length) : 'none');
  check('panel-core: caps travel in payload', live && live.detail.caps.text === '@FlashGalatine');

  // Announcement channel: broadcast → ticker:announce CustomEvent.
  await fetch(`${HTTP}/mock/announce?text=${encodeURIComponent('Flash just subscribed!')}&kind=sub&duration=3`);
  await sleep(300);
  const ann = received.find((e) => e.type === 'ticker:announce');
  check('panel-core: ticker:announce dispatched', ann && ann.detail.kind === 'sub' && ann.detail.duration === 3,
    JSON.stringify(ann?.detail));
}

// ── 4. HTTP serving ───────────────────────────────────────────────────────────

async function testHttp() {
  console.log('\n[4] mock HTTP maps');
  const overlay = await fetch(`${HTTP}/zipper-overlay/ticker.html`);
  const overlayBody = await overlay.text();
  check('overlay served', overlay.status === 200);
  check('overlay references /zipper-shared/panel-core.js', overlayBody.includes('/zipper-shared/panel-core.js'));
  check('panel-core served', (await fetch(`${HTTP}/zipper-shared/panel-core.js`)).status === 200);
  check('control page served', (await fetch(`${HTTP}/zipper-shared/control.html`)).status === 200);
  check('traversal guarded', (await fetch(`${HTTP}/zipper-shared/..%2Fconfig.example.json`)).status === 404);
}

// ── 5. Sidecar E2E ────────────────────────────────────────────────────────────

let sidecarProc = null;
async function testSidecar() {
  console.log('\n[5] sidecar (real process, stub platform, mock SB)');

  const cfgPath = resolve(tmpdir(), `zipper-verify-config-${process.pid}.json`);
  await writeFile(cfgPath, JSON.stringify({
    tournamentUrl: null, pollIntervalMs: 15000, maxItems: 20,
    caps: { text: '', logo: '' }, autoStart: false,
  }), 'utf-8');

  const client = await wsClient();
  const statsBefore = await (await fetch(`${HTTP}/mock/stats`)).json();

  sidecarProc = spawn(process.execPath, [resolve(__dirname, 'zipper-sidecar.mjs')], {
    env: {
      ...process.env,
      SB_WS_URL: WS_URL,
      ZIPPER_CONFIG: cfgPath,
      ZIPPER_GUARD_PORT: String(GUARD_PORT),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  sidecarProc.stderr.on('data', (d) => process.stderr.write(`[sidecar-err] ${d}`));

  const isStatus = (d) => d.type === 'ticker:command' && d.command === '__status';
  const parseStatus = (d) => JSON.parse(d.value);

  // client.next() scans the WHOLE event log, so a predicate that also matches an
  // earlier event ("3 matches", "no message key") resolves instantly on history
  // instead of waiting for the push under test. Those need a scan of only the
  // events that arrived after a recorded mark.
  const mark = () => client.events.length;
  const since = async (at, pred, timeoutMs = 4000) => {
    for (let i = 0; i * 100 < timeoutMs; i++) {
      const hit = client.events.slice(at).filter(pred).pop();
      if (hit) return hit;
      await sleep(100);
    }
    return null;
  };
  const isUpdate = (d) => d.type === 'ticker:update';

  // Startup status announces the connection.
  const s0 = await client.next(isStatus).then(parseStatus);
  check('sidecar: startup status broadcast', s0.polling === false && s0.url === null, JSON.stringify(s0));

  // setUrl → status echo carries the stub platform.
  const cmd = (command, value) => client.ws.send(JSON.stringify({
    request: 'DoAction', id: 'c', action: { name: 'Ticker Command' },
    args: { command, value: value ?? '' },
  }));
  cmd('setUrl', 'stub:platforms/fixtures/stub-result.json');
  const s1 = await client.next((d) => isStatus(d) && parseStatus(d).url?.startsWith('stub:')).then(parseStatus);
  check('sidecar: setUrl round-trip', s1.platform === 'stub', JSON.stringify(s1));

  // start → poll → ticker:update pushed through the Ticker Push action.
  cmd('start');
  const upd = await client.next((d) => d.type === 'ticker:update');
  check('sidecar: ticker:update pushed', upd.matches.length === 3, String(upd.matches?.length));
  check('sidecar: sorted completed → live → upcoming',
    upd.matches[0].state === 'completed' && upd.matches[1].state === 'in_progress' && upd.matches[2].state === 'upcoming',
    upd.matches.map((m) => m.state).join(','));
  check('sidecar: tournament block', upd.tournament.name === 'Stub Invitational' && upd.tournament.platform === 'stub');

  // Second poll with identical data → diff suppression (no new push).
  const statsMid = await (await fetch(`${HTTP}/mock/stats`)).json();
  cmd('pollNow');
  await client.next((d) => isStatus(d) && parseStatus(d).lastPollAt && parseStatus(d).lastPollAt !== s1.lastPollAt, 5000).catch(() => null);
  await sleep(400);
  const statsAfter = await (await fetch(`${HTTP}/mock/stats`)).json();
  check('sidecar: identical poll suppressed', statsAfter.pushCount === statsMid.pushCount,
    `pushCount ${statsMid.pushCount} → ${statsAfter.pushCount}`);

  // Caps change → forced re-push with the new caps text.
  cmd('setCapsText', '@FlashGalatine');
  const upd2 = await client.next((d) => d.type === 'ticker:update' && d.caps?.text === '@FlashGalatine');
  check('sidecar: caps change forces re-push', upd2.caps.text === '@FlashGalatine');

  // Chat "!ticker <cmd> …" exactly as a real SB Command trigger fires it: SB's
  // OWN command arg ("!ticker") + commandId + rawInput — the action must parse
  // rawInput instead of relaying "!ticker" verbatim.
  client.ws.send(JSON.stringify({
    request: 'DoAction', id: 'c', action: { name: 'Ticker Command' },
    args: { command: '!ticker', commandId: 'trig-1', rawInput: 'setCapsText @ChatSet' },
  }));
  const updChat = await client.next((d) => d.type === 'ticker:update' && d.caps?.text === '@ChatSet');
  check('sidecar: chat !ticker maps through the trigger shape', updChat.caps.text === '@ChatSet');

  // Batched setCaps (one command, JSON value — the control page's burst-safe
  // path). Sets both text and logo at once; '@'-leading text must survive.
  cmd('setCaps', JSON.stringify({ text: '@NewbieFightClub', logo: '/zipper-overlay/x.png' }));
  const updCaps = await client.next((d) => d.type === 'ticker:update' && d.caps?.text === '@NewbieFightClub');
  check('sidecar: batched setCaps sets text + logo',
    updCaps.caps.text === '@NewbieFightClub' && updCaps.caps.logo === '/zipper-overlay/x.png', JSON.stringify(updCaps.caps));

  // Batched setOptions (one command, JSON value) — interval + maxItems + topN.
  cmd('setOptions', JSON.stringify({ intervalMs: 20000, maxItems: 5, topN: 0 }));
  const sOpts = await client.next((d) => isStatus(d) && parseStatus(d).intervalMs === 20000).then(parseStatus);
  check('sidecar: batched setOptions applies all three', sOpts.intervalMs === 20000 && sOpts.maxItems === 5 && sOpts.topN === 0,
    JSON.stringify({ intervalMs: sOpts.intervalMs, maxItems: sOpts.maxItems, topN: sOpts.topN }));

  // topN filter: only matches involving the top-2 placed (Flash, Rival) remain.
  cmd('setTopN', '2');
  const upd3 = await client.next((d) => d.type === 'ticker:update' && d.matches.length === 2);
  check('sidecar: topN filters to top-placed participants',
    upd3.matches.every((m) => ['Flash', 'Rival'].includes(m.p1.name) || ['Flash', 'Rival'].includes(m.p2.name)),
    JSON.stringify(upd3.matches.map((m) => `${m.p1.name} vs ${m.p2.name}`)));
  check('sidecar: topN trims standings crawl', upd3.standings.length === 2, String(upd3.standings.length));
  const atReset = mark();
  cmd('setTopN', '0');
  const updReset = await since(atReset, (d) => isUpdate(d) && d.matches.length === 3);
  check('sidecar: topN 0 restores every match', !!updReset, 'no re-push after setTopN 0');

  // Persistent message mode: takes the strip over, results ride along underneath.
  cmd('setMessage', JSON.stringify({ text: 'Bracket starts at 8pm ET\n\n!discord for the lobby ', enabled: true }));
  const updMsg = await client.next((d) => d.type === 'ticker:update' && d.message);
  check('sidecar: setMessage splits + trims lines, drops blanks',
    JSON.stringify(updMsg.message.lines) === JSON.stringify(['Bracket starts at 8pm ET', '!discord for the lobby']),
    JSON.stringify(updMsg.message));
  check('sidecar: results still ride under the message', updMsg.matches.length === 3, String(updMsg.matches.length));
  const sMsg = await client.next((d) => isStatus(d) && parseStatus(d).message?.enabled).then(parseStatus);
  check('sidecar: message state in status', sMsg.message.text.startsWith('Bracket starts'), JSON.stringify(sMsg.message));

  // Enabled + blank text === off (no message key → results show).
  const atBlank = mark();
  cmd('setMessageText', '   ');
  const updBlank = await since(atBlank, (d) => isUpdate(d) && !d.message);
  check('sidecar: enabled but blank text falls back to results',
    updBlank?.matches.length === 3, JSON.stringify(updBlank?.message ?? updBlank?.matches.length));

  // Toggle off → message key gone even with the text still set.
  cmd('setMessageText', 'Back in 5');
  await client.next((d) => d.type === 'ticker:update' && d.message?.lines?.[0] === 'Back in 5');
  const atOff = mark();
  cmd('setMessageEnabled', 'off'); // chat spelling, not a JSON boolean
  const updOff = await since(atOff, (d) => isUpdate(d) && !d.message);
  check('sidecar: setMessageEnabled off restores results',
    updOff?.matches.length === 3, JSON.stringify(updOff?.message ?? updOff?.matches.length));

  // Persisted for a restart (text survives the toggle).
  await sleep(200);
  const persistedMsg = JSON.parse(await readFile(cfgPath, 'utf-8'));
  check('sidecar: message persisted',
    persistedMsg.message?.text === 'Back in 5' && persistedMsg.message?.enabled === false,
    JSON.stringify(persistedMsg.message));

  // stop → status shows polling off; config persisted autoStart=false.
  cmd('stop');
  const s2 = await client.next((d) => isStatus(d) && parseStatus(d).polling === false && parseStatus(d).lastPollAt).then(parseStatus);
  check('sidecar: stop round-trip', s2.polling === false);

  // The headline case: message mode with polling off. Nothing polls, so the
  // command itself has to put it on the strip.
  const atStopped = mark();
  cmd('setMessage', JSON.stringify({ text: 'Back in 5', enabled: true }));
  const updStopped = await since(atStopped, isUpdate);
  check('sidecar: message pushes immediately while polling is off',
    updStopped?.message?.lines?.[0] === 'Back in 5', JSON.stringify(updStopped?.message));

  // Same for end caps while stopped — they'd otherwise wait for a poll that
  // message-only setups never run.
  const atCaps = mark();
  cmd('setCapsText', '@Stopped');
  const updCapsStopped = await since(atCaps, isUpdate);
  check('sidecar: caps push immediately while polling is off',
    updCapsStopped?.caps?.text === '@Stopped', JSON.stringify(updCapsStopped?.caps));

  cmd('setMessageEnabled', 'off');
  await sleep(200);
  const persisted = JSON.parse(await readFile(cfgPath, 'utf-8'));
  check('sidecar: config persisted (url + autoStart off)',
    persisted.tournamentUrl?.startsWith('stub:') && persisted.autoStart === false, JSON.stringify(persisted));

  client.ws.close();
  await rm(cfgPath, { force: true });
}

// ── 6. Milestone announcements (SE mock + real sidecar) ──────────────────────

let seSidecarProc = null;
let seServer = null;
async function testMilestones() {
  console.log('\n[6] milestones (real sidecar, mock StreamElements)');
  const { createServer } = await import('node:http');

  // Tiny StreamElements sessions mock with a mutable follower counter.
  let followers = 250;
  const SE_PORT = 7477;
  seServer = createServer((req, res) => {
    if (req.url?.startsWith('/kappa/v2/sessions/')) {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ data: { 'follower-total': { count: followers } } }));
    } else {
      res.writeHead(404); res.end();
    }
  });
  await new Promise((r) => seServer.listen(SE_PORT, '127.0.0.1', r));

  // JWT whose payload carries the channel claim, like the real one.
  const jwt = `x.${Buffer.from(JSON.stringify({ channel: 'testchan' })).toString('base64url')}.x`;
  const cfgPath = resolve(tmpdir(), `zipper-verify-se-${process.pid}.json`);
  await writeFile(cfgPath, JSON.stringify({
    tournamentUrl: null, autoStart: false,
    streamElements: { jwtToken: jwt, pollSeconds: 5 },
    milestones: { followers: 100 },
  }), 'utf-8');

  const client = await wsClient();
  const before = (await (await fetch(`${HTTP}/mock/stats`)).json()).announceCount;

  seSidecarProc = spawn(process.execPath, [resolve(__dirname, 'zipper-sidecar.mjs')], {
    env: {
      ...process.env,
      SB_WS_URL: WS_URL,
      ZIPPER_CONFIG: cfgPath,
      ZIPPER_GUARD_PORT: '7498',
      ZIPPER_SE_BASE: `http://127.0.0.1:${SE_PORT}`,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  seSidecarProc.stderr.on('data', (d) => process.stderr.write(`[se-sidecar-err] ${d}`));

  // First poll records the 200 threshold silently.
  await sleep(1500);
  let stats = await (await fetch(`${HTTP}/mock/stats`)).json();
  check('milestones: first poll records silently (no announce)', stats.announceCount === before, String(stats.announceCount - before));
  const persisted1 = JSON.parse(await readFile(cfgPath, 'utf-8'));
  check('milestones: threshold persisted', persisted1.milestoneState?.followers === 200, JSON.stringify(persisted1.milestoneState));
  check('milestones: JWT stays out of status', !JSON.stringify(
    (await (async () => { const s = await client.next((d) => d.type === 'ticker:command' && d.command === '__status'); return JSON.parse(s.value); })()),
  ).includes(jwt));

  // Cross 300 → next poll announces.
  followers = 312;
  const ann = await client.next((d) => d.type === 'ticker:announce', 8000);
  check('milestones: crossing announces', ann.kind === 'milestone' && /300 followers/.test(ann.text), JSON.stringify(ann));
  await sleep(300);
  const persisted2 = JSON.parse(await readFile(cfgPath, 'utf-8'));
  check('milestones: new threshold persisted', persisted2.milestoneState?.followers === 300, JSON.stringify(persisted2.milestoneState));

  client.ws.close();
  await rm(cfgPath, { force: true });
}

// ── Run ───────────────────────────────────────────────────────────────────────

try {
  await testNormalizers();
  testDetection();
  await startMock();
  await testPanelCore();
  await testHttp();
  await testSidecar();
  await testMilestones();
} catch (err) {
  fail++;
  console.error(`\nFATAL: ${err.stack || err}`);
} finally {
  try { sidecarProc?.kill(); } catch {}
  try { seSidecarProc?.kill(); } catch {}
  try { seServer?.close(); } catch {}
  try { mockProc?.kill(); } catch {}
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : 'FAILURES'} — ${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
