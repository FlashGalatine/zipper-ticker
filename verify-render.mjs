// Real-pixel render verification: drives the overlay + control page as they ship, with
// a few test-only changes, each named where it is used. The out-of-order pages get a
// stand-in clock (skewClock). Several control pages have their 10 s status probe held
// back: some with noProbeInterval, some with init scripts of their own that do the same
// and more (one also records every frame the page sends, one also sets the random part
// of its reconnect wait to 0). The refused-port pages get a stand-in WebSocket that never
// connects (one also hides panel-core's flag from the page, one also keeps panel-core's
// sb:connection from the page's own listener). Two pages are served the older
// panel-core.js of 3a8efef, and one a 404 in its place (openPage's route). It runs in a
// real Chromium (system Edge/Chrome via playwright-core; nothing bundled)
// against the mock SB server, injects the sample fixture, and asserts the DOM.
// The control page is typed into (edits kept until a status carries what was applied,
// ZIP-03), its banner and notes are read against the mock's missing, silent, throw,
// queue and kick switches (ZIP-17), its send queue runs under bleed mode (decision
// 13), and its preview escapes both scores (ZIP-18). No sidecar runs here: the test
// sends the statuses one would, and reads what the page sent from the mock's bus.
// Every page it loads also records page errors and console errors, and the run
// fails on any. Writes test-render-1920.png / test-render-640.png / control-render.png
// (gitignored). Run: npm run verify:render
// First time: npm install (playwright-core is a dev dependency)
//
// Uses the mock on HTTP 7476 / WS 8082, and stops with one line, testing nothing,
// when anything already answers on them. The mock is stopped when the run ends.

import { spawn, spawnSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { connect } from 'node:net';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';

const __dirname = dirname(fileURLToPath(import.meta.url));
const HTTP_PORT = 7476;
const WS_PORT = 8082;
const HTTP = `http://127.0.0.1:${HTTP_PORT}`;

// Sends a ticker:status through the mock's Ticker Command, the way the sidecar does
// (the reserved command __status), and resolves once the mock has answered.
async function sendStatus(status) {
  const ws = new WebSocket(`ws://127.0.0.1:${WS_PORT}/`);
  try {
    await new Promise((res, rej) => { ws.once('open', res); ws.once('error', rej); });
    const answered = new Promise((res) => ws.on('message', (raw) => { try { if (JSON.parse(String(raw)).id === 'status') res(true); } catch {} }));
    ws.send(JSON.stringify({ request: 'DoAction', id: 'status', action: { name: 'Ticker Command' }, args: { command: '__status', value: JSON.stringify(status) } }));
    await Promise.race([answered, sleep(3000)]);
  } finally {
    try { ws.close(); } catch {}
  }
}

// A whole ticker:status as the sidecar sends it, with `over` laid on top.
function statusOf(over = {}) {
  return {
    type: 'ticker:status', seq: 1, polling: false, url: null, platform: null, lastPollAt: null, nextPollAt: null, lastError: '',
    commandNotice: '', actionNotice: '', configWarning: '', intervalMs: 30000, maxItems: 20, topN: 0,
    caps: { text: '', logo: '' }, message: { enabled: false, text: '' },
    auto: { enabled: true, eventState: null, startAt: null, waitingUntil: null, stopped: null },
    se: { configured: false, lastPollAt: null, lastError: '' },
    ...over,
  };
}

// A subscribed client of the mock's bus: the data of every General.Custom broadcast it
// receives, in order. Resolves once its Subscribe is answered.
async function busWatch() {
  const ws = new WebSocket(`ws://127.0.0.1:${WS_PORT}/`);
  const seen = [];
  await new Promise((res, rej) => {
    ws.once('error', rej);
    ws.on('open', () => ws.send(JSON.stringify({ request: 'Subscribe', id: 'sub', events: { general: ['Custom'] } })));
    ws.on('message', (raw) => {
      let m; try { m = JSON.parse(String(raw)); } catch { return; }
      if (m && m.id === 'sub' && !m.event) { res(); return; }
      if (m?.event?.source === 'General' && m.event.type === 'Custom') {
        let d = m.data;
        if (typeof d === 'string') { try { d = JSON.parse(d); } catch {} }
        seen.push(d);
      }
    });
  });
  return {
    seen,
    // The ticker:command broadcasts since index `from`.
    commands: (from = 0) => seen.slice(from).filter((d) => d && d.type === 'ticker:command'),
    close: () => { try { ws.close(); } catch {} },
  };
}

// Polls fn until it gives something truthy, or the time is up; gives what it gave, or null.
async function waitUntil(fn, timeoutMs = 4000, stepMs = 25) {
  const end = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() >= end) return null;
    await sleep(stepMs);
  }
}

let pass = 0, fail = 0;
function check(name, ok, detail) {
  if (ok) { pass++; console.log(`  ✔ ${name}`); }
  else { fail++; console.log(`  ✘ ${name}${detail ? ` — ${detail}` : ''}`); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
  for (const port of [HTTP_PORT, WS_PORT]) if (await portAnswers(port)) busy.push(port);
  if (busy.length) {
    console.error(`verify:render: something already answers on 127.0.0.1 port ${busy.join(', ')} (most likely a mock left by an earlier run); stop it and run again. Nothing was tested.`);
    process.exit(1);
  }
}

let chromium;
try {
  ({ chromium } = await import('playwright-core'));
} catch {
  console.error('playwright-core is not installed. Run:  npm install');
  process.exit(1);
}

// The mock never outlives this run: stopped on exit, on a signal and on a crash, and
// (MOCK_EXIT_WITH_PARENT, with stdin a pipe) it exits by itself if this process is
// killed outright.
let mock = null;
const stopMock = () => { if (mock && mock.exitCode === null) { try { mock.kill(); } catch {} } };
process.on('exit', stopMock);
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGBREAK']) process.on(sig, () => { stopMock(); process.exit(130); });

// Each page records its page errors and console errors; closing it checks there were none.
// The one console error passed over is the 404 for /favicon.ico: the browser asks for it
// by itself on its first page, neither page names one, and Streamer.bot's HTTP server
// has none to give either.
let browser = null;
// A stand-in for the clock the stamp rule's 5 s window runs on (panel-core reads
// window.performance.now()): the real one plus a skew the test moves on with
// window.__zipperTestClockSkew(ms), so the window is tested without waiting for it. Only
// the pages opened with it get it, through addInitScript; nothing the pages ship reads it.
function skewClock() {
  const real = performance.now.bind(performance);
  let skew = 0;
  performance.now = () => real() + skew;
  window.__zipperTestClockSkew = (ms) => { skew += ms; };
}
// Holds back the control page's 10 s status probe (its setInterval of 10000 ms never
// runs), so a test that reads a state the probe's answer would change, or counts what
// the page sends, sees only its own clicks. Only the pages opened with it get it.
function noProbeInterval() {
  const realSetInterval = window.setInterval;
  window.setInterval = (fn, ms, ...rest) => (ms === 10000 ? 0 : realSetInterval(fn, ms, ...rest));
}
// route, when given, serves one file the page asks for from memory (route.path on this
// server, with route.body, and route.status or 200), in this page's browser only: the mock
// and the disk keep theirs. initArg, when given, is handed to init.
async function openPage(label, viewport, url, { init, initArg, route } = {}) {
  const page = await browser.newPage({ viewport });
  if (init) await page.addInitScript(init, initArg);
  if (route) await page.route((u) => u.origin === HTTP && u.pathname === route.path, (r) => r.fulfill({ status: route.status ?? 200, contentType: 'application/javascript', body: route.body }));
  const problems = [];
  page.on('pageerror', (err) => problems.push(`page error: ${err.message}`));
  page.on('console', (msg) => {
    if (msg.type() !== 'error') return;
    const at = msg.location()?.url || '';
    if (/^http:\/\/127\.0\.0\.1:\d+\/favicon\.ico$/.test(at)) return;
    problems.push(`console error: ${msg.text()} (${at || 'no url'})`);
  });
  await page.goto(url);
  return { page, label, problems };
}
async function closePage(p) {
  check(`${p.label}: no page error and no console error`, p.problems.length === 0, p.problems.join(' | '));
  await p.page.close();
}

// The mock gets the shell's environment without the API keys a developer may have set
// (in any casing: Windows matches environment names without regard to case).
const mockEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => !['CHALLONGE_API_KEY', 'STARTGG_API_KEY'].includes(k.toUpperCase())));

try {
  mock = spawn(process.execPath, [resolve(__dirname, 'mock-sb-server.mjs')], {
    env: { ...mockEnv, SB_HTTP_PORT: String(HTTP_PORT), SB_WS_PORT: String(WS_PORT), MOCK_EXIT_WITH_PARENT: '1', MOCK_ARG_BLEED: '' },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  let mockOut = '';
  mock.stdout.on('data', (d) => { mockOut = (mockOut + d).slice(-20000); });
  mock.stderr.on('data', (d) => { mockOut = (mockOut + d).slice(-20000); });
  let up = false;
  for (let i = 0; i < 80 && !up && mock.exitCode === null; i++) {
    try { up = (await fetch(`${HTTP}/mock/stats`)).ok; } catch {}
    if (!up) await sleep(100);
  }
  if (!up) throw new Error(`the mock server did not come up (${mock.exitCode === null ? 'no answer in 8 s' : `it exited with code ${mock.exitCode}`})\n${mockOut}`);

  // Preload the sample payload so sync-on-connect paints the strip immediately.
  await fetch(`${HTTP}/mock/push`, {
    method: 'POST',
    body: await readFile(resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8'),
  });

  let launchedWith = null;
  for (const channel of ['msedge', 'chrome', undefined]) {
    try { browser = await chromium.launch({ channel }); launchedWith = channel || 'the Chromium playwright-core installs'; break; } catch { /* next */ }
  }
  if (!browser) throw new Error('no system Edge/Chrome and no bundled Chromium — run: npx playwright-core install chromium');
  console.log(`browser: ${launchedWith} ${browser.version()}`);

  for (const w of [1920, 640]) {
    const p = await openPage(`${w}px page`, { width: w, height: 72 }, `${HTTP}/zipper-overlay/ticker.html?w=${w}&sbport=${WS_PORT}`);
    const { page } = p;
    await page.waitForSelector('.track .seg .item', { timeout: 8000 });
    await sleep(400);

    const segs = await page.locator('.track .seg').count();
    check(`${w}px: track duplicated for seamless loop`, segs >= 2, String(segs));
    const items = await page.locator('.track .seg').first().locator('.item').count();
    check(`${w}px: items rendered`, items >= 4, String(items));
    const winText = await page.locator('.track .name.win').first().textContent();
    check(`${w}px: winner accented`, winText === 'Rival' || winText === 'Flash', String(winText));
    const live = await page.locator('.track .live-tag').count();
    check(`${w}px: LIVE tag present`, live >= 1, String(live));
    const capText = await page.locator('.cap-right').textContent();
    check(`${w}px: right cap text`, capText === '@FlashGalatine', String(capText));
    const stripW = await page.locator('.zipper').evaluate((el) => el.getBoundingClientRect().width);
    check(`${w}px: strip width honors ?w=`, Math.round(stripW) === w, String(stripW));
    const dur = await page.locator('.track').evaluate((el) => getComputedStyle(el).animationDuration);
    check(`${w}px: scroll animation running`, parseFloat(dur) > 0, dur);

    await page.screenshot({ path: resolve(__dirname, `test-render-${w}.png`) });
    await closePage(p);
  }

  // ?h= sets the strip height (mirrors ?w=).
  {
    const H = 120;
    const p = await openPage('height page', { width: 1920, height: H }, `${HTTP}/zipper-overlay/ticker.html?w=1920&h=${H}&sbport=${WS_PORT}`);
    const { page } = p;
    await page.waitForSelector('.track .seg .item', { timeout: 8000 });
    const stripH = await page.locator('.zipper').evaluate((el) => el.getBoundingClientRect().height);
    check(`height: strip honors ?h=${H}`, Math.round(stripH) === H, String(stripH));
    await closePage(p);
  }

  // Announcement takeover: strip pauses, announcement shows, then restores.
  {
    const p = await openPage('announce page', { width: 1920, height: 72 }, `${HTTP}/zipper-overlay/ticker.html?w=1920&sbport=${WS_PORT}`);
    const { page } = p;
    await page.waitForSelector('.track .seg .item', { timeout: 8000 });
    await fetch(`${HTTP}/mock/announce?text=${encodeURIComponent('Flash is raiding with 42 viewers!')}&kind=raid&duration=4`);
    await page.waitForSelector('.zipper.announcing', { timeout: 5000 });
    await sleep(500); // let the announce-in fade finish before the pixel grab
    await page.screenshot({ path: resolve(__dirname, 'test-render-announce.png') });
    const annText = await page.locator('#announce-text').textContent();
    check('announce: takeover shows text', annText === 'Flash is raiding with 42 viewers!', String(annText));
    const pill = await page.locator('#announce-pill').textContent();
    check('announce: kind pill', pill === 'Raid', String(pill));
    const paused = await page.locator('.track').evaluate((el) => getComputedStyle(el).animationPlayState);
    check('announce: crawl paused during takeover', paused === 'paused', paused);
    await page.waitForSelector('.zipper:not(.announcing)', { timeout: 8000 });
    const resumed = await page.locator('.track').evaluate((el) => getComputedStyle(el).animationPlayState);
    check('announce: crawl resumes after duration', resumed === 'running', resumed);
    const items = await page.locator('.track .seg').first().locator('.item').count();
    check('announce: results intact after takeover', items >= 4, String(items));
    await closePage(p);
  }

  // Persistent message mode: the configured lines crawl instead of the results.
  {
    const p = await openPage('message page', { width: 1920, height: 72 }, `${HTTP}/zipper-overlay/ticker.html?w=1920&sbport=${WS_PORT}`);
    const { page } = p;
    await page.waitForSelector('.track .seg .item', { timeout: 8000 });

    const sample = JSON.parse(await readFile(resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8'));
    const lines = ['Bracket starts at 8pm ET', '!discord for the lobby code'];
    // Results stay in the payload — the message is expected to cover them.
    await fetch(`${HTTP}/mock/push`, { method: 'POST', body: JSON.stringify({ ...sample, message: { lines } }) });
    await page.waitForSelector('.item.message', { timeout: 5000 });
    await sleep(300);

    // One item per line, repeated whole until the segment fills the strip (two
    // short lines don't span 1920px) so the -50% wrap stays seamless.
    const msgs = await page.locator('.track .seg').first().locator('.item.message').count();
    check('message: one item per line, repeated to fill the strip',
      msgs >= lines.length && msgs % lines.length === 0, String(msgs));
    const first = await page.locator('.track .seg .item.message .msg').first().textContent();
    check('message: line text rendered', first === lines[0], String(first));
    const names = await page.locator('.track .name').count();
    check('message: results hidden while the message is up', names === 0, String(names));
    const capText = await page.locator('.cap-right').textContent();
    check('message: end caps still render', capText === '@FlashGalatine', String(capText));
    const segs = await page.locator('.track .seg').count();
    check('message: track still duplicated for a seamless loop', segs >= 2, String(segs));
    const dur = await page.locator('.track').evaluate((el) => getComputedStyle(el).animationDuration);
    check('message: crawl animating', parseFloat(dur) > 0, dur);
    await page.screenshot({ path: resolve(__dirname, 'test-render-message.png') });

    // Dropping the message restores results with no re-poll — and leaves the
    // mock's stored payload clean for the control-page checks below.
    await fetch(`${HTTP}/mock/push`, { method: 'POST', body: JSON.stringify(sample) });
    await page.waitForSelector('.track .seg .item .name', { timeout: 5000 });
    const msgsAfter = await page.locator('.item.message').count();
    check('message: results return when the message drops', msgsAfter === 0, String(msgsAfter));
    await closePage(p);
  }

  // Out of order (ZIP-11, decision 14): the overlay and the control preview each get a
  // newer payload, then a broadcast of an older one, and both keep the newer one on
  // screen. A payload with the same stamp and other content is applied, and so is the
  // C#'s connect-sync empty update, which has no generatedAt. The older one is ignored
  // only within 5 s of the page applying its newest (the owner's ruling of 2026-11-25):
  // past that it is applied and its stamp becomes the newest, so a far-future stamp does
  // not freeze the pages. The pages' clock is moved on (skewClock), not waited for.
  {
    const sample = JSON.parse(await readFile(resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8'));
    const named = (stamp, name) => {
      const body = { ...sample, matches: sample.matches.map((m, i) => (i === 0 ? { ...m, p1: { ...m.p1, name } } : m)) };
      if (stamp === undefined) delete body.generatedAt; else body.generatedAt = stamp;
      return JSON.stringify(body);
    };
    const push = (body) => fetch(`${HTTP}/mock/push`, { method: 'POST', body });
    const ov = await openPage('out-of-order overlay page', { width: 1920, height: 72 }, `${HTTP}/zipper-overlay/ticker.html?w=1920&sbport=${WS_PORT}`, { init: skewClock });
    const ctl = await openPage('out-of-order control page', { width: 700, height: 900 }, `${HTTP}/zipper-shared/control.html?sbport=${WS_PORT}`, { init: skewClock });
    const skew = async (ms) => {
      for (const p of [ov, ctl]) await p.page.evaluate((m) => window.__zipperTestClockSkew(m), ms);
    };
    await ov.page.waitForSelector('.track .seg .item', { timeout: 8000 });
    await ctl.page.waitForSelector('#preview .m', { timeout: 8000 });
    const shows = async (name) => {
      const track = await ov.page.locator('.track').textContent();
      const preview = await ctl.page.locator('#preview').textContent();
      return { overlay: track.includes(name), preview: preview.includes(name) };
    };
    const waitBoth = async (name) => {
      await ov.page.waitForFunction((n) => document.querySelector('.track').textContent.includes(n), name, { timeout: 5000 }).catch(() => {});
      await ctl.page.waitForFunction((n) => document.getElementById('preview').textContent.includes(n), name, { timeout: 5000 }).catch(() => {});
      return shows(name);
    };
    // A marker each page shows once everything broadcast before it has arrived: an
    // announcement for the overlay, a status line for the control page.
    let markers = 0;
    const marker = async () => {
      const text = `order marker ${++markers}`;
      await fetch(`${HTTP}/mock/announce?text=${encodeURIComponent(text)}&kind=custom&duration=2`);
      await sendStatus({ type: 'ticker:status', polling: false, url: null, platform: null, lastPollAt: null, lastError: '', commandNotice: text, intervalMs: 30000, maxItems: 20, topN: 0,
        caps: { text: '', logo: '' }, message: { enabled: false, text: '' }, auto: { enabled: true, eventState: null, startAt: null, waitingUntil: null, stopped: null }, se: { configured: false, lastPollAt: null, lastError: '' } });
      await ov.page.waitForFunction((t) => document.getElementById('announce-text').textContent === t, text, { timeout: 5000 }).catch(() => {});
      await ctl.page.waitForFunction((t) => document.getElementById('status').textContent.includes(t), text, { timeout: 5000 }).catch(() => {});
    };

    await push(named('2026-07-11T20:20:00Z', 'NewerName'));
    const newer = await waitBoth('NewerName');
    await push(named('2026-07-11T20:10:00Z', 'OlderName'));
    await marker();
    const kept = await shows('NewerName');
    const older = await shows('OlderName');
    check('out of order: after a newer payload, a broadcast of an older one (by generatedAt) is ignored: the overlay and the control preview both keep the newer one on screen',
      newer.overlay && newer.preview && kept.overlay && kept.preview && !older.overlay && !older.preview, JSON.stringify({ newer, kept, older }));
    await push(named('2026-07-11T20:20:00Z', 'EqualName'));
    const equal = await waitBoth('EqualName');
    check('out of order: a payload with the same stamp and other content is applied on both pages', equal.overlay && equal.preview, JSON.stringify(equal));
    // Past the window: the older one is applied, and the newest is reset to its stamp.
    await skew(5001);
    await push(named('2026-07-11T20:10:00Z', 'LateOlderName'));
    const late = await waitBoth('LateOlderName');
    await push(named('2026-07-11T20:05:00Z', 'BelowResetName'));
    await marker();
    const belowKept = await shows('LateOlderName');
    const below = await shows('BelowResetName');
    await push(named('2026-07-11T20:15:00Z', 'AboveResetName'));
    const above = await waitBoth('AboveResetName');
    check('out of order: an older payload that arrives more than 5 s after a page applied its newest is applied on both pages; the newest is reset to its stamp, so one older than that, just after, is ignored, and one newer is applied',
      late.overlay && late.preview && belowKept.overlay && belowKept.preview && !below.overlay && !below.preview && above.overlay && above.preview,
      JSON.stringify({ late, belowKept, below, above }));
    // A far-future stamp ("9999-…") freezes neither page past the window.
    await push(named('9999-12-31T23:59:59Z', 'FarFutureName'));
    const far = await waitBoth('FarFutureName');
    await push(named('2026-07-11T20:16:00Z', 'FrozenName'));
    await marker();
    const farKept = await shows('FarFutureName');
    const frozen = await shows('FrozenName');
    await skew(5001);
    await push(named('2026-07-11T20:16:00Z', 'ThawedName'));
    const thawed = await waitBoth('ThawedName');
    check('out of order: a far-future stamp ("9999-…") does not freeze the pages: an ordinary payload after it is ignored within the 5 s, and applied on both pages once they have passed',
      far.overlay && far.preview && farKept.overlay && farKept.preview && !frozen.overlay && !frozen.preview && thawed.overlay && thawed.preview,
      JSON.stringify({ far, farKept, frozen, thawed }));
    // Every newer payload starts the window again, as each of the sidecar's pushes does in
    // steady use: past the window once more, a newer payload is applied, and an older one
    // 4 s after it is ignored on both pages.
    await skew(6000);
    await push(named('2026-07-11T20:17:00Z', 'RestartName'));
    const restart = await waitBoth('RestartName');
    await skew(4000);
    await push(named('2026-07-11T20:16:30Z', 'InsideRestartName'));
    await marker();
    const restartKept = await shows('RestartName');
    const inside = await shows('InsideRestartName');
    check('out of order: every newer payload starts the 5 s window again: past the window, a newer payload is applied on both pages, and an older one 4 s after it is ignored on both',
      restart.overlay && restart.preview && restartKept.overlay && restartKept.preview && !inside.overlay && !inside.preview,
      JSON.stringify({ restart, restartKept, inside }));
    // The C#'s connect-sync with nothing stored: the empty update, with no generatedAt,
    // broadcast to every page.
    await fetch(`${HTTP}/mock/reset`);
    {
      const ws = new WebSocket(`ws://127.0.0.1:${WS_PORT}/`);
      try {
        await new Promise((res, rej) => { ws.once('open', res); ws.once('error', rej); });
        const answered = new Promise((res) => ws.on('message', (raw) => { try { if (JSON.parse(String(raw)).id === 'sync') res(true); } catch {} }));
        ws.send(JSON.stringify({ request: 'DoAction', id: 'sync', action: { name: 'Ticker Push' }, args: { reason: 'overlay-connect' } }));
        await Promise.race([answered, sleep(3000)]);
      } finally {
        try { ws.close(); } catch {}
      }
    }
    await ov.page.waitForFunction(() => document.querySelector('.track').textContent.includes('waiting for results'), null, { timeout: 5000 }).catch(() => {});
    await ctl.page.waitForFunction(() => document.getElementById('preview').textContent.includes('empty ticker payload'), null, { timeout: 5000 }).catch(() => {});
    const emptyOverlay = await ov.page.locator('.track').textContent();
    const emptyPreview = await ctl.page.locator('#preview').textContent();
    check('out of order: the C#\'s connect-sync empty update, which has no generatedAt, is applied on both pages',
      emptyOverlay.includes('waiting for results') && emptyPreview.includes('empty ticker payload'), JSON.stringify({ emptyOverlay, emptyPreview }));
    // The store as the control-page checks below expect it: the sample.
    await push(JSON.stringify(sample));
    await closePage(ov);
    await closePage(ctl);
  }

  // Control page: preview + status fields render.
  const p = await openPage('control page', { width: 700, height: 900 }, `${HTTP}/zipper-shared/control.html?sbport=${WS_PORT}`);
  const { page } = p;
  await page.waitForSelector('#preview .m', { timeout: 8000 });
  const rows = await page.locator('#preview .m').count();
  check('control: preview mirrors ticker payload', rows >= 5, String(rows));
  // The mock has no sidecar consuming ticker:command, so no __status ever comes back.
  // Streamer.bot itself answers (the probe's reply, and its echo), so the banner is
  // "sidecar silent" (ZIP-17): the sidecar note, and no other line.
  await page.waitForSelector('body.no-sidecar', { timeout: 6000 }).catch(() => {});
  const warnShown = await page.locator('#sidecar-note').isVisible();
  const otherNote = await page.evaluate(() => { const el = document.getElementById('sb-note'); return el ? el.textContent : null; });
  check('control (ZIP-17): with no sidecar, once the probe\'s echo has had no status for 3 s, the banner says the sidecar is silent, and nothing else',
    warnShown && otherNote === '', JSON.stringify({ warnShown, otherNote }));
  // S2: this page has the current panel-core.js, which sets the marker, so the banner does
  // not say an older copy is loaded.
  const coreNow = await page.evaluate(() => ({ api: window.__ZIPPER_PANEL_CORE?.api ?? null, sb: document.getElementById('sb-note').textContent }));
  check('control (S2): with the current panel-core.js (it sets __ZIPPER_PANEL_CORE to { api: 2 }) the banner does not say an older copy is loaded',
    coreNow.api === 2 && !coreNow.sb.includes('older copy of panel-core.js'), JSON.stringify(coreNow));
  await page.screenshot({ path: resolve(__dirname, 'control-render.png') });

  // The status line shows the sidecar's commandNotice (an unknown or refused command),
  // as text: the name in it comes from whoever sent the command.
  {
    const notice = 'unknown command "<i>x</i>" — ignored';
    await sendStatus({
      type: 'ticker:status', polling: false, url: null, platform: null, lastPollAt: null, lastError: '',
      commandNotice: notice, intervalMs: 30000, maxItems: 20, topN: 0,
      caps: { text: '', logo: '' }, message: { enabled: false, text: '' },
      auto: { enabled: true, eventState: null, startAt: null, waitingUntil: null, stopped: null },
      se: { configured: false, lastPollAt: null, lastError: '' },
    });
    await page.waitForFunction((t) => document.getElementById('status').textContent.includes(t), notice, { timeout: 5000 }).catch(() => {});
    const text = await page.locator('#status').textContent();
    const tags = await page.locator('#status i').count();
    check('control: the status line shows the sidecar\'s commandNotice, as text', text.includes(notice) && tags === 0, text);
  }
  // …and its actionNotice (row 4.2: an action the sidecar sends to answered "not found").
  {
    const notice = 'Ticker Push not found';
    await sendStatus({
      type: 'ticker:status', polling: false, url: null, platform: null, lastPollAt: null, lastError: '',
      commandNotice: '', actionNotice: notice, intervalMs: 30000, maxItems: 20, topN: 0,
      caps: { text: '', logo: '' }, message: { enabled: false, text: '' },
      auto: { enabled: true, eventState: null, startAt: null, waitingUntil: null, stopped: null },
      se: { configured: false, lastPollAt: null, lastError: '' },
    });
    await page.waitForFunction((t) => document.getElementById('status').textContent.includes(t), notice, { timeout: 5000 }).catch(() => {});
    const text = await page.locator('#status').textContent();
    const errs = await page.locator('#status .err').allTextContents();
    check('control: the status line shows the sidecar\'s actionNotice ("Ticker Push not found") as an error line', errs.includes(notice), text);
  }
  // …and its configWarning (row 5.1: a config.json that did not load, a refused key, a
  // hand edit while the sidecar ran), as text, on an error line of its own.
  {
    const warning = 'config.json is not valid JSON (line 4); the sidecar runs on its defaults and saves nothing until the file is fixed and the sidecar restarted';
    await sendStatus({
      type: 'ticker:status', polling: false, url: null, platform: null, lastPollAt: null, lastError: '',
      commandNotice: '', actionNotice: '', configWarning: warning, intervalMs: 30000, maxItems: 20, topN: 0,
      caps: { text: '', logo: '' }, message: { enabled: false, text: '' },
      auto: { enabled: true, eventState: null, startAt: null, waitingUntil: null, stopped: null },
      se: { configured: false, lastPollAt: null, lastError: '' },
    });
    await page.waitForFunction((t) => document.getElementById('status').textContent.includes(t), warning, { timeout: 5000 }).catch(() => {});
    const text = await page.locator('#status').textContent();
    const errs = await page.locator('#status .err').allTextContents();
    check('control: the status line shows the sidecar\'s configWarning as an error line of its own', errs.includes(warning), text);
  }
  await closePage(p);

  const mockSwitch = async (path) => (await fetch(`${HTTP}${path}`)).json();
  const enc = encodeURIComponent;
  const sampleText = await readFile(resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8');
  const controlUrl = `${HTTP}/zipper-shared/control.html?sbport=${WS_PORT}`;
  // What a control page shows besides its status line: the banner (#sb-note, and the
  // sidecar note), and the page's own notes (#page-note).
  const notesOf = (page) => page.evaluate(() => {
    const sb = document.getElementById('sb-note');
    const side = document.getElementById('sidecar-note');
    const pageNote = document.getElementById('page-note');
    return {
      sb: sb ? sb.textContent : null,
      sbShown: !!sb && getComputedStyle(sb).display !== 'none',
      sidecar: !!side && getComputedStyle(side).display !== 'none',
      page: pageNote ? pageNote.textContent : null,
    };
  });

  // Edits kept (ZIP-03, row 6.1): a field is dirty from its first input until a status
  // carries the value its own Apply or Set sent, as the sidecar stores it. No sidecar runs
  // here: the test sends the statuses one would (sendStatus), each with a marker in
  // lastError that it waits for on the page, and reads what the page sent from the bus.
  {
    const p = await openPage('typing control page', { width: 700, height: 900 }, controlUrl);
    const { page } = p;
    const bus = await busWatch();
    try {
      await page.waitForSelector('body:not(.offline)', { timeout: 8000 });
      let n = 0;
      // Gives whether the page showed the status (its marker), so a check that reads a box
      // after it can tell a status that was applied from one that never arrived: the "stays
      // as typed" half of a check is the state the box is in before any status.
      const status = async (over = {}) => {
        const mark = `typing status ${++n}`;
        await sendStatus(statusOf({ lastError: mark, ...over }));
        return page.waitForFunction((t) => document.getElementById('status').textContent.includes(t), mark, { timeout: 5000 }).then(() => true, () => false);
      };
      const box = (id) => page.evaluate((i) => {
        const el = document.getElementById(i);
        return { value: el.type === 'checkbox' ? el.checked : el.value, dirty: el.classList.contains('dirty') };
      }, id);
      // The border a box is drawn with: the accent (--acc, #ffd166) marks an edit.
      const borderOf = (id) => page.evaluate((i) => getComputedStyle(document.getElementById(i)).borderTopColor, id);
      const ACCENT = 'rgb(255, 209, 102)';
      const EDGE = 'rgb(42, 51, 66)';
      const borders = {};
      // The value of the first ticker:command named `command` the page sent after `from`.
      const sentValue = async (command, from) => {
        const d = await waitUntil(() => bus.commands(from).find((x) => x.command === command), 4000);
        return d ? d.value : null;
      };
      const parsed = (v) => { try { return JSON.parse(v); } catch { return null; } };

      const firstSeen = await status({ caps: { text: 'Old cap', logo: '' }, message: { enabled: false, text: 'Live message' } });
      borders.cleanText = await borderOf('capstext');
      // Typing, moving to the logo box, and a status meanwhile.
      await page.locator('#capstext').fill('Typed cap');
      borders.dirtyText = await borderOf('capstext');
      await page.locator('#capslogo').focus();
      const typedSeen = await status({ caps: { text: 'Old cap', logo: '' }, message: { enabled: false, text: 'Live message' } });
      const typed = await box('capstext');
      check('control (ZIP-03): a cap text typed, then the logo box focused, survives a status that still carries the old text: the box keeps what was typed, marked as an edit',
        firstSeen && typedSeen && typed.value === 'Typed cap' && typed.dirty === true, JSON.stringify({ firstSeen, typedSeen, typed }));
      let from = bus.seen.length;
      await page.click('#btn-caps');
      const capsSent = parsed(await sentValue('setCaps', from));
      check('control (ZIP-03): Apply caps sends the typed text', capsSent?.text === 'Typed cap', JSON.stringify(capsSent));
      const staleSeen = await status({ caps: { text: 'Old cap', logo: '' }, message: { enabled: false, text: 'Live message' } });
      const stale = await box('capstext');
      check('control (ZIP-03): a status after the Apply that still carries the old text (one sent before the Apply reached the sidecar) does not put the old text back',
        staleSeen && stale.value === 'Typed cap' && stale.dirty === true, JSON.stringify({ staleSeen, stale }));
      const settledSeen = await status({ caps: { text: 'Typed cap', logo: '' }, message: { enabled: false, text: 'Live message' } });
      const settled = await box('capstext');
      check('control (ZIP-03): the status that carries the text the Apply sent settles the box: it shows that text and is no longer marked',
        settledSeen && settled.value === 'Typed cap' && settled.dirty === false, JSON.stringify({ settledSeen, settled }));

      // Interval 10 settles at 15 (the sidecar's minimum), interval 0 at 30 (the page's
      // fallback); a status still carrying the old value leaves each as typed.
      await page.locator('#interval').fill('10');
      borders.dirtyNumber = await borderOf('interval');
      from = bus.seen.length;
      await page.click('#btn-opts');
      const opts10 = parsed(await sentValue('setOptions', from));
      const seen10 = [await status({ intervalMs: 30000, caps: { text: 'Typed cap', logo: '' }, message: { enabled: false, text: 'Live message' } })];
      const stale10 = await box('interval');
      seen10.push(await status({ intervalMs: 15000, caps: { text: 'Typed cap', logo: '' }, message: { enabled: false, text: 'Live message' } }));
      const settled10 = await box('interval');
      check('control (ZIP-03): interval 10, applied (sent as 10000 ms), stays as typed through a status that still says 30 s, and settles at 15 once a status carries 15000 ms',
        seen10.every(Boolean) && opts10?.intervalMs === 10000 && stale10.value === '10' && stale10.dirty === true && settled10.value === '15' && settled10.dirty === false,
        JSON.stringify({ seen10, opts10, stale10, settled10 }));
      await page.locator('#interval').fill('0');
      from = bus.seen.length;
      await page.click('#btn-opts');
      const opts0 = parsed(await sentValue('setOptions', from));
      const seen0 = [await status({ intervalMs: 15000, caps: { text: 'Typed cap', logo: '' }, message: { enabled: false, text: 'Live message' } })];
      const stale0 = await box('interval');
      seen0.push(await status({ intervalMs: 30000, caps: { text: 'Typed cap', logo: '' }, message: { enabled: false, text: 'Live message' } }));
      const settled0 = await box('interval');
      check('control (ZIP-03): interval 0, applied (sent as 30000 ms, the page\'s fallback), stays as typed through a status that still says 15 s, and settles at 30',
        seen0.every(Boolean) && opts0?.intervalMs === 30000 && stale0.value === '0' && stale0.dirty === true && settled0.value === '30' && settled0.dirty === false,
        JSON.stringify({ seen0, opts0, stale0, settled0 }));
      // Max items above 100 settles at 100 (the sidecar's cap).
      await page.locator('#maxitems').fill('250');
      from = bus.seen.length;
      await page.click('#btn-opts');
      const optsMax = parsed(await sentValue('setOptions', from));
      const seenMax = [await status({ maxItems: 20, caps: { text: 'Typed cap', logo: '' }, message: { enabled: false, text: 'Live message' } })];
      const staleMax = await box('maxitems');
      seenMax.push(await status({ maxItems: 100, caps: { text: 'Typed cap', logo: '' }, message: { enabled: false, text: 'Live message' } }));
      const settledMax = await box('maxitems');
      check('control (ZIP-03): max items 250 stays as typed through a status that still says 20, and settles at 100, the sidecar\'s cap',
        seenMax.every(Boolean) && optsMax?.maxItems === 250 && staleMax.value === '250' && staleMax.dirty === true && settledMax.value === '100' && settledMax.dirty === false,
        JSON.stringify({ seenMax, optsMax, staleMax, settledMax }));

      // Esc puts the live value back: in the message box (a textarea) and in an input.
      await page.locator('#msgtext').fill('a draft line');
      const draft = await box('msgtext');
      borders.dirtyTextarea = await borderOf('msgtext');
      await page.locator('#msgtext').press('Escape');
      const revertedMsg = await box('msgtext');
      borders.revertedTextarea = await borderOf('msgtext');
      check('control (row 6.1): Esc in the message box (a textarea) puts back the live value and drops the mark',
        draft.dirty === true && revertedMsg.value === 'Live message' && revertedMsg.dirty === false, JSON.stringify({ draft, revertedMsg }));
      await page.locator('#capstext').fill('another draft');
      await page.locator('#capstext').press('Escape');
      const revertedCap = await box('capstext');
      check('control (row 6.1): Esc in an input puts back the live value and drops the mark',
        revertedCap.value === 'Typed cap' && revertedCap.dirty === false, JSON.stringify(revertedCap));
      // The mark is drawn: a box typed in gets the accent border (a text box, a number box
      // and the message textarea alike), and one not typed in, or put back, keeps its edge.
      check('control (row 6.1): a box typed in is drawn with the accent border (text, number and textarea boxes alike); a box not typed in, or put back with Esc, keeps its plain edge',
        borders.dirtyText === ACCENT && borders.dirtyNumber === ACCENT && borders.dirtyTextarea === ACCENT
          && borders.cleanText === EDGE && borders.revertedTextarea === EDGE, JSON.stringify(borders));

      // The two checkboxes: each sends at once, and a status that still carries the old
      // state does not flip it back.
      from = bus.seen.length;
      await page.click('#msgon');
      const msgSent = parsed(await sentValue('setMessage', from));
      const seenOn = [await status({ message: { enabled: false, text: 'Live message' }, caps: { text: 'Typed cap', logo: '' } })];
      const staleOn = await box('msgon');
      seenOn.push(await status({ message: { enabled: true, text: 'Live message' }, caps: { text: 'Typed cap', logo: '' } }));
      const settledOn = await box('msgon');
      check('control (row 6.1): the message checkbox, ticked, sends its state with the text in the box (it counts as the message Apply), stays ticked through a status that still says off, and is settled by one that says on',
        seenOn.every(Boolean) && msgSent?.enabled === true && msgSent?.text === 'Live message' && staleOn.value === true && staleOn.dirty === true && settledOn.value === true && settledOn.dirty === false,
        JSON.stringify({ seenOn, msgSent, staleOn, settledOn }));
      from = bus.seen.length;
      await page.click('#autosched');
      const autoSent = await sentValue('setAutoSchedule', from);
      const seenAuto = [await status({ message: { enabled: true, text: 'Live message' }, caps: { text: 'Typed cap', logo: '' } })];
      const staleAuto = await box('autosched');
      seenAuto.push(await status({ message: { enabled: true, text: 'Live message' }, caps: { text: 'Typed cap', logo: '' }, auto: { enabled: false, eventState: null, startAt: null, waitingUntil: null, stopped: null } }));
      const settledAuto = await box('autosched');
      check('control (row 6.1): the Auto start/stop checkbox, unticked, stays unticked through a status that still says on, and is settled by one that says off',
        seenAuto.every(Boolean) && autoSent === 'false' && staleAuto.value === false && staleAuto.dirty === true && settledAuto.value === false && settledAuto.dirty === false,
        JSON.stringify({ seenAuto, autoSent, staleAuto, settledAuto }));

      // A logo the sidecar refuses stays as typed, and marked; commandNotice says why.
      const autoOff = { enabled: false, eventState: null, startAt: null, waitingUntil: null, stopped: null };
      await page.locator('#capslogo').fill('file:///C:/logo.png');
      from = bus.seen.length;
      await page.click('#btn-caps');
      const logoSent = parsed(await sentValue('setCaps', from));
      const seenLogo = await status({ caps: { text: 'Typed cap', logo: '' }, message: { enabled: true, text: 'Live message' }, auto: autoOff,
        commandNotice: 'Logo not changed: use an http(s) URL or a path on the overlay\'s server, such as /zipper-overlay/assets/logo.png' });
      const refusedLogo = await box('capslogo');
      check('control (row 6.1): a logo the sidecar refuses (the status keeps the old one) was sent by Apply caps, and stays as typed, and marked',
        seenLogo && logoSent?.logo === 'file:///C:/logo.png' && refusedLogo.value === 'file:///C:/logo.png' && refusedLogo.dirty === true,
        JSON.stringify({ seenLogo, logoSent, refusedLogo }));

      // A URL with spaces round it is sent trimmed and settled by the trimmed URL; a field
      // edited again after its Set stays as edited, and marked.
      await page.locator('#url').fill('  https://challonge.com/zipaaa  ');
      from = bus.seen.length;
      await page.click('#btn-seturl');
      const urlSent = await sentValue('setUrl', from);
      const seenUrl = [await status({ url: 'https://challonge.com/zipaaa', caps: { text: 'Typed cap', logo: '' }, message: { enabled: true, text: 'Live message' }, auto: autoOff })];
      const settledUrl = await box('url');
      await page.locator('#url').fill('https://challonge.com/zipbbb');
      from = bus.seen.length;
      await page.click('#btn-seturl');
      const urlSent2 = await sentValue('setUrl', from);
      await page.locator('#url').fill('https://challonge.com/zipbbbc');
      await page.locator('#capstext').focus();
      seenUrl.push(await status({ url: 'https://challonge.com/zipbbb', caps: { text: 'Typed cap', logo: '' }, message: { enabled: true, text: 'Live message' }, auto: autoOff }));
      const editedAgain = await box('url');
      check('control (row 6.1): a URL with spaces round it is sent trimmed and settled by the status that carries it trimmed; one edited again after its Set stays as edited, and marked, when the status carries what was sent',
        seenUrl.every(Boolean) && urlSent === 'https://challonge.com/zipaaa' && settledUrl.value === 'https://challonge.com/zipaaa' && settledUrl.dirty === false
          && urlSent2 === 'https://challonge.com/zipbbb' && editedAgain.value === 'https://challonge.com/zipbbbc' && editedAgain.dirty === true,
        JSON.stringify({ seenUrl, urlSent, urlSent2, settledUrl, editedAgain }));

      // The sidecar's other clamps and cuts (row 6.1): top N 150 settles at 100, and a caps
      // text, a logo and a message longer than the sidecar keeps settle at their first 120,
      // 500 and 1000 characters. The page sends them whole; a status that still carries
      // the old values leaves each as typed.
      const longCap = `${'C'.repeat(125)}tail`;
      const longLogo = `https://example.invalid/${'l'.repeat(600)}`;
      const longMsg = `line ${'m'.repeat(95)}\n`.repeat(11);
      await page.locator('#topn').fill('150');
      await page.locator('#capstext').fill(longCap);
      await page.locator('#capslogo').fill(longLogo);
      await page.locator('#msgtext').fill(longMsg);
      from = bus.seen.length;
      await page.click('#btn-opts');
      const optsCut = parsed(await sentValue('setOptions', from));
      from = bus.seen.length;
      await page.click('#btn-caps');
      const capsCut = parsed(await sentValue('setCaps', from));
      from = bus.seen.length;
      await page.click('#btn-msg');
      const msgCut = parsed(await sentValue('setMessage', from));
      const sentWhole = optsCut?.topN === 150 && capsCut?.text === longCap && capsCut?.logo === longLogo && msgCut?.text === longMsg;
      const base = { url: 'https://challonge.com/zipbbb', maxItems: 100, auto: autoOff };
      const seenCut = [await status({ ...base, topN: 0, caps: { text: 'Typed cap', logo: '' }, message: { enabled: true, text: 'Live message' } })];
      const staleCut = { topn: await box('topn'), capstext: await box('capstext'), capslogo: await box('capslogo'), msgtext: await box('msgtext') };
      seenCut.push(await status({ ...base, topN: 100, caps: { text: longCap.slice(0, 120), logo: longLogo.slice(0, 500) }, message: { enabled: true, text: longMsg.slice(0, 1000) } }));
      const settledCut = { topn: await box('topn'), capstext: await box('capstext'), capslogo: await box('capslogo'), msgtext: await box('msgtext') };
      const short = (b) => ({ dirty: b.dirty, length: typeof b.value === 'string' ? b.value.length : b.value });
      check('control (row 6.1): top N 150, a caps text of 129 characters, a logo of 624 and a message of 1111 are sent whole, stay as typed through a status with the old values, and settle at 100 and at their first 120, 500 and 1000 characters, as the sidecar keeps them',
        seenCut.every(Boolean) && sentWhole
          && staleCut.topn.value === '150' && staleCut.capstext.value === longCap && staleCut.capslogo.value === longLogo && staleCut.msgtext.value === longMsg
          && Object.values(staleCut).every((b) => b.dirty === true)
          && settledCut.topn.value === '100' && settledCut.capstext.value === longCap.slice(0, 120)
          && settledCut.capslogo.value === longLogo.slice(0, 500) && settledCut.msgtext.value === longMsg.slice(0, 1000)
          && Object.values(settledCut).every((b) => b.dirty === false),
        JSON.stringify({ seenCut, sentWhole, stale: Object.fromEntries(Object.entries(staleCut).map(([k, b]) => [k, short(b)])), settled: Object.fromEntries(Object.entries(settledCut).map(([k, b]) => [k, short(b)])) }));
    } finally {
      bus.close();
    }
    await closePage(p);
  }

  // The banner (ZIP-17, row 6.2) tells apart "action missing", "did not answer", a
  // ticker:error and "sidecar silent", from what Streamer.bot (here the mock) answers.
  // The page's 10 s status probe is held back here (noProbeInterval), so only the test's
  // clicks reach the mock, and each state is read while its switch is still on.
  {
    const p = await openPage('banner control page', { width: 700, height: 900 }, controlUrl, { init: noProbeInterval });
    const { page } = p;
    const sbNoteHas = (re) => page.waitForFunction((src) => new RegExp(src).test(document.getElementById('sb-note')?.textContent || ''), re.source, { timeout: 4000 }).catch(() => {});
    try {
      await page.waitForSelector('body:not(.offline)', { timeout: 8000 });
      // An action Streamer.bot does not have.
      let missing = null;
      await mockSwitch(`/mock/missing?name=${enc('Ticker Command')}&on=1`);
      try {
        await page.click('#btn-poll');
        await sbNoteHas(/Action missing/);
        await sleep(3200); // past the sidecar heartbeat: the sidecar is not blamed
        missing = await notesOf(page);
      } finally {
        await mockSwitch(`/mock/missing?name=${enc('Ticker Command')}&on=0`);
      }
      check('control (ZIP-17): a click whose action Streamer.bot does not have (the mock\'s missing switch) shows "Action missing", naming Ticker Command, and 3 s on still does not blame the sidecar',
        /Action missing/.test(missing?.sb || '') && (missing?.sb || '').includes('Ticker Command') && missing.sbShown && !missing.sidecar, JSON.stringify(missing));
      // Queued, but never runs: no echo.
      let silent = null;
      await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=1`);
      try {
        await page.click('#btn-poll');
        await sbNoteHas(/did not answer/);
        silent = await notesOf(page);
      } finally {
        await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=0`);
      }
      check('control (ZIP-17): an ok reply with no echo within 1.5 s (the mock\'s silent switch) shows "Ticker Command did not answer", not "sidecar silent"',
        /Ticker Command did not answer/.test(silent?.sb || '') && silent.sbShown && !silent.sidecar, JSON.stringify(silent));
      // The action throws: a ticker:error, which outranks "did not answer".
      let thrown = null;
      await mockSwitch(`/mock/throw?name=${enc('Ticker Command')}&on=1&message=${enc('mock throw for the banner')}`);
      try {
        await page.click('#btn-poll');
        await sleep(2000); // past the 1.5 s echo wait, so "did not answer" holds as well
        thrown = await notesOf(page);
      } finally {
        await mockSwitch(`/mock/throw?name=${enc('Ticker Command')}&on=0`);
      }
      check('control (ZIP-17): a ticker:error is shown with its message, and outranks "did not answer"',
        (thrown?.sb || '').includes('mock throw for the banner') && !/did not answer/.test(thrown?.sb || '') && thrown.sbShown, JSON.stringify(thrown));
      await page.click('#btn-poll');
      await page.waitForFunction(() => (document.getElementById('sb-note')?.textContent ?? 'none') === '', null, { timeout: 4000 }).catch(() => {});
      const cleared = await notesOf(page);
      // That echo is the new click's own: the earlier Poll now clicks that never echoed
      // (the silent one, the one that threw) must not take it, or the new click's own wait
      // would run out and say "did not answer" 1.5 s later.
      await sleep(1700);
      const stillClear = await notesOf(page);
      check('control (ZIP-17): the ticker:error line, and "did not answer", clear on the next echo, and stay clear past the 1.5 s echo wait: the echo is matched to the new click, not to an earlier one alike that never echoed',
        cleared.sb === '' && !cleared.sbShown && stillClear.sb === '' && !stillClear.sbShown, JSON.stringify({ cleared, stillClear }));

      // No answer at all: Streamer.bot holds the reply 3.5 s (the mock's delay switch), so
      // panel-core raises sb:action-timeout at 0.8 s and the page waits 1.5 s more for the
      // echo. The late answer and its run then end it.
      let delayed = null;
      let delayedLater = null;
      await mockSwitch(`/mock/delay?name=${enc('Ticker Command')}&ms=3500`);
      try {
        await page.click('#btn-poll');
        await sbNoteHas(/Ticker Command did not answer/);
        delayed = await notesOf(page);
        await page.waitForFunction(() => (document.getElementById('sb-note')?.textContent ?? 'none') === '', null, { timeout: 4000 }).catch(() => {});
        delayedLater = await notesOf(page);
      } finally {
        await mockSwitch(`/mock/delay?name=${enc('Ticker Command')}&ms=0`);
      }
      check('control (ZIP-17): a click Streamer.bot does not answer in time (its reply held 3.5 s) shows "Ticker Command did not answer" once 0.8 s with no answer and 1.5 s with no echo have passed; the late answer and echo take the line away',
        /Ticker Command did not answer/.test(delayed?.sb || '') && delayed.sbShown && delayedLater?.sb === '', JSON.stringify({ delayed, delayedLater }));

      // Another error answer is shown in Streamer.bot's own words. The mock has no such
      // answer (the action exists or it is "not found"), so the event panel-core raises
      // for one (code "error"; verify.mjs section 2d checks that half) is raised here.
      await page.evaluate(() => window.dispatchEvent(new CustomEvent('sb:action-error', {
        detail: { action: 'Ticker Command', id: 'render-check', sync: false, code: 'error', error: 'Something else went wrong' },
      })));
      const refused = await notesOf(page);
      await page.click('#btn-poll'); // an ok answer from Ticker Command ends it
      await page.waitForFunction(() => (document.getElementById('sb-note')?.textContent ?? 'none') === '', null, { timeout: 4000 }).catch(() => {});
      const refusedGone = await notesOf(page);
      check('control (ZIP-17): an error answer that is not "was not found" is shown in Streamer.bot\'s own words, naming the action; an ok answer from that action takes it away',
        refused.sb === 'Streamer.bot refused Ticker Command: Something else went wrong' && refused.sbShown && refusedGone.sb === '',
        JSON.stringify({ refused, refusedGone }));

      // A ticker:error with a code (row 9.4 adds one): the message, then the code. It clears
      // on a status as well as on an echo.
      let coded = null;
      try {
        await fetch(`${HTTP}/mock/push`, { method: 'POST', body: JSON.stringify({ type: 'ticker:error', message: 'A coded failure', code: 'ZIP_E42' }) });
        await sbNoteHas(/A coded failure/);
        coded = await notesOf(page);
      } finally {
        await fetch(`${HTTP}/mock/push`, { method: 'POST', body: sampleText }); // the store as the checks below expect it
      }
      await sendStatus(statusOf({ lastError: 'banner status mark' }));
      await page.waitForFunction(() => (document.getElementById('sb-note')?.textContent ?? 'none') === '', null, { timeout: 4000 }).catch(() => {});
      const codedGone = await notesOf(page);
      check('control (row 6.2): a ticker:error that carries a code shows its message with the code after it, and a status clears the line',
        coded?.sb === 'A Zipper action failed in Streamer.bot: A coded failure (ZIP_E42). See Streamer.bot\'s log.' && codedGone.sb === '',
        JSON.stringify({ coded, codedGone }));

      // The ticker:error line clears on a reconnect. Ticker Command is silent after the
      // reconnect, so the probe's echo (which would clear it too) never comes, and "did
      // not answer" waits 1.5 s: the banner is read before that.
      let thrownAgain = null;
      let afterReconnect = null;
      try {
        await mockSwitch(`/mock/throw?name=${enc('Ticker Command')}&on=1&message=${enc('mock throw before a reconnect')}`);
        await page.click('#btn-poll');
        await sbNoteHas(/mock throw before a reconnect/);
        thrownAgain = await notesOf(page);
        await mockSwitch(`/mock/throw?name=${enc('Ticker Command')}&on=0`);
        await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=1`);
        await page.evaluate(() => fetch('/mock/kick'));
        await page.waitForSelector('body.offline', { timeout: 3000 }).catch(() => {});
        await page.waitForSelector('body:not(.offline)', { timeout: 8000 }).catch(() => {});
        await sleep(300);
        afterReconnect = await notesOf(page);
      } finally {
        await mockSwitch(`/mock/throw?name=${enc('Ticker Command')}&on=0`);
        await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=0`);
      }
      check('control (row 6.2): the ticker:error line clears on a reconnect, before any echo or status comes',
        /mock throw before a reconnect/.test(thrownAgain?.sb || '') && afterReconnect?.sb === '' && !afterReconnect.sbShown,
        JSON.stringify({ thrownAgain, afterReconnect }));
      await page.click('#btn-poll'); // the silent probe's "did not answer" goes with this echo
      await page.waitForFunction(() => (document.getElementById('sb-note')?.textContent ?? 'none') === '', null, { timeout: 4000 }).catch(() => {});

      // The Default-queue note goes once a later answer names another queue.
      let inDefault = null;
      await mockSwitch(`/mock/queue?name=${enc('Ticker Command')}&queue=Default`);
      try {
        await page.click('#btn-poll');
        await page.waitForFunction(() => /Ticker Command is in Streamer\.bot's Default queue/.test(document.getElementById('page-note')?.textContent || ''), null, { timeout: 4000 }).catch(() => {});
        inDefault = await notesOf(page);
      } finally {
        await mockSwitch(`/mock/queue?name=${enc('Ticker Command')}&queue=`);
      }
      await page.click('#btn-poll');
      await page.waitForFunction(() => !/Default queue/.test(document.getElementById('page-note')?.textContent || ''), null, { timeout: 4000 }).catch(() => {});
      const outOfDefault = await notesOf(page);
      check('control (row 6.2): an answer naming the Default queue for Ticker Command puts the note under the status line; a later answer naming another queue takes it away',
        /^Ticker Command is in Streamer\.bot's Default queue/.test(inDefault?.page || '') && !/Default queue/.test(outOfDefault.page || ''),
        JSON.stringify({ inDefault, outOfDefault }));

      // A click, and an announcement, while Streamer.bot is offline: nothing is sent, and
      // the page says so under the status line. The announcement stays in its box.
      await page.evaluate(() => fetch('/mock/kick'));
      await page.waitForSelector('body.offline', { timeout: 3000 }).catch(() => {});
      const offlineClicks = await page.evaluate(() => {
        const note = () => document.getElementById('page-note').textContent;
        document.getElementById('btn-poll').click();
        const afterClick = note();
        document.getElementById('ann-text').value = 'Offline announcement';
        document.getElementById('btn-announce').click();
        return { offline: document.body.classList.contains('offline'), afterClick, afterAnnounce: note(), box: document.getElementById('ann-text').value };
      });
      check('control (row 6.2): a click while Streamer.bot is offline says that click was not sent; an announcement says so too, and stays in its box',
        offlineClicks.offline && offlineClicks.afterClick === 'Not connected to Streamer.bot: that click was not sent.'
          && offlineClicks.afterAnnounce === 'Not connected to Streamer.bot: that announcement was not sent.' && offlineClicks.box === 'Offline announcement',
        JSON.stringify(offlineClicks));
      await page.waitForSelector('body:not(.offline)', { timeout: 8000 }).catch(() => {});
      await sleep(500);
      await page.fill('#ann-text', '');

      // The Announce box keeps its text until its announcement comes back.
      let annSilent = null;
      await mockSwitch(`/mock/silent?name=${enc('Ticker Announce')}&on=1`);
      try {
        await page.fill('#ann-text', 'Silent announcement');
        await page.click('#btn-announce');
        await sbNoteHas(/Ticker Announce did not answer/);
        annSilent = { box: await page.inputValue('#ann-text'), ...(await notesOf(page)) };
      } finally {
        await mockSwitch(`/mock/silent?name=${enc('Ticker Announce')}&on=0`);
      }
      check('control (ZIP-17): an announcement that is queued and never comes back stays in the box, and the page says "Ticker Announce did not answer"',
        annSilent?.box === 'Silent announcement' && /Ticker Announce did not answer/.test(annSilent?.sb || ''), JSON.stringify(annSilent));
      await page.click('#btn-announce');
      await page.waitForFunction(() => document.getElementById('ann-text').value === '', null, { timeout: 4000 }).catch(() => {});
      const annBack = { box: await page.inputValue('#ann-text'), ...(await notesOf(page)) };
      check('control (ZIP-17): the Announce box clears once its announcement comes back (its echo), and "did not answer" goes',
        annBack.box === '' && annBack.sb === '', JSON.stringify(annBack));

      // Two clicks on Send with the same text, the second while the first is on its way
      // (the box keeps its text until the echo): the announcement is shown once.
      const annBus = await busWatch();
      let doubled = null;
      try {
        await page.fill('#ann-text', 'Double-clicked announcement');
        const from = annBus.seen.length;
        await page.evaluate(() => { const b = document.getElementById('btn-announce'); b.click(); b.click(); });
        await page.waitForFunction(() => document.getElementById('ann-text').value === '', null, { timeout: 4000 }).catch(() => {});
        await sleep(1200); // a second one would have followed the first's echo by now
        doubled = { box: await page.inputValue('#ann-text'), shown: annBus.seen.slice(from).filter((d) => d?.type === 'ticker:announce' && d.text === 'Double-clicked announcement').length };
      } finally {
        annBus.close();
      }
      check('control (ZIP-17): two clicks on Send with the same text, the second while the first is on its way, show the announcement once, and the box clears',
        doubled?.shown === 1 && doubled.box === '', JSON.stringify(doubled));

      // An announcement whose answer and run come late (the reply held 3.5 s): the page
      // says it did not answer, and when it does come back the box still clears.
      let lateAnn = null;
      let lateAfter = null;
      await mockSwitch(`/mock/delay?name=${enc('Ticker Announce')}&ms=3500`);
      try {
        await page.fill('#ann-text', 'Late announcement');
        await page.click('#btn-announce');
        await sbNoteHas(/Ticker Announce did not answer/);
        lateAnn = { box: await page.inputValue('#ann-text'), ...(await notesOf(page)) };
        await page.waitForFunction(() => document.getElementById('ann-text').value === '', null, { timeout: 4000 }).catch(() => {});
        lateAfter = { box: await page.inputValue('#ann-text'), ...(await notesOf(page)) };
      } finally {
        await mockSwitch(`/mock/delay?name=${enc('Ticker Announce')}&ms=0`);
      }
      check('control (ZIP-17): an announcement that comes back late (its reply held 3.5 s) keeps its text while the page says "Ticker Announce did not answer"; once it comes back the box clears and the line goes',
        lateAnn?.box === 'Late announcement' && /Ticker Announce did not answer/.test(lateAnn?.sb || '') && lateAfter?.box === '' && lateAfter?.sb === '',
        JSON.stringify({ lateAnn, lateAfter }));

      // Text ending in U+0085 (next line): the C# trims it, JavaScript's trim() does not.
      // The page sends the text as the action will trim it, so the echo matches.
      const NEL = String.fromCharCode(0x85);
      await page.fill('#ann-text', `Ends in a next line${NEL}`);
      const typedNel = await page.inputValue('#ann-text');
      await page.click('#btn-announce');
      await page.waitForFunction(() => document.getElementById('ann-text').value === '', null, { timeout: 4000 }).catch(() => {});
      await sleep(1700); // past the 1.5 s echo wait: no "did not answer" may follow
      const nel = { typedEndsInNel: typedNel.endsWith(NEL), box: await page.inputValue('#ann-text'), ...(await notesOf(page)) };
      check('control (ZIP-17): an announcement ending in U+0085 (next line) comes back and clears the box, and the page does not say it did not answer',
        nel.typedEndsInNel && nel.box === '' && nel.sb === '', JSON.stringify(nel));

      // Ticker Announce throws: its error line, not "did not answer", is what the page
      // says, and once the line clears (here on a click's echo) nothing takes its place.
      // The box keeps the text, and Send then sends it again.
      let annThrown = null;
      let annThrownGone = null;
      await mockSwitch(`/mock/throw?name=${enc('Ticker Announce')}&on=1&message=${enc('mock announce throw')}`);
      try {
        await page.fill('#ann-text', 'Throwing announcement');
        await page.click('#btn-announce');
        await sbNoteHas(/mock announce throw/);
        await sleep(1700); // past the 1.5 s echo wait
        annThrown = await notesOf(page);
      } finally {
        await mockSwitch(`/mock/throw?name=${enc('Ticker Announce')}&on=0`);
      }
      await page.click('#btn-poll');
      await page.waitForFunction(() => !/mock announce throw/.test(document.getElementById('sb-note')?.textContent || ''), null, { timeout: 4000 }).catch(() => {});
      await sleep(300);
      annThrownGone = { box: await page.inputValue('#ann-text'), ...(await notesOf(page)) };
      await page.click('#btn-announce');
      await page.waitForFunction(() => document.getElementById('ann-text').value === '', null, { timeout: 4000 }).catch(() => {});
      const retried = await page.inputValue('#ann-text');
      check('control (ZIP-17): when Ticker Announce throws, the page shows its error and not "did not answer", and after the next echo clears the error it does not say "Ticker Announce did not answer"; the box keeps the text, and Send then sends it again',
        /mock announce throw/.test(annThrown?.sb || '') && !/did not answer/.test(annThrown?.sb || '')
          && annThrownGone.sb === '' && annThrownGone.box === 'Throwing announcement' && retried === '',
        JSON.stringify({ annThrown, annThrownGone, retried }));
      let annMissing = null;
      await mockSwitch(`/mock/missing?name=${enc('Ticker Announce')}&on=1`);
      try {
        await page.fill('#ann-text', 'Missing announcement');
        await page.click('#btn-announce');
        await sbNoteHas(/Action missing/);
        annMissing = { box: await page.inputValue('#ann-text'), ...(await notesOf(page)) };
      } finally {
        await mockSwitch(`/mock/missing?name=${enc('Ticker Announce')}&on=0`);
      }
      check('control (ZIP-17): with Ticker Announce missing, the box keeps its text and the banner names Ticker Announce',
        annMissing?.box === 'Missing announcement' && /Action missing/.test(annMissing?.sb || '') && (annMissing?.sb || '').includes('Ticker Announce'), JSON.stringify(annMissing));
    } finally {
      for (const name of ['Ticker Command', 'Ticker Announce']) {
        await mockSwitch(`/mock/missing?name=${enc(name)}&on=0`);
        await mockSwitch(`/mock/silent?name=${enc(name)}&on=0`);
        await mockSwitch(`/mock/throw?name=${enc(name)}&on=0`);
      }
    }
    await closePage(p);
  }

  // The Default queue (row 6.2): the mock answers "Default" for Ticker Push only, which a
  // page hears in the reply to its connect-sync.
  {
    await mockSwitch(`/mock/queue?name=${enc('Ticker Push')}&queue=Default`);
    let p = null;
    let text = null;
    try {
      p = await openPage('Default-queue control page', { width: 700, height: 900 }, controlUrl);
      await p.page.waitForFunction(() => /Default queue/.test(document.getElementById('page-note')?.textContent || ''), null, { timeout: 5000 }).catch(() => {});
      text = (await notesOf(p.page)).page;
    } finally {
      await mockSwitch(`/mock/queue?name=${enc('Ticker Push')}&queue=`);
    }
    check('control (row 6.2): with Ticker Push in the Default queue (its replies only), the page says so, names Ticker Push and not Ticker Command as the one in it, and points to README, setup step 1',
      /^Ticker Push is in Streamer\.bot's Default queue/.test(text || '') && !/Ticker Command is in/.test(text || '') && (text || '').includes('README, setup step 1'),
      JSON.stringify(text));
    if (p) await closePage(p);
  }

  // A dropped connection (row 6.2): a socket closed (/mock/kick) while two clicks wait to
  // be sent. On this page the 10 s status probe is held back (its setInterval never
  // runs; the test calls the probe itself, as window.__zipperProbe) and every frame the
  // page sends is recorded, so the probe on reconnect is told from the periodic one.
  {
    const init = () => {
      const realSetInterval = window.setInterval;
      window.setInterval = (fn, ms, ...rest) => (ms === 10000 ? ((window.__zipperProbe = fn), 0) : realSetInterval(fn, ms, ...rest));
      window.__sentFrames = [];
      const realSend = WebSocket.prototype.send;
      WebSocket.prototype.send = function (data) {
        try { window.__sentFrames.push(String(data)); } catch {}
        return realSend.call(this, data);
      };
    };
    const p = await openPage('dropped-connection control page', { width: 700, height: 900 }, controlUrl, { init });
    const { page } = p;
    try {
      await page.waitForSelector('body:not(.offline)', { timeout: 8000 });
      // The connect probe has had its echo (nothing is in flight), and with no sidecar here
      // the banner already says the sidecar is silent: a close must not carry that over.
      await page.waitForSelector('body.no-sidecar', { timeout: 6000 }).catch(() => {});
      const before = await notesOf(page);
      await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=1`);
      // The first click is sent and gets no echo (silent), so it holds the next two for
      // 800 ms. The page asks for the kick itself, right after the clicks (the mock serves
      // the page, so /mock/kick is on its own address), well inside that.
      const clickedAt = Date.now();
      await page.evaluate(async () => {
        for (const id of ['btn-poll', 'btn-start', 'btn-stop']) document.getElementById(id).click();
        await fetch('/mock/kick');
      });
      await page.waitForSelector('body.offline', { timeout: 3000 }).catch(() => {});
      await sleep(200);
      const off = await notesOf(page);
      check('control (ZIP-17): a connection closed while two clicks wait to be sent says that 2 were not sent and that the one on its way may not have run; the banner says Streamer.bot offline, and does not blame the sidecar',
        /2 clicks were not sent/.test(off.page || '') && /may not have run/.test(off.page || '') && /Streamer\.bot offline/.test(off.sb || '') && !off.sidecar,
        JSON.stringify(off));
      await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=0`);
      const mark = await page.evaluate(() => window.__sentFrames.length);
      await page.waitForSelector('body:not(.offline)', { timeout: 8000 }).catch(() => {});
      await page.waitForFunction((m) => window.__sentFrames.slice(m).some((f) => f.includes('"command":"status"')), mark, { timeout: 3000 }).catch(() => {});
      const after = await page.evaluate((m) => window.__sentFrames.slice(m).map((f) => {
        try { const j = JSON.parse(f); return j.request === 'DoAction' ? `${j.action?.name}:${j.args?.command ?? ''}` : j.request; } catch { return '?'; }
      }), mark);
      check('control (ZIP-17): on reconnect the page sends status (the probe), after the Subscribe; nothing from before the close is sent again',
        after[0] === 'Subscribe' && after.includes('Ticker Command:status') && !after.some((a) => /:(pollNow|start|stop)$/.test(a)), JSON.stringify(after));
      // 3.5 s after the clicks: the page is back (its wait is 2 to 2.5 s) and its probe's
      // heartbeat runs to about 5 s. Neither the clicks that were never sent (or never ran)
      // nor the sidecar note from before the close may make it say the sidecar is silent.
      await sleep(Math.max(0, clickedAt + 3500 - Date.now()));
      const later = await notesOf(page);
      check('control (ZIP-17): 3.5 s after the dropped clicks, with the page connected again, the banner does not blame the sidecar, though it did before the close (it waits for its new probe\'s answer)',
        before.sidecar === true && !later.sidecar && later.sb === '', JSON.stringify({ before, later }));

      // A status probe is not a click: one on its way, or one waiting, at a close is not
      // counted. (1) The probe on its way (Ticker Command silent, so it holds the queue)
      // and a click waiting behind it: "1 click was not sent", and nothing "may not have
      // run". (2) A click on its way and a probe waiting: only the click on its way.
      await sleep(1000); // the reconnect probe has had its echo, and nothing is in flight
      const dropWith = async (order) => {
        await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=1`);
        try {
          await page.evaluate(async (o) => {
            for (const step of o) {
              if (step === 'probe') window.__zipperProbe(); else document.getElementById('btn-poll').click();
            }
            await fetch('/mock/kick');
          }, order);
          await page.waitForSelector('body.offline', { timeout: 3000 }).catch(() => {});
          await sleep(200);
          return (await notesOf(page)).page;
        } finally {
          await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=0`);
          await page.waitForSelector('body:not(.offline)', { timeout: 8000 }).catch(() => {});
          await sleep(1000);
        }
      };
      const probeFirst = await dropWith(['probe', 'click']);
      const clickFirst = await dropWith(['click', 'probe']);
      check('control (row 6.2): a status probe is not counted as a click at a close: with the probe on its way and one click waiting, the page says 1 click was not sent and nothing about one that may not have run; with a click on its way and the probe waiting, it says only that the last click may not have run',
        probeFirst === '1 click was not sent: the connection to Streamer.bot closed first.'
          && clickFirst === 'The last click may not have run: the connection to Streamer.bot closed while it was on its way.',
        JSON.stringify({ probeFirst, clickFirst }));
    } finally {
      await mockSwitch(`/mock/silent?name=${enc('Ticker Command')}&on=0`);
    }
    await closePage(p);
  }

  // The heartbeat (row 6.2): a close clears it. A click's echo arms it (3 s); the page
  // kicks its own connection the moment that echo comes; it is back 2 s later (the random
  // part of the wait is 0 here), and its new probe gets no answer for 4 s (the mock's
  // delay switch), so no echo and no "did not answer" comes before about 4.3 s after the
  // click's echo. Read 3.5 s after it, the banner must not blame the sidecar: a heartbeat
  // left running from before the close would have set the sidecar note at 3 s.
  {
    const init = () => {
      const realSetInterval = window.setInterval;
      window.setInterval = (fn, ms, ...rest) => (ms === 10000 ? 0 : realSetInterval(fn, ms, ...rest));
      Math.random = () => 0;
    };
    const p = await openPage('heartbeat control page', { width: 700, height: 900 }, controlUrl, { init });
    const { page } = p;
    let r = null;
    try {
      await page.waitForSelector('body:not(.offline)', { timeout: 8000 });
      await page.waitForSelector('body.no-sidecar', { timeout: 6000 }).catch(() => {}); // the connect probe's heartbeat has run out
      await page.evaluate(() => {
        window.__echoAt = null;
        window.addEventListener('ticker:command', (e) => {
          if (window.__echoAt !== null || e.detail?.command !== 'pollNow') return;
          window.__echoAt = performance.now();
          fetch('/mock/kick');
        });
        document.getElementById('btn-poll').click();
      });
      await page.waitForSelector('body.offline', { timeout: 3000 }).catch(() => {});
      await mockSwitch(`/mock/delay?name=${enc('Ticker Command')}&ms=4000`);
      const handle = await page.waitForFunction(() => {
        if (window.__echoAt === null || performance.now() < window.__echoAt + 3500) return false;
        const side = document.getElementById('sidecar-note');
        return {
          afterEchoMs: Math.round(performance.now() - window.__echoAt),
          connected: !document.body.classList.contains('offline'),
          sidecar: !!side && getComputedStyle(side).display !== 'none',
          sb: document.getElementById('sb-note')?.textContent ?? null,
        };
      }, null, { timeout: 8000, polling: 50 }).catch(() => null);
      r = handle ? await handle.jsonValue() : null;
    } finally {
      await mockSwitch(`/mock/delay?name=${enc('Ticker Command')}&ms=0`);
    }
    check('control (row 6.2): a close clears the heartbeat a click\'s echo armed: 3.5 s after that echo, connected again and with its new probe not yet answered, the banner does not say the sidecar is silent',
      !!r && r.connected === true && r.afterEchoMs < 4200 && r.sidecar === false && r.sb === '', JSON.stringify(r));
    await closePage(p);
  }

  // The page's send queue (decision 13): under the mock's bleed mode, the page's status
  // probe and an Apply sent together each arrive with their own arguments.
  {
    // Its own 10 s probe is held back, so only the test's two requests are counted.
    const p = await openPage('bleed control page', { width: 700, height: 900 }, controlUrl, { init: noProbeInterval });
    const { page } = p;
    const bus = await busWatch();
    let cmds = [];
    try {
      await page.waitForSelector('body:not(.offline)', { timeout: 8000 });
      await sleep(1000);
      await mockSwitch('/mock/bleed?on=1');
      const from = bus.seen.length;
      await page.evaluate(() => { window.__ZIPPER_SEND_COMMAND('status'); document.getElementById('btn-opts').click(); });
      await waitUntil(() => bus.commands(from).length >= 2, 4000);
      await sleep(300); // a bled repeat would be in by now
      cmds = bus.commands(from);
    } finally {
      await mockSwitch('/mock/bleed?on=0');
      bus.close();
    }
    const opts = (() => { try { return JSON.parse(cmds[1]?.value); } catch { return null; } })();
    check('control (decision 13): under the mock\'s bleed mode, the page\'s status probe and an Apply sent together each run once, with their own arguments',
      cmds.length === 2 && cmds[0].command === 'status' && cmds[0].value === '' && cmds[1].command === 'setOptions' && opts?.intervalMs === 30000 && opts?.maxItems === 20,
      JSON.stringify(cmds));
    await closePage(p);
  }

  // Escaping (ZIP-18, row 6.3): both scores of an in-progress match in the preview are
  // drawn as text.
  {
    const p = await openPage('escape control page', { width: 700, height: 900 }, controlUrl);
    const { page } = p;
    let r = null;
    try {
      await page.waitForSelector('#preview .m', { timeout: 8000 });
      const payload = {
        type: 'ticker:update', generatedAt: new Date().toISOString(),
        tournament: { name: 'Escape check', url: null, state: 'in_progress' },
        matches: [{ id: 'e1', round: 'Round 1', state: 'in_progress', p1: { name: 'Ann', score: '<img alt="zipper-score-1">' }, p2: { name: 'Bo', score: '<img alt="zipper-score-2">' }, winner: 0 }],
        standings: [], caps: { text: '', logo: '' },
      };
      await fetch(`${HTTP}/mock/push`, { method: 'POST', body: JSON.stringify(payload) });
      await page.waitForFunction(() => document.getElementById('preview').textContent.includes('Escape check'), null, { timeout: 5000 }).catch(() => {});
      r = await page.evaluate(() => ({ imgs: document.querySelectorAll('#preview img').length, text: document.getElementById('preview').textContent }));
    } finally {
      await fetch(`${HTTP}/mock/push`, { method: 'POST', body: sampleText });
    }
    check('control (ZIP-18): both scores of an in-progress match are drawn as text: a score of <img …> makes no element',
      !!r && r.imgs === 0 && r.text.includes('<img alt="zipper-score-1">') && r.text.includes('<img alt="zipper-score-2">'), JSON.stringify(r));
    await closePage(p);
  }

  // A refused ?sbport (the owner's ruling of 2026-11-26): a value that is not a port from
  // 1 to 65535 dials nothing, and the banner says so, in place of "Streamer.bot offline".
  // This page's WebSocket is a stand-in that records each address it is given and never
  // connects, so the page reaches no Streamer.bot even if panel-core dialled one. The
  // refused value carries markup, which the page must neither draw nor repeat.
  {
    const init = () => {
      window.__dialled = [];
      window.__connEvents = [];
      window.addEventListener('sb:connection', (e) => window.__connEvents.push(e.detail));
      class NoSocket {
        constructor(url) { window.__dialled.push(String(url)); this.readyState = 0; }
        send() { throw new Error('the stand-in socket never connects'); }
        close() {}
      }
      NoSocket.CONNECTING = 0; NoSocket.OPEN = 1; NoSocket.CLOSING = 2; NoSocket.CLOSED = 3;
      window.WebSocket = NoSocket;
    };
    const p = await openPage('refused-port control page', { width: 700, height: 900 }, `${HTTP}/zipper-shared/control.html?sbport=${enc('<img alt="zipper-port">')}`, { init });
    const { page } = p;
    await page.waitForFunction(() => (document.getElementById('sb-note')?.textContent || '') !== '', null, { timeout: 4000 }).catch(() => {});
    await sleep(2600); // past the 2 to 2.5 s a closed socket waits before it dials again
    const r = await page.evaluate(() => {
      const sb = document.getElementById('sb-note');
      document.getElementById('btn-poll').click();
      return {
        dialled: window.__dialled, events: window.__connEvents,
        sb: sb.textContent, sbShown: getComputedStyle(sb).display !== 'none',
        offline: document.body.classList.contains('offline'), flag: window.__ZIPPER_SB_PORT_REFUSED ?? null,
        doAction: window.__ZIPPER_DO_ACTION('Ticker Command', { command: 'status', value: '' }),
        clickNote: document.getElementById('page-note').textContent,
        imgs: document.querySelectorAll('img').length, repeated: document.body.innerHTML.includes('zipper-port'),
      };
    });
    check('control (the ruling of 2026-11-26): with a refused ?sbport the page dials nothing (its WebSocket is a stand-in here), is marked offline, and the banner says the port in its address was refused, not "Streamer.bot offline"; panel-core raised sb:connection once, with reason sbport-refused; a click is not sent; the refused value (markup) is neither drawn nor repeated',
      r.dialled.length === 0 && r.events.length === 1 && r.events[0]?.connected === false && r.events[0]?.reason === 'sbport-refused'
        && r.sb === 'The port in this page\'s address was refused, so the page did not connect. Fix ?sbport (a number from 1 to 65535) and reload.'
        && r.sbShown && r.offline && r.flag === true && r.doAction === false
        && r.clickNote === 'Not connected to Streamer.bot: that click was not sent.' && r.imgs === 0 && !r.repeated,
      JSON.stringify(r));
    await closePage(p);
  }

  // The page's two ways of knowing the port was refused, each alone (the finding of
  // 2026-11-26): panel-core's flag, read as the page starts, and sb:connection's reason.
  // On the page above both come through. Here one page hides the flag (it reads
  // undefined; what panel-core sets is kept aside), so only the event can tell it. The
  // other keeps that event from the page's own listener (a listener added before it
  // stops it), so only the flag can. Each must still show the refused line.
  for (const variant of ['event only', 'flag only']) {
    const init = (v) => {
      window.__dialled = [];
      window.__swallowed = 0;
      class NoSocket {
        constructor(url) { window.__dialled.push(String(url)); this.readyState = 0; }
        send() { throw new Error('the stand-in socket never connects'); }
        close() {}
      }
      NoSocket.CONNECTING = 0; NoSocket.OPEN = 1; NoSocket.CLOSING = 2; NoSocket.CLOSED = 3;
      window.WebSocket = NoSocket;
      if (v === 'event only') {
        Object.defineProperty(window, '__ZIPPER_SB_PORT_REFUSED', {
          configurable: true, get: () => undefined, set: (x) => { window.__flagSet = x; },
        });
      } else {
        window.addEventListener('sb:connection', (e) => {
          if (e.detail && e.detail.reason === 'sbport-refused') { window.__swallowed++; e.stopImmediatePropagation(); }
        });
      }
    };
    const p = await openPage(`refused-port control page (${variant})`, { width: 700, height: 900 }, `${HTTP}/zipper-shared/control.html?sbport=abc`, { init, initArg: variant });
    const { page } = p;
    await page.waitForFunction(() => document.readyState === 'complete', null, { timeout: 4000 }).catch(() => {});
    await sleep(300); // past panel-core's zero timer
    const r = await page.evaluate(() => ({
      dialled: window.__dialled, sb: document.getElementById('sb-note').textContent,
      flagRead: window.__ZIPPER_SB_PORT_REFUSED ?? null, flagSet: window.__flagSet ?? null, swallowed: window.__swallowed,
    }));
    const line = 'The port in this page\'s address was refused, so the page did not connect. Fix ?sbport (a number from 1 to 65535) and reload.';
    if (variant === 'event only') {
      check('control (the ruling of 2026-11-26): with panel-core\'s flag hidden from the page, sb:connection\'s reason sbport-refused alone still makes the banner say the port was refused (not "Streamer.bot offline"); nothing is dialled',
        r.flagRead === null && r.flagSet === true && r.sb === line && r.dialled.length === 0, JSON.stringify(r));
    } else {
      check('control (the ruling of 2026-11-26): with panel-core\'s refused sb:connection kept from the page\'s listener, its flag __ZIPPER_SB_PORT_REFUSED alone still makes the banner say the port was refused; nothing is dialled',
        r.swallowed === 1 && r.flagRead === true && r.sb === line && r.dialled.length === 0, JSON.stringify(r));
    }
    await closePage(p);
  }

  // An older cached panel-core.js (S2): the copy at 3a8efef (Release 1.0.24) is served in
  // place of the current one, to this page only (openPage's route; the mock and the disk
  // keep the current file). It sets no __ZIPPER_PANEL_CORE, raises no sb:* event, and its
  // send functions return true, not a request id. The page must say an older copy is
  // loaded, throw nothing (closePage checks), and still show the connection the old core
  // reports through body.offline. It dials the mock (?sbport is the mock's port).
  {
    const old = spawnSync('git', ['show', '3a8efef:zipper-shared/panel-core.js'], { cwd: __dirname, encoding: 'utf8' });
    if (old.status !== 0 || !old.stdout) {
      check('control (S2): the 3a8efef panel-core.js can be read with git show', false, String(old.stderr || old.error?.message || '').trim());
    } else {
      const p = await openPage('old panel-core control page', { width: 700, height: 900 }, controlUrl, { route: { path: '/zipper-shared/panel-core.js', body: old.stdout } });
      const { page } = p;
      let r = null;
      try {
        const connected = await page.waitForSelector('body:not(.offline)', { timeout: 8000 }).then(() => true, () => false);
        const sent = await page.evaluate(() => window.__ZIPPER_SEND_COMMAND('status'));
        await page.click('#btn-poll'); // the old send returns true, not an id: the page must cope
        await sleep(1500);
        r = { connected, sent, ...(await page.evaluate(() => {
          const sb = document.getElementById('sb-note');
          return {
            marker: window.__ZIPPER_PANEL_CORE ?? null, sb: sb.textContent, sbShown: getComputedStyle(sb).display !== 'none',
            offline: document.body.classList.contains('offline'),
          };
        })) };
      } catch (err) {
        r = { error: String(err && err.message) };
      }
      check('control (S2): with the 3a8efef panel-core.js (no __ZIPPER_PANEL_CORE; its send returns true) the banner says an older copy is loaded and to reload the dock or restart OBS; the page throws nothing, and shows the connection the old core reports (body.offline off once it connects)',
        !!r && r.connected === true && r.sent === true && r.marker === null
          && r.sb === 'An older copy of panel-core.js is loaded. Reload this dock; if that does not help, restart OBS.' && r.sbShown && r.offline === false,
        JSON.stringify(r));
      await closePage(p);

      // With that core every send gives true, not a request id (the finding of
      // 2026-11-26). The page keeps each request under an id of its own: an
      // announcement's box still clears when its echo comes after another click, and one
      // that never comes back can be sent again once its wait is over (800 ms, then
      // 1.5 s). A second click while it is still on its way sends nothing, as with the
      // current core. The 10 s probe is held back, and every frame the page sends is
      // recorded.
      const echoInit = () => {
        const realSetInterval = window.setInterval;
        window.setInterval = (fn, ms, ...rest) => (ms === 10000 ? 0 : realSetInterval(fn, ms, ...rest));
        window.__sentFrames = [];
        const realSend = WebSocket.prototype.send;
        WebSocket.prototype.send = function (data) {
          try { window.__sentFrames.push(String(data)); } catch {}
          return realSend.call(this, data);
        };
      };
      const q = await openPage('old panel-core echo control page', { width: 700, height: 900 }, controlUrl, { init: echoInit, route: { path: '/zipper-shared/panel-core.js', body: old.stdout } });
      const announced = (text) => q.page.evaluate((t) => window.__sentFrames.filter((f) => {
        try { const j = JSON.parse(f); return j.request === 'DoAction' && j.action?.name === 'Ticker Announce' && j.args?.text === t; } catch { return false; }
      }).length, text);
      const box = () => q.page.evaluate(() => document.getElementById('ann-text').value);
      let e = null;
      try {
        const connected = await q.page.waitForSelector('body:not(.offline)', { timeout: 8000 }).then(() => true, () => false);
        // An announcement, then Poll at once: the second request must not take the
        // first one's place.
        await q.page.evaluate(() => {
          document.getElementById('ann-text').value = 'Old core echo one';
          document.getElementById('btn-announce').click();
          document.getElementById('btn-poll').click();
        });
        await waitUntil(async () => (await box()) === '', 4000);
        const boxOne = await box();
        // An announcement that never comes back: Ticker Announce is queued and its run
        // broadcasts nothing.
        await mockSwitch(`/mock/silent?name=${enc('Ticker Announce')}&on=1`);
        try {
          await q.page.evaluate(() => {
            document.getElementById('ann-text').value = 'Old core echo two';
            document.getElementById('btn-announce').click();
            document.getElementById('btn-announce').click(); // still on its way: sends nothing
          });
          await sleep(500);
          const early = await announced('Old core echo two');
          await sleep(2600); // past the 800 ms and 1.5 s the page waits for its echo
          // The wait is over, so the page also holds "did not answer": the old-copy
          // line must still be the one shown, because it is the cause to fix first.
          const banner = await q.page.evaluate(() => document.getElementById('sb-note').textContent);
          check('control (S2): with the 3a8efef panel-core.js and an announcement that did not answer, the banner still says an older copy is loaded: that line outranks "did not answer"',
            banner === 'An older copy of panel-core.js is loaded. Reload this dock; if that does not help, restart OBS.', JSON.stringify({ banner }));
          await q.page.evaluate(() => document.getElementById('btn-announce').click());
          await waitUntil(async () => (await announced('Old core echo two')) >= 2, 2000);
          await sleep(300); // nothing more comes
          e = { connected, boxOne, early, later: await announced('Old core echo two'), boxTwo: await box(), one: await announced('Old core echo one') };
        } finally {
          await mockSwitch(`/mock/silent?name=${enc('Ticker Announce')}&on=0`);
        }
      } catch (err) {
        e = { error: String(err && err.message) };
      }
      check('control (S2, the finding of 2026-11-26): with the 3a8efef panel-core.js (each send gives true) the page keeps each request apart: an announcement followed at once by Poll still clears its box when its echo comes; one that never comes back is not sent twice while on its way, and is sent again once its wait is over',
        !!e && e.connected === true && e.one === 1 && e.boxOne === '' && e.early === 1 && e.later === 2 && e.boxTwo === 'Old core echo two',
        JSON.stringify(e));
      await closePage(q);
    }
  }

  // No panel-core.js at all (the finding of 2026-11-26): the dock's copy did not load (a
  // 404 here, as from a wrong folder). The page must not say an older copy is loaded:
  // only a copy that loaded is named. It says the file did not load. It throws nothing, and a click says it was not
  // sent. The browser logs the 404 itself as a console error. That line, once, is the
  // one problem this page is expected to have.
  {
    const p = await openPage('no panel-core control page', { width: 700, height: 900 }, controlUrl, { init: noProbeInterval, route: { path: '/zipper-shared/panel-core.js', status: 404, body: '/* not found */' } });
    const { page } = p;
    await page.waitForFunction(() => document.readyState === 'complete', null, { timeout: 4000 }).catch(() => {});
    await sleep(300);
    const r = await page.evaluate(() => {
      const sb = document.getElementById('sb-note');
      document.getElementById('btn-poll').click();
      return {
        core: typeof window.__ZIPPER_DO_ACTION, marker: window.__ZIPPER_PANEL_CORE ?? null,
        sb: sb.textContent, sbShown: getComputedStyle(sb).display !== 'none',
        clickNote: document.getElementById('page-note').textContent,
        offline: document.body.classList.contains('offline'),
      };
    });
    const notFound = p.problems.filter((x) => x.startsWith('console error: ') && x.includes('404') && x.includes('/zipper-shared/panel-core.js'));
    p.problems = p.problems.filter((x) => !notFound.includes(x));
    // The owner's ruling of 2026-11-26: the page starts offline (a red dot, not a green
    // one) and says the file did not load.
    check('control (S2): with no panel-core.js loaded (it answers 404) the page is offline (red dot) and the banner says panel-core.js did not load, not that an older copy is loaded; a click says it was not sent; the browser logged that 404 once',
      r.core === 'undefined' && r.marker === null && r.offline === true && r.sbShown === true
        && r.sb === 'panel-core.js did not load, so this page cannot connect. Check that the zipper-shared folder is served, then reload this dock.'
        && r.clickNote === 'Not connected to Streamer.bot: that click was not sent.' && notFound.length === 1,
      JSON.stringify({ ...r, notFound }));
    await closePage(p);
  }
} catch (err) {
  fail++;
  console.error(`\nFATAL: ${err.stack || err}`);
} finally {
  try { await browser?.close(); } catch {}
  stopMock();
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : 'FAILURES'} — ${pass} passed, ${fail} failed (screenshots: test-render-1920.png, test-render-640.png, test-render-message.png, control-render.png)`);
process.exit(fail === 0 ? 0 : 1);
