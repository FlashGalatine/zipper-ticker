// Real-pixel render verification: drives the overlay + control page as they ship, with
// a few test-only changes, each named where it is used. The out-of-order pages get a
// stand-in clock (skewClock). Several control pages have their 10 s status probe held
// back: some with noProbeInterval, some with init scripts of their own that do the same
// and more (one also records every frame the page sends, one also sets the random part
// of its reconnect wait to 0). The refused-port pages get a stand-in WebSocket that never
// connects (one also hides panel-core's flag from the page, one also keeps panel-core's
// sb:connection from the page's own listener). Two pages are served the older
// panel-core.js of 3a8efef, and one a 404 in its place (openPage's route). The overlay
// pages get more, each named where it is used. The message page and the out-of-order
// overlay page get crawlDriver, which pauses the crawl and moves a held crawl through the
// wrap, so their checks do not hang on FADE_THRESHOLD_MS. Most Sprint 7 pages get
// crawlProbe, which records each change to #track and lets the test pause the crawl and
// set its currentTime. On top of that: a stand-in WebSocket that never connects (the
// refused-port overlay page); no ResizeObserver (the no-observer page); style rules of
// their own (the scaled, placed-seg and long-fade pages); a counter of the img src the
// page sets, with skewClock (the logo page); a late web font (fontCss, the late-font
// pages), with the first loop paused (pauseFirstLoop) on all but the first of them, and an
// announcement on the first update (announceFirst) or the moment the re-measure's fade
// starts (announceOnFade). Routes serve three test logos (one 1.5 s late, one that fails
// once) and a system font file 1.5 s late, and the wrap-swap page has every request
// beyond 127.0.0.1 aborted. It runs in a real Chromium (system Edge/Chrome via
// playwright-core; nothing bundled) against the mock SB server, injects the sample
// fixture, and asserts the DOM.
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
// Lets a check bring new crawl content into an overlay page whatever FADE_THRESHOLD_MS is
// (the message page and the out-of-order overlay page). The overlay brings a new crawl in
// under the fade when the loop's wrap is further off than the threshold, and holds it for
// the wrap otherwise. pauseAt(ms) pauses the loop ms into it, so the wrap is a known time
// away, and gives how many ticker:update events have come. For each update it records how
// far the wrap was and the way ticker.js took it: 'fade', 'held' (for the wrap) or 'at
// once'. throughWrap() moves a held crawl through the wrap and the loop's end by setting
// the paused loop's currentTime, as the Sprint 7 checks do. Only the pages opened with it
// get it.
function crawlDriver() {
  const T = () => document.getElementById('track');
  const anim = () => { const t = T(); return t && typeof t.getAnimations === 'function' ? (t.getAnimations().find((x) => x.animationName === 'ticker-scroll') || null) : null; };
  const frame = () => new Promise((r) => requestAnimationFrame(() => r()));
  const seen = [];
  window.addEventListener('ticker:update', () => {
    const t = T();
    const a = anim();
    const tm = a && a.effect ? a.effect.getComputedTiming() : null;
    const d = tm ? Number(tm.duration) : NaN;
    const rec = { toWrap: d > 0 ? (1 - (tm.progress == null ? 1 : Number(tm.progress))) * d : null, path: null };
    const before = t ? t.innerHTML : '';
    seen.push(rec);
    // This listener was added before ticker.js's, so it runs first: what ticker.js did is
    // read once its listener has run.
    setTimeout(() => {
      const now = T();
      rec.path = now.classList.contains('swap-fade') ? 'fade' : now.innerHTML === before ? 'held' : 'at once';
    }, 0);
  });
  window.__drive = {
    updates: () => seen.length,
    taken: (k) => (seen[k] && seen[k].path ? { ...seen[k] } : null),
    async pauseAt(ms) {
      const a = anim();
      if (a) { a.pause(); a.currentTime = ms; await frame(); }
      return seen.length;
    },
    async throughWrap() {
      let a = anim();
      if (!a) return false;
      const tm = a.effect.getComputedTiming();
      const k = Number(tm.currentIteration) || 0;
      a.pause(); a.currentTime = (k + 1) * Number(tm.duration) + 1; // the wrap: the copies off screen take it
      await frame(); await frame();
      a = anim();
      const end = a && a.effect ? Number(a.effect.getComputedTiming().endTime) : NaN;
      if (end > 0 && end < Infinity) { a.pause(); a.currentTime = end + 1; await frame(); await frame(); } // the loop's end: every copy takes it
      return true;
    },
  };
}
// route, when given, serves one file the page asks for from memory (route.path on this
// server, with route.body, and route.status or 200), in this page's browser only: the mock
// and the disk keep theirs. init may be one function or several. initArg, when given, is
// handed to each. setup, when given, is
// called with the page before it loads (for routes and listeners of its own).
async function openPage(label, viewport, url, { init, initArg, route, setup } = {}) {
  const page = await browser.newPage({ viewport });
  for (const f of [].concat(init || [])) await page.addInitScript(f, initArg);
  if (route) await page.route((u) => u.origin === HTTP && u.pathname === route.path, (r) => r.fulfill({ status: route.status ?? 200, contentType: 'application/javascript', body: route.body }));
  if (setup) await setup(page);
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

// ticker.js's own figures, read from its source, so the crawl checks follow the figures the
// owner sets. FADE_THRESHOLD_MS: new content waits for the wrap when it is this near, and
// comes in under the fade when it is further off. NEAR_MS: how far before the wrap the held
// cases send new content, inside the threshold whatever it is.
const tickerSrc = await readFile(resolve(__dirname, 'overlay', 'ticker.js'), 'utf-8');
const constOf = (name) => { const m = new RegExp(`const ${name} = (\\d+);`).exec(tickerSrc); return m ? Number(m[1]) : NaN; };
const FADE_THRESHOLD_MS = constOf('FADE_THRESHOLD_MS');
const LOGO_RETRY_MS = constOf('LOGO_RETRY_MS');
const NEAR_MS = Math.min(5000, Math.floor(FADE_THRESHOLD_MS / 2));

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
    // The loop running before the announcement, kept to compare with the one after it. It is
    // moved 3 s on (and left running), so the place it pauses at is not its start.
    await page.evaluate(() => {
      const a = document.getElementById('track').getAnimations().find((x) => x.animationName === 'ticker-scroll') || null;
      if (a) a.currentTime = 3000;
      window.__annLoop = a;
    });
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
    const loopAt = () => page.evaluate(() => {
      const a = document.getElementById('track').getAnimations().find((x) => x.animationName === 'ticker-scroll') || null;
      return { same: !!a && a === window.__annLoop, t: a ? Number(a.currentTime) : null };
    });
    const pausedLoop = await loopAt();
    await page.waitForSelector('.zipper:not(.announcing)', { timeout: 8000 });
    const resumed = await page.locator('.track').evaluate((el) => getComputedStyle(el).animationPlayState);
    check('announce: crawl resumes after duration', resumed === 'running', resumed);
    const resumedLoop = await loopAt();
    check('announce: with nothing new, the crawl resumes where it paused: the loop that ran before the announcement runs on after it, from the time it was paused at, not from the start of a new loop',
      pausedLoop.same === true && resumedLoop.same === true && pausedLoop.t >= 3000 && resumedLoop.t >= pausedLoop.t,
      JSON.stringify({ pausedLoop, resumedLoop }));
    const items = await page.locator('.track .seg').first().locator('.item').count();
    check('announce: results intact after takeover', items >= 4, String(items));
    await closePage(p);
  }

  // Persistent message mode: the configured lines crawl instead of the results. The overlay
  // brings the message in, and the results back, as it does any new crawl: under the fade
  // when the loop's wrap is further off than FADE_THRESHOLD_MS, and at the wrap otherwise.
  // Before each change the loop is paused 1 s in (crawlDriver), so the threshold says which
  // way it comes, and a crawl held for the wrap is moved through it.
  {
    const p = await openPage('message page', { width: 1920, height: 72 }, `${HTTP}/zipper-overlay/ticker.html?w=1920&sbport=${WS_PORT}`, { init: crawlDriver });
    const { page } = p;
    await page.waitForSelector('.track .seg .item', { timeout: 8000 });

    // Pushes body through the mock; gives the way ticker.js took it, and the way the
    // threshold says it takes it.
    const bringIn = async (body) => {
      const n = await page.evaluate(() => window.__drive.pauseAt(1000));
      await fetch(`${HTTP}/mock/push`, { method: 'POST', body });
      const got = await waitUntil(() => page.evaluate((k) => window.__drive.taken(k), n), 5000);
      if (got && got.path === 'held') await page.evaluate(() => window.__drive.throughWrap());
      return { ...(got || { path: '(no update)', toWrap: null }), expected: got && got.toWrap !== null ? (got.toWrap > FADE_THRESHOLD_MS ? 'fade' : 'held') : null };
    };
    const firstCopyHas = (sel) => page.waitForFunction((s) => {
      const t = document.getElementById('track');
      const seg = t.querySelector('.seg');
      return !!seg && !!seg.querySelector(s) && !t.classList.contains('swap-fade');
    }, sel, { timeout: 5000 }).then(() => true, () => false);

    const sample = JSON.parse(await readFile(resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8'));
    const lines = ['Bracket starts at 8pm ET', '!discord for the lobby code'];
    // Results stay in the payload — the message is expected to cover them.
    const on = await bringIn(JSON.stringify({ ...sample, message: { lines } }));
    const onShown = await firstCopyHas('.item.message');
    await sleep(400); // the fade back in, before the pixel grab

    // One item per line, repeated whole until the segment fills the strip (two
    // short lines don't span 1920px), so the loop, which wraps at one copy's width
    // (--seg-w), stays seamless.
    const msgs = await page.locator('.track .seg').first().locator('.item.message').count();
    check('message: one item per line, repeated to fill the strip',
      msgs >= lines.length && msgs % lines.length === 0, String(msgs));
    const first = await page.evaluate(() => { const el = document.querySelector('.track .seg .item.message .msg'); return el ? el.textContent : null; });
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

    // Dropping the message brings results back with no re-poll, and leaves the
    // mock's stored payload clean for the control-page checks below.
    const off = await bringIn(JSON.stringify(sample));
    const offShown = await firstCopyHas('.item .name');
    const msgsAfter = await page.locator('.item.message').count();
    check('message: results return when the message drops', offShown && msgsAfter === 0, JSON.stringify({ offShown, msgsAfter }));
    check(`message: the message comes in, and the results go back, the way new results do: under the fade when the wrap is further off than FADE_THRESHOLD_MS (${FADE_THRESHOLD_MS} ms), held for the wrap otherwise (here the loop was paused 1 s in, and a held crawl moved through the wrap)`,
      onShown && on.path === on.expected && off.path === off.expected, JSON.stringify({ FADE_THRESHOLD_MS, on, onShown, off, offShown }));
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
    const ov = await openPage('out-of-order overlay page', { width: 1920, height: 72 }, `${HTTP}/zipper-overlay/ticker.html?w=1920&sbport=${WS_PORT}`, { init: [skewClock, crawlDriver] });
    // A payload the pages are to apply: the overlay brings it in under the fade, or holds it
    // for the wrap when that is FADE_THRESHOLD_MS or less away. So before the push its loop
    // is paused 1 s in, and a crawl held for the wrap is moved through it (crawlDriver):
    // these checks follow the stamp rule whatever the threshold is.
    const pushNew = async (body) => {
      const n = await ov.page.evaluate(() => window.__drive.pauseAt(1000));
      await push(body);
      const got = await waitUntil(() => ov.page.evaluate((k) => window.__drive.taken(k), n), 5000);
      if (got && got.path === 'held') await ov.page.evaluate(() => window.__drive.throughWrap());
      return got;
    };
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
      await ov.page.waitForFunction((n) => document.querySelector('.track').textContent.includes(n), name, { timeout: 5000, polling: 50 }).catch(() => {});
      await ctl.page.waitForFunction((n) => document.getElementById('preview').textContent.includes(n), name, { timeout: 5000, polling: 50 }).catch(() => {});
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

    await pushNew(named('2026-07-11T20:20:00Z', 'NewerName'));
    const newer = await waitBoth('NewerName');
    await push(named('2026-07-11T20:10:00Z', 'OlderName'));
    await marker();
    const kept = await shows('NewerName');
    const older = await shows('OlderName');
    check('out of order: after a newer payload, a broadcast of an older one (by generatedAt) is ignored: the overlay and the control preview both keep the newer one on screen',
      newer.overlay && newer.preview && kept.overlay && kept.preview && !older.overlay && !older.preview, JSON.stringify({ newer, kept, older }));
    await pushNew(named('2026-07-11T20:20:00Z', 'EqualName'));
    const equal = await waitBoth('EqualName');
    check('out of order: a payload with the same stamp and other content is applied on both pages', equal.overlay && equal.preview, JSON.stringify(equal));
    // Past the window: the older one is applied, and the newest is reset to its stamp.
    await skew(5001);
    await pushNew(named('2026-07-11T20:10:00Z', 'LateOlderName'));
    const late = await waitBoth('LateOlderName');
    await push(named('2026-07-11T20:05:00Z', 'BelowResetName'));
    await marker();
    const belowKept = await shows('LateOlderName');
    const below = await shows('BelowResetName');
    await pushNew(named('2026-07-11T20:15:00Z', 'AboveResetName'));
    const above = await waitBoth('AboveResetName');
    check('out of order: an older payload that arrives more than 5 s after a page applied its newest is applied on both pages; the newest is reset to its stamp, so one older than that, just after, is ignored, and one newer is applied',
      late.overlay && late.preview && belowKept.overlay && belowKept.preview && !below.overlay && !below.preview && above.overlay && above.preview,
      JSON.stringify({ late, belowKept, below, above }));
    // A far-future stamp ("9999-…") freezes neither page past the window.
    await pushNew(named('9999-12-31T23:59:59Z', 'FarFutureName'));
    const far = await waitBoth('FarFutureName');
    await push(named('2026-07-11T20:16:00Z', 'FrozenName'));
    await marker();
    const farKept = await shows('FarFutureName');
    const frozen = await shows('FrozenName');
    await skew(5001);
    await pushNew(named('2026-07-11T20:16:00Z', 'ThawedName'));
    const thawed = await waitBoth('ThawedName');
    check('out of order: a far-future stamp ("9999-…") does not freeze the pages: an ordinary payload after it is ignored within the 5 s, and applied on both pages once they have passed',
      far.overlay && far.preview && farKept.overlay && farKept.preview && !frozen.overlay && !frozen.preview && thawed.overlay && thawed.preview,
      JSON.stringify({ far, farKept, frozen, thawed }));
    // Every newer payload starts the window again, as each of the sidecar's pushes does in
    // steady use: past the window once more, a newer payload is applied, and an older one
    // 4 s after it is ignored on both pages. The real time the pages took to show the newer
    // one counts toward the 4 s, so a slow fade cannot carry the older one past the window.
    await skew(6000);
    const restartSent = Date.now();
    await pushNew(named('2026-07-11T20:17:00Z', 'RestartName'));
    const restart = await waitBoth('RestartName');
    await skew(Math.max(0, 4000 - (Date.now() - restartSent)));
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

  // ── Sprint 7: the crawl does not jump (ZIP-04, decisions 4, 8 and 14), and the overlay's
  // rendering details (ZIP-19). Time is driven, not waited for: the page's ticker-scroll
  // animation is paused through its own Animation object and its currentTime is set
  // (seeking fires animationiteration and animationend in the system browser), and
  // ticker:update events are dispatched in the page; one case comes through the mock's
  // push path and one through its connect-sync replay. Each of these pages also records,
  // after every change to #track (a MutationObserver, so in the same task as the change,
  // before any frame), which ticker-scroll animation runs, how far in it is, its duration
  // and the loop's end (--seg-w), and each start of the fade (crawlProbe). Not every frame:
  // headless Edge runs requestAnimationFrame only 1 to 3 times a second while a composited
  // transform animation runs, so a frame-by-frame record misses most of what happens; the
  // two-frame waits in seek() take a second or two for the same reason. The fade threshold is
  // ticker.js's FADE_THRESHOLD_MS, read from its source: the held cases are NEAR_MS (5 s, or
  // half the threshold when that is less than 10 s) from the wrap, and the cases that must
  // take the fade (or must not, with no change) run a loop of about 190 s or more (?speed=10) and
  // check that it is far longer than the threshold.
  {
    const sample = JSON.parse(sampleText);
    // A crawl other than the sample, as wide as `extra` more matches make it.
    const crawlOf = (tag, extra = 1, over = {}) => ({
      ...sample, generatedAt: '2026-07-11T20:16:00Z',
      matches: [
        ...Array.from({ length: extra }, (_, i) => ({ id: `s7-${tag}-${i}`, round: 'Grand Final', state: 'completed',
          p1: { name: `${tag} Player ${i + 1}`, score: 3 }, p2: { name: `${tag} Rival ${i + 1}`, score: 1 }, winner: 1 })),
        ...sample.matches,
      ],
      ...over,
    });
    const blankOf = (name) => ({ ...sample, tournament: { ...sample.tournament, name }, matches: [], standings: [] });
    const crawlProbe = () => {
      const c = { updates: 0, frames: [], ids: new Map(), next: 1, fades: 0, pips: 0, fadeAt: null, swapAfter: [] };
      window.__crawl = c;
      window.addEventListener('ticker:update', () => { c.updates++; });
      // Every animationiteration that is not the crawl's own (the LIVE pip's blink), wherever it starts.
      window.addEventListener('animationiteration', (e) => { if (e.animationName !== 'ticker-scroll') c.pips++; }, true);
      const T = () => document.getElementById('track');
      const anim = () => { const t = T(); return t && typeof t.getAnimations === 'function' ? (t.getAnimations().find((x) => x.animationName === 'ticker-scroll') || null) : null; };
      const idOf = (a) => { if (!a) return null; if (!c.ids.has(a)) c.ids.set(a, c.next++); return c.ids.get(a); };
      let fadingWas = false;
      const record = (muts) => {
        const t = T();
        const a = anim();
        if (a) {
          c.frames.push({ id: idOf(a), t: a.currentTime, d: a.effect.getComputedTiming().duration, w: parseFloat(t.style.getPropertyValue('--seg-w')), at: performance.now() });
          if (c.frames.length > 6000) c.frames.splice(0, 2000);
        }
        const fadingNow = t.classList.contains('swap-fade');
        if (fadingNow && !fadingWas) { c.fades++; c.fadeAt = performance.now(); }
        fadingWas = fadingNow;
        // How long after a fade started the copies were replaced (the swap under it).
        if (c.fadeAt !== null && (muts || []).some((m) => m.type === 'childList')) { c.swapAfter.push(performance.now() - c.fadeAt); c.fadeAt = null; }
      };
      // The track exists once the document is parsed; the first crawl comes later, over the socket.
      document.addEventListener('readystatechange', () => {
        const t = T();
        if (!t || t.__s7watched) return;
        t.__s7watched = true;
        new MutationObserver(record).observe(t, { attributes: true, attributeFilter: ['style', 'class'], childList: true });
      });
      const frame = () => new Promise((r) => requestAnimationFrame(() => r()));
      const segs = () => Array.from(T().children).filter((e) => e.classList.contains('seg'));
      window.__zt = {
        id: () => idOf(anim()),
        dur: () => { const a = anim(); return a ? a.effect.getComputedTiming().duration : null; },
        // Pauses the loop ms into its animation, then lets two frames pass for its events.
        async seek(ms) { const a = anim(); if (!a) return false; a.pause(); a.currentTime = ms; await frame(); await frame(); return true; },
        // Lets a paused loop run on in real time.
        async play() { const a = anim(); if (!a) return false; a.play(); await frame(); return true; },
        // Where the loop now running started, as first recorded (in the task that built it):
        // its time, duration and end, the px into it (t / d × w), and when.
        startOf() {
          const id = idOf(anim());
          const f = c.frames.find((x) => x.id === id);
          return f ? { id, t: f.t, d: f.d, w: f.w, px: (f.t / f.d) * f.w, at: f.at } : null;
        },
        // As seek, and gives what the page shows the moment the track's own ticker-scroll
        // event (the wrap, or the loop's end) has been handled: this listener comes after
        // ticker.js's, so it runs right after it, in the same dispatch, before any timer.
        async snapAt(ms) {
          const t = T();
          let snap = null;
          const take = (e) => {
            if (e.target !== t || e.animationName !== 'ticker-scroll' || snap) return;
            snap = { type: e.type, view: this.view(), copies: this.copies(), fading: this.fading(), othersOff: this.othersOff() };
          };
          t.addEventListener('animationiteration', take);
          t.addEventListener('animationend', take);
          try { await this.seek(ms); } finally { t.removeEventListener('animationiteration', take); t.removeEventListener('animationend', take); }
          return snap;
        },
        // As seek, and gives where the track is and what is in view when the loop's end
        // comes, before ticker.js handles it (a capture listener on window runs first): what
        // the browser would draw if a frame came before that handler.
        async snapBeforeEnd(ms) {
          const t = T();
          let snap = null;
          const take = (e) => {
            if (e.target !== t || e.animationName !== 'ticker-scroll' || snap) return;
            snap = { offset: this.offset(), view: this.view() };
          };
          window.addEventListener('animationend', take, true);
          try { await this.seek(ms); } finally { window.removeEventListener('animationend', take, true); }
          return snap;
        },
        offset: () => new DOMMatrix(getComputedStyle(T()).transform).m41,
        segW: () => parseFloat(T().style.getPropertyValue('--seg-w')),
        copies: () => segs().map((e) => e.textContent),
        widths: () => segs().map((e) => e.getBoundingClientRect().width),
        wrapW: () => document.getElementById('track-wrap').clientWidth,
        fading: () => T().classList.contains('swap-fade'),
        // The opacity transition running on the track, as its start and end values.
        fadeTransition() {
          const tr = T().getAnimations().find((x) => x.transitionProperty === 'opacity');
          if (!tr) return null;
          const k = tr.effect.getKeyframes();
          return { from: k[0] && k[0].opacity, to: k[k.length - 1] && k[k.length - 1].opacity };
        },
        loopStyle: () => { const cs = getComputedStyle(T()); return { count: cs.animationIterationCount, fill: cs.animationFillMode }; },
        isStatic: () => T().classList.contains('static'),
        text: () => T().textContent,
        // The items in view, inside the edge fades: their text, and where each starts.
        view() {
          const wr = document.getElementById('track-wrap').getBoundingClientRect();
          const lo = wr.left + 24; const hi = wr.right - 24;
          const out = [];
          for (const it of T().querySelectorAll('.seg .item')) {
            const r = it.getBoundingClientRect();
            if (r.right > lo && r.left < hi) out.push({ text: it.textContent, x: r.left - wr.left });
          }
          return out;
        },
        // Whether the copies after the first start at or past the strip's right edge.
        othersOff() { const s = segs(); return s.length > 1 && s[1].getBoundingClientRect().left >= document.getElementById('track-wrap').getBoundingClientRect().right - 24; },
        // Whether, at the loop's end, the copies after the first cover the strip's middle.
        coveredAtEnd() { const s = segs(); let rest = 0; for (const e of s.slice(1)) rest += e.getBoundingClientRect().width; return rest >= document.getElementById('track-wrap').clientWidth; },
        send(payload) { window.dispatchEvent(new CustomEvent('ticker:update', { detail: payload })); },
        mark() { const s = segs()[0]; s.__s7mark = (s.__s7mark || 0) + 1; return true; },
        marked: () => { const s = segs()[0]; return !!(s && s.__s7mark); },
        // Waits for a ticker-scroll animation other than id, then pauses it 1 ms in.
        async nextAnim(id, ms = 3000) {
          const end = performance.now() + ms;
          while (performance.now() < end) {
            const a = anim();
            if (a && idOf(a) !== id) {
              const first = c.frames.find((f) => f.id === idOf(a));
              a.pause(); a.currentTime = 1; await frame(); await frame();
              return { id: idOf(a), firstT: first ? first.t : null };
            }
            await frame();
          }
          return null;
        },
        frames: () => c.frames,
        fades: () => c.fades,
        pips: () => c.pips,
        swapAfter: () => c.swapAfter.slice(),
        // Each copy's width in the strip's own px (a transform on the strip not counted).
        layoutWidths: () => segs().map((e) => parseFloat(getComputedStyle(e).width)),
        async settle(n = 3) { for (let k = 0; k < n; k++) await frame(); return true; },
        updates: () => c.updates,
      };
    };
    // What the records say about the loop: no ticker-scroll animation changed its duration
    // while it ran, and after every change the loop's end and duration agree with the speed.
    const loopFrames = (frames, speed) => {
      const durs = new Map();
      let changed = 0; let mismatched = 0;
      for (const f of frames) {
        if (!durs.has(f.id)) durs.set(f.id, f.d);
        else if (Math.abs(durs.get(f.id) - f.d) > 0.5) changed++;
        if (!(Math.abs(f.d - (f.w / speed) * 1000) <= 2)) mismatched++;
      }
      return { frames: frames.length, animations: durs.size, changed, mismatched };
    };
    const sameView = (a, b) => Array.isArray(a) && Array.isArray(b) && a.length > 0 && a.length === b.length
      && a.every((v, i) => v.text === b[i].text && Math.abs(v.x - b[i].x) <= 1);
    const overlayUrl = (w = 1920, extra = '') => `${HTTP}/zipper-overlay/ticker.html?w=${w}&sbport=${WS_PORT}${extra}`;
    const zt = (page, fn, arg) => page.evaluate(fn, arg);
    // ticker.js's own figures (FADE_THRESHOLD_MS, LOGO_RETRY_MS) are read from its source at
    // the top, so the cases below follow the figures the owner sets: the held cases (NEAR_MS
    // before the wrap) sit inside the threshold and the fade cases far outside it.
    {
      // Each doc that gives the threshold in seconds gives ticker.js's figure.
      const figures = {};
      for (const f of ['README.md', 'docs/PROTOCOL.md', 'docs/THEMING.md']) {
        const text = (await readFile(resolve(__dirname, f), 'utf-8')).replace(/\s+/g, ' ');
        figures[f] = Array.from(text.matchAll(/(\d+) seconds (?:away )?or less/g), (m) => Number(m[1]));
      }
      check(`crawl: ticker.js names its fade threshold (FADE_THRESHOLD_MS) and its logo retry wait (LOGO_RETRY_MS); the held cases below (${NEAR_MS / 1000} s from the wrap) sit inside the threshold; README, PROTOCOL and THEMING each give the same threshold in seconds`,
        FADE_THRESHOLD_MS > 0 && NEAR_MS > 0 && NEAR_MS < FADE_THRESHOLD_MS && LOGO_RETRY_MS > 0
          && Object.values(figures).every((v) => v.length >= 1 && v.every((s) => s * 1000 === FADE_THRESHOLD_MS)),
        JSON.stringify({ FADE_THRESHOLD_MS, NEAR_MS, LOGO_RETRY_MS, figures }));
    }

    // The overlay with a refused ?sbport (the ruling of 2026-11-26): it dials nothing, is
    // marked offline and keeps its static line. Its WebSocket is a stand-in that never
    // connects, so it reaches no Streamer.bot even if panel-core dialled one.
    {
      const init = () => {
        window.__dialled = [];
        class NoSocket {
          constructor(url) { window.__dialled.push(String(url)); this.readyState = 0; }
          send() { throw new Error('the stand-in socket never connects'); }
          close() {}
        }
        NoSocket.CONNECTING = 0; NoSocket.OPEN = 1; NoSocket.CLOSING = 2; NoSocket.CLOSED = 3;
        window.WebSocket = NoSocket;
      };
      const p = await openPage('refused-port overlay page', { width: 1920, height: 72 }, `${HTTP}/zipper-overlay/ticker.html?w=1920&sbport=abc`, { init });
      await p.page.waitForFunction(() => document.readyState === 'complete', null, { timeout: 4000 }).catch(() => {});
      await sleep(2600); // past the 2 to 2.5 s a closed socket waits before it dials again
      const r = await p.page.evaluate(() => ({
        dialled: window.__dialled, offline: document.body.classList.contains('offline'),
        flag: window.__ZIPPER_SB_PORT_REFUSED ?? null, text: document.getElementById('track').textContent,
      }));
      check('overlay (the ruling of 2026-11-26): with a refused ?sbport the overlay dials nothing (its WebSocket is a stand-in here), is marked offline and keeps its static line',
        r.dialled.length === 0 && r.offline === true && r.flag === true && r.text === 'waiting for results…', JSON.stringify(r));
      await closePage(p);
    }

    // The swap at the wrap (decision 8), through the mock's push path. Outbound requests
    // are blocked on this page, as a guard that the overlay needs no network.
    {
      const outbound = [];
      const setup = (page) => page.route((u) => u.hostname !== '127.0.0.1', (r) => { outbound.push(r.request().url()); return r.abort(); });
      const p = await openPage('wrap-swap overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: crawlProbe, setup });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const d = await zt(page, () => window.__zt.dur());
      const id0 = await zt(page, () => window.__zt.id());
      await zt(page, (t) => window.__zt.seek(t), d - NEAR_MS); // NEAR_MS before the wrap
      const before = await zt(page, () => ({ offset: window.__zt.offset(), view: window.__zt.view(), copies: window.__zt.copies() }));
      const updates = await zt(page, () => window.__zt.updates());
      await fetch(`${HTTP}/mock/push`, { method: 'POST', body: JSON.stringify(crawlOf('WrapNew', 2)) });
      await page.waitForFunction((n) => window.__zt.updates() > n, updates, { timeout: 5000 }).catch(() => {});
      const arrived = await zt(page, () => ({ updates: window.__zt.updates(), offset: window.__zt.offset(), view: window.__zt.view(), copies: window.__zt.copies(), id: window.__zt.id(), fading: window.__zt.fading() }));
      check(`crawl (ZIP-04): a new result that arrives ${NEAR_MS / 1000} s before the wrap, through the mock's push path, does not move the track or change the text in view; it is held: no copy holds it yet, no fade, the same loop runs`,
        d > 10000 && arrived.updates > updates && arrived.offset === before.offset && sameView(arrived.view, before.view)
          && JSON.stringify(arrived.copies) === JSON.stringify(before.copies) && !arrived.copies.join('').includes('WrapNew') && arrived.id === id0 && arrived.fading === false,
        JSON.stringify({ d, updates, arrivedUpdates: arrived.updates, before: before.offset, after: arrived.offset, copies: arrived.copies.map((c) => c.slice(0, 40)), id0, id: arrived.id, fading: arrived.fading }));
      // The LIVE pip blinks every 1.2 s and its animationiteration bubbles to the track:
      // it must not pass for the wrap. Animation events come only with frames, which come
      // seldom here, so the check waits until at least two pip events have come.
      const pip0 = await zt(page, () => ({ pips: window.__zt.pips(), fades: window.__zt.fades() }));
      await page.waitForFunction((n) => window.__zt.pips() >= n + 2, pip0.pips, { timeout: 8000 }).catch(() => {});
      const afterPip = await zt(page, () => ({ copies: window.__zt.copies(), id: window.__zt.id(), pips: window.__zt.pips(), fading: window.__zt.fading(), fades: window.__zt.fades() }));
      check('crawl (decision 8): the LIVE pip\'s blink, whose animationiteration bubbles up to the track, does not pass for the wrap: after two of its events the held result is still in no copy, the same loop runs, and no fade started',
        afterPip.pips >= pip0.pips + 2 && !afterPip.copies.join('').includes('WrapNew') && afterPip.id === id0
          && afterPip.fading === false && afterPip.fades === pip0.fades,
        JSON.stringify({ pip0, afterPip: { ...afterPip, copies: afterPip.copies.map((c) => c.slice(0, 40)) } }));
      await zt(page, (t) => window.__zt.seek(t), d - 1);
      const v1 = await zt(page, () => ({ view: window.__zt.view(), copies: window.__zt.copies() }));
      await zt(page, (t) => window.__zt.seek(t), d + 1); // across the wrap
      const v2 = await zt(page, () => ({ view: window.__zt.view(), copies: window.__zt.copies(), id: window.__zt.id(), othersOff: window.__zt.othersOff(), segW: window.__zt.segW(), widths: window.__zt.widths() }));
      check('crawl (decision 8): at the wrap the text in view does not change, while the copy off screen already holds the new items: the first copy keeps the old ones, the loop runs on and still ends at the old copy\'s width',
        sameView(v1.view, v2.view) && !v1.copies.join('').includes('WrapNew')
          && !v2.copies[0].includes('WrapNew') && v2.copies.slice(1).length >= 1 && v2.copies.slice(1).every((c) => c.includes('WrapNew'))
          && v2.othersOff === true && v2.id === id0 && Math.abs(v2.segW - v2.widths[0]) <= 0.5,
        JSON.stringify({ v1: v1.view.map((v) => [v.text.slice(0, 24), Math.round(v.x)]), v2: v2.view.map((v) => [v.text.slice(0, 24), Math.round(v.x)]), copies: v2.copies.map((c) => c.slice(0, 40)), othersOff: v2.othersOff, id: v2.id, segW: v2.segW, widths: v2.widths }));
      await zt(page, (t) => window.__zt.seek(t), 2 * d - 1);
      const v3 = await zt(page, () => ({ view: window.__zt.view(), offset: window.__zt.offset(), widths: window.__zt.widths() }));
      const atEnd = await zt(page, (t) => window.__zt.snapBeforeEnd(t), 2 * d + 1); // the next wrap
      const next = await zt(page, (id) => window.__zt.nextAnim(id), id0);
      const v4 = await zt(page, () => ({ view: window.__zt.view(), copies: window.__zt.copies(), segW: window.__zt.segW(), widths: window.__zt.widths(), dur: window.__zt.dur(), frames: window.__zt.frames(), loop: window.__zt.loopStyle() }));
      const lf = loopFrames(v4.frames, 80);
      check('crawl (decision 8): at the next wrap every copy holds the new items and the loop starts again at offset 0, in the frame that sets its new end and duration, and repeats for good again; just before it, the loop had moved by the old copy\'s width (not half the track), and at its end it is held there until the overlay handles it, so the copy in view is the new one whichever comes first; the text in view does not change; no loop changed its duration while it ran',
        !!next && next.firstT !== null && next.firstT === 0 && sameView(v3.view, v4.view)
          && Math.abs(v3.offset + (v3.widths[0] - 0.08)) <= 1 && Math.abs(v3.widths[0] - v3.widths[1]) > 50
          && !!atEnd && Math.abs(atEnd.offset + v3.widths[0]) <= 1 && sameView(atEnd.view, v3.view)
          && v4.loop.count === 'infinite' && v4.loop.fill === 'none'
          && v4.copies.every((c) => c.includes('WrapNew')) && Math.abs(v4.segW - v4.widths[0]) <= 0.5
          && Math.abs(v4.dur - (v4.segW / 80) * 1000) <= 2 && lf.changed === 0 && lf.mismatched === 0 && lf.animations >= 2,
        JSON.stringify({ next, atEnd: atEnd && { offset: atEnd.offset, view: atEnd.view.map((v) => [v.text.slice(0, 24), Math.round(v.x)]) }, v3: v3.view.map((v) => [v.text.slice(0, 24), Math.round(v.x)]), v3offset: v3.offset, v3widths: v3.widths, v4: v4.view.map((v) => [v.text.slice(0, 24), Math.round(v.x)]), segW: v4.segW, widths: v4.widths, dur: v4.dur, loop: v4.loop, lf }));
      check('overlay: with outbound requests blocked the page painted and crawled; it asked for nothing beyond 127.0.0.1',
        outbound.length === 0 && v4.copies.length >= 2, JSON.stringify(outbound));
      await closePage(p);
      // The store as it was: the sample.
      await fetch(`${HTTP}/mock/push`, { method: 'POST', body: sampleText });
    }

    // The fade (decisions 4 and 8): a loop far longer than the threshold (?speed=10), and a
    // result that arrives about 190 s before the wrap. Then unchanged content, and caps.
    {
      const p = await openPage('fade overlay page', { width: 1920, height: 72 }, overlayUrl(1920, '&speed=10'), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const d = await zt(page, () => window.__zt.dur());
      const id0 = await zt(page, () => window.__zt.id());
      await zt(page, () => window.__zt.seek(1000));
      const at = await zt(page, (payload) => {
        const before = { offset: window.__zt.offset(), view: window.__zt.view(), copies: window.__zt.copies() };
        window.__zt.send(payload);
        return { before, after: { offset: window.__zt.offset(), view: window.__zt.view(), copies: window.__zt.copies(), fading: window.__zt.fading(), transition: window.__zt.fadeTransition() } };
      }, crawlOf('FadeNew', 3));
      check('crawl (ZIP-04): a new result that arrives far from the wrap (the loop is slowed with ?speed=10, so the wrap is well past the threshold) does not move the track or change the text at the moment it arrives; the track starts to fade (an opacity transition to 0)',
        d - 1000 > FADE_THRESHOLD_MS + 10000 && at.after.offset === at.before.offset && sameView(at.after.view, at.before.view)
          && JSON.stringify(at.after.copies) === JSON.stringify(at.before.copies) && at.after.fading === true
          && !!at.after.transition && String(at.after.transition.to) === '0',
        JSON.stringify({ d, before: at.before.offset, after: at.after.offset, fading: at.after.fading, transition: at.after.transition, copies: at.after.copies.map((c) => c.slice(0, 40)) }));
      await sleep(700); // past the fade's two halves
      const faded = await zt(page, () => ({ copies: window.__zt.copies(), fading: window.__zt.fading(), segW: window.__zt.segW(), widths: window.__zt.widths(), dur: window.__zt.dur(), frames: window.__zt.frames(), fades: window.__zt.fades() }));
      const fadedId = await zt(page, () => window.__zt.id());
      const firstNew = faded.frames.find((f) => f.id === fadedId);
      const lf = loopFrames(faded.frames, 10);
      // The old loop was paused 1 s in, so it stood at 10 px through the fade: the new loop
      // starts there (the owner's ruling of 2026-11-26), as px into it (time ÷ duration × end).
      const startPx = firstNew ? (firstNew.t / firstNew.d) * firstNew.w : NaN;
      check('crawl (decision 4, the owner\'s ruling of 2026-11-26): under the fade the swap is at once: every copy holds the new items, a new loop started with its new end and duration at the place the old one stood (10 px in), the fade is over, and no loop changed its duration while it ran',
        faded.copies.every((c) => c.includes('FadeNew')) && faded.fading === false && faded.fades === 1 && fadedId !== id0
          && !!firstNew && -at.before.offset > 5 && Math.abs(startPx - -at.before.offset) <= 0.5
          && Math.abs(faded.segW - faded.widths[0]) <= 0.5 && Math.abs(faded.dur - (faded.segW / 10) * 1000) <= 2
          && lf.changed === 0 && lf.mismatched === 0,
        JSON.stringify({ copies: faded.copies.map((c) => c.slice(0, 40)), fading: faded.fading, fades: faded.fades, id0, fadedId, firstNew, startPx, before: at.before.offset, segW: faded.segW, widths: faded.widths, dur: faded.dur, lf }));
      // Unchanged content does nothing (decision 14), and a cap change applies at once
      // without touching the crawl.
      await zt(page, () => { window.__zt.mark(); return true; });
      const fades0 = await zt(page, () => window.__zt.fades());
      const same = await zt(page, (payload) => {
        const offset = window.__zt.offset();
        window.__zt.send(JSON.parse(JSON.stringify(payload)));
        return { offsetSame: window.__zt.offset() === offset, fading: window.__zt.fading() };
      }, crawlOf('FadeNew', 3));
      const caps = await zt(page, (payload) => {
        const offset = window.__zt.offset();
        window.__zt.send(payload);
        return { offsetSame: window.__zt.offset() === offset, fading: window.__zt.fading(), cap: document.getElementById('cap-right').textContent };
      }, crawlOf('FadeNew', 3, { caps: { text: '@NewCapText', logo: '' } }));
      await sleep(400);
      const after = await zt(page, () => ({ marked: window.__zt.marked(), id: window.__zt.id(), fades: window.__zt.fades(), copies: window.__zt.copies() }));
      check('crawl (decision 14): an update with the same crawl does nothing to the track (the same copies, the same loop, no fade), and a right-cap change in it shows at once without touching the crawl',
        same.offsetSame && same.fading === false && caps.offsetSame && caps.fading === false && caps.cap === '@NewCapText'
          && after.marked === true && after.id === fadedId && after.fades === fades0 && after.copies.every((c) => c.includes('FadeNew')),
        JSON.stringify({ same, caps, after: { ...after, copies: after.copies.length } }));
      await closePage(p);
    }

    // The fade keeps the crawl's place (the owner's ruling of 2026-11-26): the new loop starts
    // at the offset the old one had reached, modulo the new copy's width, so updates that come
    // often do not keep the end of a long list out of view. A long loop (?speed=10).
    {
      const p = await openPage('fade-place overlay page', { width: 1920, height: 72 }, overlayUrl(1920, '&speed=10'), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const SPEED = 10;
      // How far the page's own clock for the crawl (its frame time, which here moves on only
      // 1 to 3 times a second) may lag a reading by, as px of crawl, plus rounding.
      const LAG_PX = SPEED * 1.2 + 1;
      // Sends payload (the loop as it is: running, or paused) and waits for the fade's swap.
      // before: the loop's id, how far into it the track is (px), its end, when, and the fades
      // so far. after: the new loop's start (startOf), the fades, the copies.
      const fadeIn = async (payload, moveTo = null) => {
        const before = await zt(page, (a) => {
          const r = { id: window.__zt.id(), px: -window.__zt.offset(), w: window.__zt.segW(), at: performance.now(), fades: window.__zt.fades() };
          window.__zt.send(a.payload);
          if (a.moveTo !== null) {
            // The paused loop moves on while the track fades out.
            const an = document.getElementById('track').getAnimations().find((x) => x.animationName === 'ticker-scroll');
            an.currentTime = a.moveTo;
            r.movedPx = -window.__zt.offset();
          }
          return r;
        }, { payload, moveTo });
        await page.waitForFunction((id) => window.__zt.id() !== id && !window.__zt.fading(), before.id, { timeout: 4000, polling: 50 }).catch(() => {});
        const after = await zt(page, () => ({ start: window.__zt.startOf(), fades: window.__zt.fades(), copies: window.__zt.copies() }));
        return { before, after, toWrap: before.w > 0 ? ((before.w - before.px) / SPEED) * 1000 : NaN };
      };
      // (1) A shorter crawl (one message line, repeated to fill the strip) from far into a long
      // running loop (the sample and four more matches, from the static line so at once): the
      // new loop starts at the offset the old one had by the swap (the offset just before plus
      // the crawl in the time the fade took, if the loop moved on), modulo its own width.
      await zt(page, (pl) => { window.__zt.send(pl.blank); window.__zt.send(pl.long); return true; }, { blank: blankOf('Sprint7 Place'), long: crawlOf('PlaceOld', 4) });
      await sleep(300);
      const d0 = await zt(page, () => window.__zt.dur());
      await zt(page, (t) => window.__zt.seek(t), Math.min(d0 - FADE_THRESHOLD_MS - 10000, 300000));
      await zt(page, () => window.__zt.play());
      const one = await fadeIn({ ...sample, message: { lines: ['The fade keeps its place'] } });
      const s1 = one.after.start;
      const took1 = s1 ? s1.at - one.before.at : NaN;
      const want1 = s1 ? (one.before.px + (SPEED * took1) / 1000) % s1.w : NaN;
      check('crawl (the owner\'s ruling of 2026-11-26): a shorter crawl that arrives far from the wrap of a long running loop comes in under one fade, and the new loop starts at the offset the old one had reached by the swap (the offset just before, plus the crawl while the fade ran), modulo the new copy\'s width, give or take the page\'s frame clock',
        one.toWrap > FADE_THRESHOLD_MS + 5000 && !!s1 && s1.id !== one.before.id && one.after.fades === one.before.fades + 1
          && one.before.px > s1.w && want1 > 50 && want1 < s1.w - 50 && Math.abs(s1.px - want1) <= LAG_PX
          && one.after.copies.length >= 2 && one.after.copies.every((c) => c.includes('The fade keeps its place')),
        JSON.stringify({ d0, before: one.before, toWrap: one.toWrap, start: s1, took1, want1, LAG_PX }));
      // (2) The offset is read at the swap, with the track out of sight: the loop is paused, the
      // crawl sent, and the paused loop moved on by half its length while the track fades out.
      await sleep(400);
      const d1 = await zt(page, () => window.__zt.dur());
      await zt(page, (t) => window.__zt.seek(t), 0.1 * d1);
      const two = await fadeIn(crawlOf('ReadAtSwap', 3), 0.6 * d1);
      const s2 = two.after.start;
      const want2 = s2 ? two.before.movedPx % s2.w : NaN;
      check('crawl (the owner\'s ruling of 2026-11-26): the offset the new loop starts at is read at the swap, while the track is faded out: a paused loop moved on during the fade starts the new loop where it was moved to, not where it was when the crawl arrived',
        two.toWrap > FADE_THRESHOLD_MS + 5000 && !!s2 && s2.id !== two.before.id && two.after.fades === two.before.fades + 1
          && Math.abs(two.before.movedPx - two.before.px) > 300 && Math.abs(s2.px - want2) <= 0.5
          && two.after.copies.every((c) => c.includes('ReadAtSwap Player')),
        JSON.stringify({ d1, before: two.before, toWrap: two.toWrap, start: s2, want2 }));
      // (3) Three updates, each far from the wrap of a long loop, with the crawl moved on by
      // 30 s (300 px) before each: each comes in under a fade of its own, and the offset keeps
      // growing (it never goes back to the start of the loop). Time is driven here: a new loop
      // in this headless browser may wait for a frame before it moves at all.
      const runs = [];
      for (const tag of ['GrowOne', 'GrowTwo', 'GrowThree']) {
        await sleep(400);
        const s = await zt(page, () => window.__zt.startOf());
        await zt(page, (t) => window.__zt.seek(t), (s ? s.t : 0) + 30000);
        runs.push(await fadeIn(crawlOf(tag, 3)));
      }
      const each = runs.map((r, i) => {
        const s = r.after.start;
        return { tag: ['GrowOne', 'GrowTwo', 'GrowThree'][i], beforePx: r.before.px, toWrap: r.toWrap, startPx: s ? s.px : null, want: s ? r.before.px % s.w : null, fade: r.after.fades === r.before.fades + 1 && !!s && s.id !== r.before.id };
      });
      check('crawl (the owner\'s ruling of 2026-11-26): three updates, each far from the wrap of a long loop, with the crawl moved on 300 px before each, come in under a fade each, and the offset keeps growing: each new loop starts where the old one had got to, 300 px on from the one before',
        each.every((e) => e.fade && e.toWrap > FADE_THRESHOLD_MS + 5000 && Math.abs(e.startPx - e.want) <= 0.5)
          && Math.abs(each[1].startPx - each[0].startPx - 300) <= 1 && Math.abs(each[2].startPx - each[1].startPx - 300) <= 1
          && each[0].startPx > 500 && runs[2].after.copies.every((c) => c.includes('GrowThree Player')),
        JSON.stringify({ each }));
      await closePage(p);
    }

    // A second page connecting mid-loop: the mock replays the stored payload to every
    // subscriber, so the first overlay gets a payload identical to the one on screen. The
    // loop is slowed (?speed=10) so the wrap is far past the threshold: a replay taken for
    // new content would fade at once, rather than wait for the wrap out of sight of the check.
    {
      const p = await openPage('replay overlay page', { width: 1920, height: 72 }, overlayUrl(1920, '&speed=10'), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      await zt(page, () => window.__zt.seek(4000));
      await zt(page, () => { window.__zt.mark(); return true; });
      const before = await zt(page, () => ({ offset: window.__zt.offset(), id: window.__zt.id(), updates: window.__zt.updates(), fades: window.__zt.fades(), d: window.__zt.dur() }));
      const second = await openPage('second overlay page', { width: 1920, height: 72 }, overlayUrl());
      await second.page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await page.waitForFunction((n) => window.__zt.updates() > n, before.updates, { timeout: 5000 }).catch(() => {});
      await sleep(400);
      const after = await zt(page, () => ({ offset: window.__zt.offset(), id: window.__zt.id(), updates: window.__zt.updates(), fades: window.__zt.fades(), marked: window.__zt.marked() }));
      check('crawl (decision 14): a second overlay connecting mid-loop sends the first the payload already on screen; it causes no swap and no fade, and the track offset is unchanged (the wrap was far past the threshold, so new content would have faded)',
        before.d - 4000 > FADE_THRESHOLD_MS + 10000
          && after.updates > before.updates && after.offset === before.offset && after.id === before.id && after.fades === before.fades && after.marked === true,
        JSON.stringify({ before, after }));
      await closePage(second);
      await closePage(p);
    }

    // One held payload, and the newest wins (row 7.1), on a slow loop (?speed=10), so the
    // wrap is far past the threshold except where a case seeks near it. The sends of each
    // case run in one evaluate, so nothing happens in between.
    {
      const p = await openPage('held-payload overlay page', { width: 1920, height: 72 }, overlayUrl(1920, '&speed=10'), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const short = (cs) => cs.map((c) => c.slice(0, 36));
      // (1) Far from the wrap, two payloads at once: one fade, and its swap shows the newer.
      await zt(page, (pl) => { window.__zt.send(pl.blank); window.__zt.send(pl.one); return true; }, { blank: blankOf('Sprint7 Held'), one: crawlOf('HOne', 1) });
      await zt(page, () => window.__zt.seek(1000));
      const d0 = await zt(page, () => window.__zt.dur());
      const fades0 = await zt(page, () => window.__zt.fades());
      await zt(page, (pl) => { window.__zt.send(pl.b); window.__zt.send(pl.c); return true; }, { b: crawlOf('HTwo', 1), c: crawlOf('HThree', 1) });
      await sleep(800);
      const one = await zt(page, () => ({ copies: window.__zt.copies(), fades: window.__zt.fades(), fading: window.__zt.fading() }));
      check('crawl (row 7.1): two payloads that arrive together far from the wrap take one fade, and its swap puts the newer one in every copy',
        d0 - 1000 > FADE_THRESHOLD_MS + 10000 && one.fades === fades0 + 1 && one.fading === false
          && one.copies.length >= 2 && one.copies.every((c) => c.includes('HThree Player') && !c.includes('HTwo Player')),
        JSON.stringify({ d0, fades0, one: { ...one, copies: short(one.copies) } }));
      // (2) Near the wrap a payload is held; then the content on screen comes again. The
      // held one is no longer wanted: at the wrap no copy takes it.
      const d = await zt(page, () => window.__zt.dur());
      const id1 = await zt(page, () => window.__zt.id());
      await zt(page, (t) => window.__zt.seek(t), d - NEAR_MS);
      const two = await zt(page, (pl) => {
        window.__zt.send(pl.b);
        const held = { copies: window.__zt.copies(), fading: window.__zt.fading() };
        window.__zt.send(pl.a);
        return held;
      }, { b: crawlOf('HFour', 1), a: crawlOf('HThree', 1) });
      await zt(page, (t) => window.__zt.seek(t), d - 1);
      const s2 = (await zt(page, (t) => window.__zt.snapAt(t), d + 1)) || { type: null, copies: [], fading: null };
      const two2 = await zt(page, () => ({ id: window.__zt.id(), fades: window.__zt.fades() }));
      check('crawl (row 7.1): a payload held for the wrap, then the content on screen again: at the wrap no copy takes the held one, no fade, and the same loop runs on',
        two.fading === false && !two.copies.join('').includes('HFour Player')
          && s2.type === 'animationiteration' && s2.copies.length >= 2 && !s2.copies.join('').includes('HFour Player') && s2.fading === false
          && two2.id === id1 && two2.fades === fades0 + 1,
        JSON.stringify({ two: { ...two, copies: short(two.copies) }, s2: { type: s2.type, copies: short(s2.copies), fading: s2.fading }, two2, id1 }));
      // (3) Near the next wrap, two payloads held: the newer goes into the copies off screen.
      await zt(page, (t) => window.__zt.seek(t), 2 * d - NEAR_MS);
      await zt(page, (pl) => { window.__zt.send(pl.b); window.__zt.send(pl.c); return true; }, { b: crawlOf('HFive', 1), c: crawlOf('HSix', 1) });
      await zt(page, (t) => window.__zt.seek(t), 2 * d - 1);
      const s3 = (await zt(page, (t) => window.__zt.snapAt(t), 2 * d + 1)) || { type: null, copies: [], fading: null };
      check('crawl (row 7.1): of two payloads held for the same wrap, the newer goes into the copies off screen and the older into none; the first copy keeps what is on screen; no fade',
        s3.type === 'animationiteration' && s3.copies.length >= 2 && s3.copies[0].includes('HThree Player')
          && s3.copies.slice(1).every((c) => c.includes('HSix Player')) && !s3.copies.join('').includes('HFive Player') && s3.fading === false,
        JSON.stringify({ type: s3.type, copies: short(s3.copies), fading: s3.fading }));
      // (4) The content going into the copies off screen comes again, with the loop's end a
      // whole loop away (far past the threshold): it does nothing.
      await zt(page, () => { window.__zt.mark(); return true; });
      const f4 = await zt(page, () => window.__zt.fades());
      const again = await zt(page, (pl) => { window.__zt.send(pl); return { fading: window.__zt.fading() }; }, crawlOf('HSix', 1));
      await sleep(500);
      const four = await zt(page, () => ({ fades: window.__zt.fades(), fading: window.__zt.fading(), marked: window.__zt.marked(), id: window.__zt.id(), copies: window.__zt.copies() }));
      check('crawl (row 7.1, decision 14): the content going into the copies off screen, sent again with the loop\'s end a whole loop away, does nothing: no fade, the same copies, the same loop',
        again.fading === false && four.fades === f4 && four.fading === false && four.marked === true && four.id === id1
          && four.copies[0].includes('HThree Player') && four.copies.slice(1).every((c) => c.includes('HSix Player')),
        JSON.stringify({ again, f4, four: { ...four, copies: short(four.copies) } }));
      // (5) A payload held while that content is going in: at the loop's end every copy takes
      // that content and the held one goes straight into the copies off screen.
      await zt(page, (t) => window.__zt.seek(t), 3 * d - NEAR_MS);
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, crawlOf('HSeven', 1));
      await zt(page, (t) => window.__zt.seek(t), 3 * d - 1);
      const s5 = (await zt(page, (t) => window.__zt.snapAt(t), 3 * d + 1)) || { type: null, copies: [], fading: null };
      const five = await zt(page, () => ({ id: window.__zt.id(), fades: window.__zt.fades() }));
      check('crawl (row 7.1): a payload that arrives while other content is going into the copies off screen goes into them itself at the loop\'s end, while the first copy takes the content before it; no fade',
        s5.type === 'animationend' && s5.copies.length >= 2 && s5.copies[0].includes('HSix Player')
          && s5.copies.slice(1).every((c) => c.includes('HSeven Player')) && s5.fading === false && five.id !== id1 && five.fades === f4,
        JSON.stringify({ type: s5.type, copies: short(s5.copies), fading: s5.fading, five }));
      await closePage(p);
    }

    // A strip scaled by a theme or OBS Custom CSS (.zipper { transform: scale(1.5) }): the
    // loop is measured in the strip's own px, so it still wraps where the next copy starts.
    {
      const scaled = () => {
        const add = () => { const s = document.createElement('style'); s.textContent = '.zipper { transform: scale(1.5); transform-origin: 0 0; }'; (document.head || document.documentElement).appendChild(s); };
        if (document.documentElement) add(); else document.addEventListener('readystatechange', add, { once: true });
      };
      const p = await openPage('scaled overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: [crawlProbe, scaled] });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const d = await zt(page, () => window.__zt.dur());
      await zt(page, (t) => window.__zt.seek(t), d - 1);
      const v1 = await zt(page, () => ({ view: window.__zt.view(), segW: window.__zt.segW(), layout: window.__zt.layoutWidths(), rect: window.__zt.widths(), wrap: window.__zt.wrapW() }));
      const v2 = (await zt(page, (t) => window.__zt.snapAt(t), d + 1)) || { type: null, view: [] };
      check('crawl: on a strip scaled by custom CSS (1.5 times) the loop is measured in the strip\'s own px: it ends at the first copy\'s width (1.5 times that on screen), the copies cover the middle, and at the wrap the text in view does not move',
        Math.abs(v1.rect[0] - 1.5 * v1.layout[0]) <= 1 && Math.abs(v1.segW - v1.layout[0]) <= 0.5 && v1.layout[0] >= v1.wrap
          && v2.type === 'animationiteration' && sameView(v1.view, v2.view),
        JSON.stringify({ d, segW: v1.segW, layout: v1.layout, rect: v1.rect, wrap: v1.wrap, type: v2.type, v1: v1.view.map((v) => [v.text.slice(0, 20), Math.round(v.x)]), v2: v2.view.map((v) => [v.text.slice(0, 20), Math.round(v.x)]) }));
      await closePage(p);
    }

    // A theme that styles a .seg by its place (here .seg + .seg gets a border, to mark where
    // the loop repeats): the loop's end is read from the first copy in place, so a swap under
    // the fade is measured right and is not followed by fade after fade.
    {
      const placed = () => {
        const add = () => { const s = document.createElement('style'); s.textContent = '.seg + .seg { border-left: 2px solid #ffd166; }'; (document.head || document.documentElement).appendChild(s); };
        if (document.documentElement) add(); else document.addEventListener('readystatechange', add, { once: true });
      };
      const p = await openPage('placed-seg overlay page', { width: 1920, height: 72 }, overlayUrl(1920, '&speed=10'), { init: [crawlProbe, placed] });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      await zt(page, () => window.__zt.seek(1000));
      const fades0 = await zt(page, () => window.__zt.fades());
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, crawlOf('Placed', 2));
      await sleep(2500);
      const r = await zt(page, () => ({ fades: window.__zt.fades(), fading: window.__zt.fading(), segW: window.__zt.segW(), widths: window.__zt.widths(), copies: window.__zt.copies() }));
      check('crawl: with a theme rule for .seg + .seg, a swap under the fade takes one fade, the loop ends at the first copy\'s width, and no fade follows it',
        r.fades === fades0 + 1 && r.fading === false && Math.abs(r.segW - r.widths[0]) <= 0.5 && r.copies.every((c) => c.includes('Placed Player')),
        JSON.stringify({ fades0, ...r, copies: r.copies.map((c) => c.slice(0, 30)) }));
      await closePage(p);
    }

    // A theme that gives the track a longer fade (1 s): the swap waits for it, so the copies
    // are never replaced while the track is still partly in sight.
    {
      const longFade = () => {
        const add = () => { const s = document.createElement('style'); s.textContent = '#track.track { transition: opacity 1s linear; }'; (document.head || document.documentElement).appendChild(s); };
        if (document.documentElement) add(); else document.addEventListener('readystatechange', add, { once: true });
      };
      const p = await openPage('long-fade overlay page', { width: 1920, height: 72 }, overlayUrl(1920, '&speed=10'), { init: [crawlProbe, longFade] });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      await zt(page, () => window.__zt.seek(1000));
      const n0 = (await zt(page, () => window.__zt.swapAfter())).length;
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, crawlOf('LongFade', 2));
      await sleep(2600); // 1 s out, then 1 s back in
      const r = await zt(page, () => ({ swapAfter: window.__zt.swapAfter(), fading: window.__zt.fading(), copies: window.__zt.copies() }));
      const after = r.swapAfter.slice(n0);
      check('crawl: with a theme\'s 1 s fade on .track, the swap under the fade waits until the track has faded out (1 s or more after the fade starts), and the new content is in every copy',
        after.length === 1 && after[0] >= 1000 && after[0] < 2500 && r.fading === false && r.copies.every((c) => c.includes('LongFade Player')),
        JSON.stringify({ after, fading: r.fading, copies: r.copies.map((c) => c.slice(0, 30)) }));
      await closePage(p);
    }

    // During an announcement: a held result is swapped in when the announcement hides the
    // track, and one that arrives while it is up is swapped in at once. Either swap starts its
    // loop at offset 0: the owner's ruling of 2026-11-26 is about the fade, and the
    // announcement path keeps what it did before it.
    {
      const p = await openPage('announce swap overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const d = await zt(page, () => window.__zt.dur());
      await zt(page, (t) => window.__zt.seek(t), d - NEAR_MS);
      const fades0 = await zt(page, () => window.__zt.fades());
      // Sent and read in one go: held, so in no copy, not fading, and no fade started. Also
      // how far in the loop is (px) and which loop it is.
      const heldNow = await zt(page, (payload) => { window.__zt.send(payload); return { copies: window.__zt.copies(), fading: window.__zt.fading(), fades: window.__zt.fades(), px: -window.__zt.offset(), id: window.__zt.id() }; }, crawlOf('AnnHeld', 1));
      // 10 s, so the steps below run well inside it; the page is closed right after.
      await fetch(`${HTTP}/mock/announce?text=${enc('Sprint 7 hides the track')}&kind=custom&duration=10`);
      await page.waitForSelector('.zipper.announcing', { timeout: 5000 }).catch(() => {});
      const during = await zt(page, () => ({ copies: window.__zt.copies(), announcing: document.querySelector('.zipper').classList.contains('announcing'), fades: window.__zt.fades(), start: window.__zt.startOf() }));
      // The loop the bar has paused, moved 3 s in, so a swap that kept the crawl's place would show.
      await zt(page, () => window.__zt.seek(3000));
      const liveAt = await zt(page, () => ({ px: -window.__zt.offset(), id: window.__zt.id() }));
      await zt(page, (payload) => { window.__zt.send(payload); return true; }, crawlOf('AnnLive', 2));
      const live = await zt(page, () => ({ copies: window.__zt.copies(), announcing: document.querySelector('.zipper').classList.contains('announcing'), fading: window.__zt.fading(), fades: window.__zt.fades(), start: window.__zt.startOf() }));
      check('crawl (row 7.1): a result held for the wrap (in no copy, no fade) is swapped in when an announcement hides the track, and one that arrives during the announcement is swapped in at once, with no fade',
        !heldNow.copies.join('').includes('AnnHeld') && heldNow.fading === false && heldNow.fades === fades0
          && during.announcing && during.copies.every((c) => c.includes('AnnHeld')) && during.fades === fades0
          && live.announcing && live.copies.every((c) => c.includes('AnnLive')) && live.fading === false && live.fades === fades0,
        JSON.stringify({ fades0, heldNow: { ...heldNow, copies: heldNow.copies.map((c) => c.slice(0, 30)) }, during: { ...during, copies: during.copies.map((c) => c.slice(0, 30)) }, live: { ...live, copies: live.copies.map((c) => c.slice(0, 30)) } }));
      check('crawl (row 7.1, the announcement path): both swaps under the announcement start the new loop at offset 0, not at the crawl\'s place: the held result put in as the bar came (the loop over 100 px in), and the result that came while it was up (the paused loop moved 3 s in)',
        heldNow.px >= 100 && !!during.start && during.start.id !== heldNow.id && Math.abs(during.start.px) <= 0.5
          && liveAt.px >= 100 && !!live.start && live.start.id !== liveAt.id && Math.abs(live.start.px) <= 0.5,
        JSON.stringify({ heldAt: heldNow.px, heldId: heldNow.id, during: during.start, liveAt, live: live.start }));
      await closePage(p);
    }

    // Applied at once: the blank strip (decision 5), and the first crawl after a static
    // line. Then the rendering details of ZIP-19: null entries, and a completed match with
    // no scores and no winner. Each of those is sent after a blank strip, so it is the
    // first crawl after a static line and is drawn at once.
    {
      const p = await openPage('static overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const d = await zt(page, () => window.__zt.dur());
      await zt(page, (t) => window.__zt.seek(t), d - NEAR_MS);
      const blank = await zt(page, (payload) => { window.__zt.send(payload); return { isStatic: window.__zt.isStatic(), text: window.__zt.text() }; }, blankOf('Sprint7 Open'));
      const first = await zt(page, (payload) => { window.__zt.send(payload); return { copies: window.__zt.copies(), fading: window.__zt.fading(), isStatic: window.__zt.isStatic() }; }, crawlOf('FirstAfter', 1));
      check('crawl (row 7.1): the blank strip after a new URL is applied at once, even near the wrap, and so is the first crawl after it, with no fade',
        blank.isStatic === true && blank.text === 'Sprint7 Open — waiting for results…'
          && first.isStatic === false && first.copies.length >= 2 && first.copies.every((c) => c.includes('FirstAfter')) && first.fading === false,
        JSON.stringify({ blank, first: { ...first, copies: first.copies.map((c) => c.slice(0, 30)) } }));
      const at = (payload) => zt(page, (pl) => {
        window.__zt.send(pl.blank);
        window.__zt.send(pl.payload);
        const seg = document.querySelector('.track .seg');
        return { isStatic: window.__zt.isStatic(), items: seg ? Array.from(seg.querySelectorAll('.item')).map((e) => e.textContent) : [], wins: seg ? seg.querySelectorAll('.win').length : -1 };
      }, { blank: blankOf('Sprint7 Null'), payload });
      const m = sample.matches;
      const nullMatch = await at({ ...sample, matches: [null, m[0], m[1]] });
      const nullStanding = await at({ ...sample, matches: [], standings: [null, { placement: 1, name: 'Standing One' }, { placement: 2, name: 'Standing Two' }] });
      check('overlay (ZIP-19): a payload with a null match still draws the other matches, and one with a null standing still draws the other standings',
        !nullMatch.isStatic && nullMatch.items.length >= 2 && nullMatch.items.some((t) => t.includes('Zangief Main')) && nullMatch.items.some((t) => t.includes('Newbie'))
          && !nullStanding.isStatic && nullStanding.items.some((t) => t.includes('Standing One')) && nullStanding.items.some((t) => t.includes('Standing Two')),
        JSON.stringify({ nullMatch, nullStanding }));
      const vs = await at({ ...sample, matches: [{ id: 'vs1', round: 'Pools', state: 'completed', p1: { name: 'Alpha', score: null }, p2: { name: 'Bravo', score: null }, winner: 0 }] });
      check('overlay (ZIP-19): a completed match with no scores and no winner reads "Alpha vs Bravo", with no one marked as the winner',
        vs.items.length >= 1 && vs.items[0] === 'PoolsAlphavsBravo' && vs.wins === 0, JSON.stringify(vs));
      // The score's dash has a thin space (U+2009) on each side, as it had before this sprint.
      const scores = await zt(page, (pl) => {
        window.__zt.send(pl.blank);
        window.__zt.send(pl.payload);
        return Array.from(document.querySelectorAll('.track .seg:first-child .score'), (e) => e.textContent);
      }, { blank: blankOf('Sprint7 Score'), payload: sample });
      check('overlay: the scores read "2 – 1" (completed) and "1 – 1" (LIVE), with a thin space (U+2009) on each side of the dash',
        scores.length >= 2 && scores[0] === '2 – 1' && scores.includes('1 – 1') && scores.every((s) => !s.includes(' ')),
        JSON.stringify(scores.map((s) => Array.from(s, (ch) => ch.codePointAt(0).toString(16)).join(' '))));
      await closePage(p);
    }

    // The logo (ZIP-19): one img, its src set only when the logo changes; a logo that does
    // not load (here an address holding a key's label, which the mock answers 404) hides the
    // left cap, and a later logo that loads shows it again; a logo that loads late, and the
    // failed one, leave the crawl seamless.
    {
      const svg = (wpx) => `<svg xmlns="http://www.w3.org/2000/svg" width="${wpx}" height="40"><rect width="${wpx}" height="40" fill="#ffd166"/></svg>`;
      const logoReqs = [];
      const setup = async (page) => {
        page.on('request', (r) => { const u = new URL(r.url()); if (u.pathname.startsWith('/zipper-overlay/__s7-') || u.pathname.includes('Challonge')) logoReqs.push(decodeURIComponent(u.pathname)); });
        await page.route((u) => u.origin === HTTP && u.pathname === '/zipper-overlay/__s7-logo.svg', (r) => r.fulfill({ status: 200, contentType: 'image/svg+xml', body: svg(40) }));
        await page.route((u) => u.origin === HTTP && u.pathname === '/zipper-overlay/__s7-late-logo.svg', async (r) => { await sleep(1500); await r.fulfill({ status: 200, contentType: 'image/svg+xml', body: svg(200) }); });
        // A host that fails the first request (503) and answers every one after it.
        let flakyHits = 0;
        await page.route((u) => u.origin === HTTP && u.pathname === '/zipper-overlay/__s7-flaky-logo.svg', (r) => (++flakyHits === 1
          ? r.fulfill({ status: 503, contentType: 'text/plain', body: 'down for a moment' })
          : r.fulfill({ status: 200, contentType: 'image/svg+xml', body: svg(60) })));
      };
      // Counts every src the page gives an img, through the property or setAttribute, the
      // img in the document or not yet.
      const init = () => {
        window.__srcSets = 0;
        const prop = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, 'src');
        Object.defineProperty(HTMLImageElement.prototype, 'src', { configurable: true, enumerable: prop.enumerable, get() { return prop.get.call(this); }, set(v) { window.__srcSets++; prop.set.call(this, v); } });
        const setAttr = Element.prototype.setAttribute;
        Element.prototype.setAttribute = function (name, value) {
          if (this instanceof HTMLImageElement && String(name).toLowerCase() === 'src') window.__srcSets++;
          return setAttr.call(this, name, value);
        };
      };
      // skewClock: the logo's retry wait (LOGO_RETRY_MS) is passed by moving the page's clock on.
      const p = await openPage('logo overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: [crawlProbe, init, skewClock], setup });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await sleep(300);
      const capOf = () => page.evaluate(() => {
        const c = document.getElementById('cap-left');
        const img = c.querySelector('img');
        return { on: c.classList.contains('on'), shown: getComputedStyle(c).display !== 'none', width: c.getBoundingClientRect().width, imgs: c.querySelectorAll('img').length, src: img ? img.getAttribute('src') : null, sets: window.__srcSets, kept: !!(img && img.__s7) };
      });
      const withLogo = (logo) => ({ ...sample, caps: { text: '@FlashGalatine', logo } });
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo('/zipper-overlay/__s7-logo.svg'));
      await page.waitForFunction(() => { const i = document.querySelector('#cap-left img'); return !!(i && i.complete && i.naturalWidth > 0); }, null, { timeout: 4000 }).catch(() => {});
      await page.evaluate(() => { const i = document.querySelector('#cap-left img'); if (i) i.__s7 = true; });
      const one = await capOf();
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo(' /zipper-overlay/__s7-logo.svg '));
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo('/zipper-overlay/__s7-logo.svg'));
      await sleep(300);
      const two = await capOf();
      check('overlay (ZIP-19): the logo img is kept across updates and its src is set only when the logo changes (the same logo, trimmed or not, sets nothing and asks for nothing again)',
        one.on && one.imgs === 1 && one.sets === 1 && two.on && two.imgs === 1 && two.kept === true && two.sets === 1
          && logoReqs.filter((x) => x === '/zipper-overlay/__s7-logo.svg').length === 1,
        JSON.stringify({ one, two, logoReqs }));
      // A logo that does not load hides the left cap; the same logo again keeps it hidden.
      const labelLogo = '/zipper-overlay/assets/[Challonge key].png';
      const id0 = await zt(page, () => window.__zt.id());
      const fades0 = await zt(page, () => window.__zt.fades());
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo(labelLogo));
      // Read as soon as the failure is in, before any later update could fold it into the cap.
      await page.waitForFunction(() => { const i = document.querySelector('#cap-left img'); return !!(i && i.complete && i.naturalWidth === 0); }, null, { timeout: 4000 }).catch(() => {});
      await sleep(100);
      const failedNow = await capOf();
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo(labelLogo));
      await sleep(300);
      const failed = await capOf();
      const failedLoop = await zt(page, () => ({ id: window.__zt.id(), fades: window.__zt.fades(), segW: window.__zt.segW(), widths: window.__zt.widths(), covered: window.__zt.coveredAtEnd() }));
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo('/zipper-overlay/__s7-logo.svg'));
      await page.waitForFunction(() => document.getElementById('cap-left').classList.contains('on'), null, { timeout: 4000 }).catch(() => {});
      const back = await capOf();
      check('overlay (ZIP-19): a logo that does not load (an address holding a key\'s label, answered 404) hides the left cap as soon as it fails, with no other update; the same logo in a later update keeps it hidden and asks for nothing again; a logo that loads shows it again',
        failedNow.on === false && failedNow.shown === false && failedNow.src === labelLogo
          && failed.on === false && failed.shown === false && failed.src === labelLogo && failed.sets === failedNow.sets
          && logoReqs.filter((x) => x === '/zipper-overlay/assets/[Challonge key].png').length === 1
          && back.on === true && back.shown === true,
        JSON.stringify({ failedNow, failed, back, logoReqs }));
      check('overlay (ZIP-19): when the failed logo hides the left cap and the strip\'s middle grows, the crawl stays seamless: the same loop, no fade, its end still the first copy\'s width, and the copies after it still cover the middle',
        failedLoop.id === id0 && failedLoop.fades === fades0 && Math.abs(failedLoop.segW - failedLoop.widths[0]) <= 0.5 && failedLoop.covered === true,
        JSON.stringify({ id0, fades0, failedLoop }));
      // A logo that loads late (1.5 s): the cap widens when it comes, and the crawl is untouched.
      const id1 = await zt(page, () => window.__zt.id());
      const fades1 = await zt(page, () => window.__zt.fades());
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo('/zipper-overlay/__s7-late-logo.svg'));
      const early = await capOf();
      await page.waitForFunction(() => { const i = document.querySelector('#cap-left img'); return !!(i && i.complete && i.naturalWidth > 0); }, null, { timeout: 5000 }).catch(() => {});
      await sleep(300);
      const late = await capOf();
      const lateLoop = await zt(page, () => ({ id: window.__zt.id(), fades: window.__zt.fades(), segW: window.__zt.segW(), widths: window.__zt.widths(), covered: window.__zt.coveredAtEnd(), frames: window.__zt.frames() }));
      const lf = loopFrames(lateLoop.frames, 80);
      check('overlay (ZIP-19): a logo that loads late widens the left cap when it comes, and the crawl stays seamless: the same loop, no fade, its end still the first copy\'s width, the middle still covered; no loop changed its duration while it ran',
        late.on && late.width > early.width + 100 && lateLoop.id === id1 && lateLoop.fades === fades1
          && Math.abs(lateLoop.segW - lateLoop.widths[0]) <= 0.5 && lateLoop.covered === true && lf.changed === 0 && lf.mismatched === 0,
        JSON.stringify({ early, late, id1, lateLoop: { ...lateLoop, frames: undefined }, lf }));
      // A logo whose host fails once (503) and then answers: the failure hides the left cap;
      // an update inside LOGO_RETRY_MS asks for nothing; the first update after it asks for
      // the same address again, and once that loads the cap is back.
      const flaky = '/zipper-overlay/__s7-flaky-logo.svg';
      const flakyReqs = () => logoReqs.filter((x) => x === flaky).length;
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo(flaky));
      await page.waitForFunction(() => { const i = document.querySelector('#cap-left img'); return !!(i && i.complete && i.naturalWidth === 0); }, null, { timeout: 4000 }).catch(() => {});
      await sleep(100);
      const flakyFailed = await capOf();
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo(flaky));
      await sleep(300);
      const flakyInside = { ...(await capOf()), reqs: flakyReqs() };
      await page.evaluate((ms) => window.__zipperTestClockSkew(ms), LOGO_RETRY_MS + 1000);
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo(flaky));
      await page.waitForFunction(() => document.getElementById('cap-left').classList.contains('on'), null, { timeout: 4000 }).catch(() => {});
      const flakyBack = await capOf();
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, withLogo(flaky));
      await sleep(300);
      const reqsAfter = flakyReqs();
      check('overlay (ZIP-19): a logo whose host fails once (503) hides the left cap; an update inside the retry wait asks for nothing; the first update after LOGO_RETRY_MS asks for the same address again, and once it loads the cap is back; a later update asks for nothing more',
        flakyFailed.on === false && flakyFailed.shown === false && flakyInside.on === false && flakyInside.reqs === 1
          && flakyBack.on === true && flakyBack.shown === true && flakyBack.src === flaky && reqsAfter === 2,
        JSON.stringify({ flakyFailed, flakyInside, flakyBack, reqsAfter }));
      // The problems this page is expected to have: the browser's own console lines for the
      // logo that answered 404 and for the one whose host failed once (503).
      const decoded = (x) => { try { return decodeURIComponent(x); } catch { return x; } };
      const notFound = p.problems.filter((x) => x.startsWith('console error: ') && x.includes('404') && decoded(x).includes('[Challonge key]'));
      const unavailable = p.problems.filter((x) => x.startsWith('console error: ') && x.includes('503') && x.includes('__s7-flaky-logo.svg'));
      p.problems = p.problems.filter((x) => !notFound.includes(x) && !unavailable.includes(x));
      check('overlay: the logos that did not load are the only things the browser complained about on that page (the 404 and the one 503, each logged once by the browser)',
        notFound.length === 1 && unavailable.length === 1, JSON.stringify({ notFound, unavailable }));
      await closePage(p);
    }

    // A long right cap at 640 px (ZIP-19): the cap stops at its maximum width and its text
    // ends in an ellipsis, leaving room for the crawl.
    {
      const p = await openPage('long-cap overlay page', { width: 640, height: 72 }, overlayUrl(640), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      const text = 'Follow @FlashGalatine on every platform for brackets, results, schedules and the Discord lobby code each week!!';
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, { ...sample, caps: { text, logo: '' } });
      await sleep(200);
      const r = await page.evaluate(() => {
        const cap = document.getElementById('cap-right');
        const span = cap.querySelector('.cap-text') || cap;
        return { cap: cap.getBoundingClientRect().width, text: cap.textContent, overflow: getComputedStyle(span).textOverflow, cut: span.scrollWidth > span.clientWidth, wrap: document.getElementById('track-wrap').clientWidth };
      });
      check('overlay (ZIP-19): at 640 px a long right cap (110 characters) stops at 40% of the strip and ends in an ellipsis, so the crawl keeps the rest',
        r.text === text && r.cap <= 640 * 0.4 + 0.5 && r.overflow === 'ellipsis' && r.cut === true && r.wrap >= 640 * 0.6 - 1,
        JSON.stringify(r));
      await closePage(p);
    }

    // The strip's middle grows past a short crawl (here the strip is widened from 1280 to
    // 1920 px, with no new payload): the loop must not run past its last copy.
    {
      const p = await openPage('widened overlay page', { width: 1920, height: 72 }, overlayUrl(1280), { init: crawlProbe });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await zt(page, (pl) => { window.__zt.send(pl.blank); window.__zt.send(pl.msg); return true; }, { blank: blankOf('Sprint7 Short'), msg: { ...sample, caps: { text: '', logo: '' }, message: { lines: ['Back soon'] } } });
      await sleep(300);
      const id0 = await zt(page, () => window.__zt.id());
      const fades0 = await zt(page, () => window.__zt.fades());
      // The first copy's own ResizeObserver gives its first notice a frame or more after the
      // build; let that pass, so only the observer on #track-wrap can see the widening.
      await zt(page, () => window.__zt.settle(3));
      const before = await zt(page, () => ({ wrap: window.__zt.wrapW(), widths: window.__zt.widths() }));
      await page.evaluate(() => document.documentElement.style.setProperty('--ticker-width', '1920px'));
      await sleep(400);
      const d = await zt(page, () => window.__zt.dur());
      await zt(page, (t) => window.__zt.seek(t), d - 1); // the loop's end, where the view reaches furthest
      const r = await page.evaluate(() => {
        const wr = document.getElementById('track-wrap').getBoundingClientRect();
        const last = document.getElementById('track').lastElementChild.getBoundingClientRect();
        return { wrap: wr.width, gap: Math.max(0, wr.right - last.right), copies: window.__zt.copies().length, id: window.__zt.id(), fades: window.__zt.fades(), segW: window.__zt.segW(), widths: window.__zt.widths() };
      });
      check('crawl (ZIP-19): when the strip\'s middle grows past a short crawl with no new payload, the loop shows no gap at its end (copies are added off screen), with no fade and no restart',
        before.wrap < 1300 && r.wrap > 1800 && r.gap === 0 && r.id === id0 && r.fades === fades0 && Math.abs(r.segW - r.widths[0]) <= 0.5,
        JSON.stringify({ before, r }));
      // Now the copy after the first is in view at the wrap (the middle is wider than one
      // copy), so it cannot take new content there. At this loop's end every copy is built
      // again for the wider middle (row 7.2: the re-measure goes through the swap path),
      // with nothing in view changing, and the content held for the wrap goes into the
      // copies off screen then, so it scrolls in from the right. No fade (decision 8).
      const noWrap = { type: null, view: [], copies: ['(no wrap event)'], fading: null, othersOff: null };
      await zt(page, (t) => window.__zt.seek(t), d - Math.min(2000, NEAR_MS));
      const fadesW = await zt(page, () => window.__zt.fades());
      await zt(page, (pl) => { window.__zt.send(pl); return true; }, { ...sample, caps: { text: '', logo: '' }, message: { lines: ['Back in five'] } });
      await zt(page, (t) => window.__zt.seek(t), d - 1);
      const w1 = await zt(page, () => ({ view: window.__zt.view(), copies: window.__zt.copies() }));
      const w2 = (await zt(page, (t) => window.__zt.snapAt(t), d + 1)) || noWrap;
      const w2b = await zt(page, () => ({ id: window.__zt.id(), widths: window.__zt.widths(), wrap: window.__zt.wrapW(), segW: window.__zt.segW(), dur: window.__zt.dur(), fading: window.__zt.fading() }));
      await zt(page, (t) => window.__zt.seek(t), w2b.dur - 1);
      const w3a = await zt(page, () => ({ view: window.__zt.view() }));
      const w3 = (await zt(page, (t) => window.__zt.snapAt(t), w2b.dur + 1)) || noWrap;
      const w4 = await zt(page, () => ({ fades: window.__zt.fades(), fading: window.__zt.fading() }));
      check('crawl (decision 8, ZIP-19): when the copy after the first is in view at the wrap, held content does not go into it there; at that loop\'s end every copy is built again for the wider middle with nothing in view changing, and the held content goes into the copies off screen; at the next wrap every copy holds it, again with nothing in view changing; no fade at any point',
        w2.type === 'animationend' && sameView(w1.view, w2.view) && w2.fading === false && w2.othersOff === true
          && !w2.copies[0].includes('Back in five') && w2.copies.length >= 2 && w2.copies.slice(1).every((c) => c.includes('Back in five'))
          && w2b.id !== id0 && w2b.widths[0] >= w2b.wrap && Math.abs(w2b.segW - w2b.widths[0]) <= 0.5 && w2b.fading === false
          && w3.type === 'animationend' && sameView(w3a.view, w3.view) && w3.copies.every((c) => c.includes('Back in five'))
          && w4.fades === fadesW && w4.fading === false,
        JSON.stringify({ w1: w1.view.map((v) => [v.text.slice(0, 16), Math.round(v.x)]), w2: { ...w2, view: w2.view.map((v) => [v.text.slice(0, 16), Math.round(v.x)]), copies: w2.copies.map((c) => c.slice(0, 20)) }, w2b, w3: { ...w3, view: w3.view.map((v) => [v.text.slice(0, 16), Math.round(v.x)]), copies: w3.copies.map((c) => c.slice(0, 20)) }, w4, fadesW }));
      await closePage(p);
    }

    // The copy after the first in view at the wrap (decision 8). This page has no
    // ResizeObserver (ticker.js runs without one), so a strip widened after the build is not
    // measured again, and at the wrap the copy after the first is in view. Held content must
    // not go into a copy in view: it comes in under the fade instead.
    {
      const noResizeObserver = () => { try { delete window.ResizeObserver; } catch { /* kept */ } window.ResizeObserver = undefined; };
      const p = await openPage('no-observer overlay page', { width: 1920, height: 72 }, overlayUrl(1280), { init: [noResizeObserver, crawlProbe] });
      const { page } = p;
      await page.waitForSelector('.track .seg .item', { timeout: 8000 });
      await zt(page, (pl) => { window.__zt.send(pl.blank); window.__zt.send(pl.msg); return true; }, { blank: blankOf('Sprint7 NoObserver'), msg: { ...sample, caps: { text: '', logo: '' }, message: { lines: ['Back soon'] } } });
      await sleep(300);
      const NEW = 'Back in five';
      const d = await zt(page, () => window.__zt.dur());
      await zt(page, (t) => window.__zt.seek(t), d - NEAR_MS);
      const fades0 = await zt(page, () => window.__zt.fades());
      const held = await zt(page, (pl) => { window.__zt.send(pl); return { copies: window.__zt.copies(), fading: window.__zt.fading() }; }, { ...sample, caps: { text: '', logo: '' }, message: { lines: [NEW] } });
      await page.evaluate(() => document.documentElement.style.setProperty('--ticker-width', '1920px'));
      await sleep(300);
      const wide = await zt(page, () => ({ wrap: window.__zt.wrapW(), widths: window.__zt.widths(), ro: typeof window.ResizeObserver, fades: window.__zt.fades() }));
      await zt(page, (t) => window.__zt.seek(t), d - 1);
      const snap = (await zt(page, (t) => window.__zt.snapAt(t), d + 1)) || { type: null, view: [], copies: [], fading: null, othersOff: null };
      await sleep(900);
      const after = await zt(page, () => ({ fades: window.__zt.fades(), fading: window.__zt.fading(), copies: window.__zt.copies() }));
      check('crawl (decision 8): when the copy after the first is in view at the wrap (the strip widened on a page with no ResizeObserver, so nothing measured it again), held content does not go into it: at the wrap no copy and no item in view holds it and the fade starts, and the fade then puts it in every copy',
        wide.ro === 'undefined' && wide.widths[0] < wide.wrap && wide.fades === fades0 && held.fading === false && !held.copies.join('').includes(NEW)
          && snap.type === 'animationiteration' && snap.othersOff === false && snap.view.length > 0
          && snap.view.every((v) => !v.text.includes(NEW)) && snap.copies.every((c) => !c.includes(NEW)) && snap.fading === true
          && after.fades === fades0 + 1 && after.fading === false && after.copies.length >= 2 && after.copies.every((c) => c.includes(NEW)),
        JSON.stringify({ d, fades0, wide, held: { ...held, copies: held.copies.map((c) => c.slice(0, 20)) }, snap: { type: snap.type, othersOff: snap.othersOff, fading: snap.fading, view: snap.view.map((v) => [v.text.slice(0, 16), Math.round(v.x)]), copies: snap.copies.map((c) => c.slice(0, 20)) }, after: { ...after, copies: after.copies.map((c) => c.slice(0, 20)) } }));
      await closePage(p);
    }

    // A web font served late (1.5 s, a delayed route), as a font set through OBS Custom CSS
    // would be: the loop was measured with the fallback font, and is measured again when
    // the font arrives, so it has no gap or jump at the wrap and keeps its speed. The font
    // file is one of the system's, read from disk and served from this page's route only.
    // The page has no end caps, so the font triggers are what must see the change.
    {
      const candidates = ['C:/Windows/Fonts/consola.ttf', 'C:/Windows/Fonts/cour.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf', '/System/Library/Fonts/Courier.dfont'];
      let font = null;
      for (const f of candidates) { try { font = await readFile(f); break; } catch { /* next */ } }
      if (!font) {
        check('crawl (ZIP-19): a system font file to serve late was found', false, candidates.join(', '));
      } else {
        const setup = (page) => page.route((u) => u.origin === HTTP && u.pathname === '/zipper-overlay/__s7-font.ttf', async (r) => { await sleep(1500); await r.fulfill({ status: 200, contentType: 'font/ttf', body: font }); });
        const fontCss = () => {
          const add = () => {
            const s = document.createElement('style');
            s.textContent = "@font-face { font-family: 'ZipperS7Late'; src: url('/zipper-overlay/__s7-font.ttf') format('truetype'); font-display: swap; } :root { --ticker-font: 'ZipperS7Late', 'Segoe UI', sans-serif; }";
            (document.head || document.documentElement).appendChild(s);
          };
          if (document.documentElement) add(); else document.addEventListener('readystatechange', add, { once: true });
          // Whether the font had arrived when the first crawl was built (it must not have).
          window.addEventListener('ticker:update', () => {
            if (window.__fontAtFirstCrawl === undefined) window.__fontAtFirstCrawl = Array.from(document.fonts).some((f) => f.family.includes('ZipperS7Late') && f.status === 'loaded');
          });
        };
        await fetch(`${HTTP}/mock/push`, { method: 'POST', body: JSON.stringify({ ...sample, caps: { text: '', logo: '' } }) });
        const p = await openPage('late-font overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: [crawlProbe, fontCss], setup });
        const { page } = p;
        await page.waitForSelector('.track .seg .item', { timeout: 8000 });
        // page.goto waits for the load event, which here waits for the font: the loop as
        // first built is read from the page's first record.
        const early = await zt(page, () => { const f = window.__zt.frames()[0]; return { segW: f ? f.w : null, d: f ? f.d : null, loaded: window.__fontAtFirstCrawl ?? null }; });
        await page.waitForFunction(() => Array.from(document.fonts).some((f) => f.family.includes('ZipperS7Late') && f.status === 'loaded'), null, { timeout: 6000 }).catch(() => {});
        await sleep(900); // the re-measure, and the fade it takes
        const r = await zt(page, () => ({ segW: window.__zt.segW(), widths: window.__zt.widths(), dur: window.__zt.dur(), fading: window.__zt.fading(), fades: window.__zt.fades(), copies: window.__zt.copies(), covered: window.__zt.coveredAtEnd(), frames: window.__zt.frames() }));
        const lf = loopFrames(r.frames, 80);
        // The first crawl is built from the static line with no fade, so the font's
        // re-measure is the one fade on this page.
        check('crawl (ZIP-19): a web font served 1.5 s late re-measures the loop: its end is again the first copy\'s width (no gap or jump at the wrap) and its duration keeps the speed; it was swapped under the fade (one fade, and it is over), and no loop changed its duration while it ran',
          early.loaded === false && Math.abs(r.segW - early.segW) > 1 && Math.abs(r.segW - r.widths[0]) <= 0.5
            && Math.abs(r.dur - (r.segW / 80) * 1000) <= 2 && r.fading === false && r.fades === 1 && r.covered === true
            && r.copies.every((c) => c === r.copies[0]) && lf.changed === 0 && lf.mismatched === 0,
          JSON.stringify({ early, segW: r.segW, widths: r.widths, dur: r.dur, fading: r.fading, fades: r.fades, covered: r.covered, lf }));
        await closePage(p);

        // The first loop paused 1.5 s in (120 px at 80 px a second) as soon as it is built,
        // before the font comes, so a swap that keeps the crawl's place starts at 120 px and
        // one that starts again starts at 0.
        const pauseFirstLoop = () => {
          window.addEventListener('ticker:update', () => {
            if (window.__s7paused) return;
            window.__s7paused = true;
            setTimeout(() => {
              const t = document.getElementById('track');
              const a = t && t.getAnimations().find((x) => x.animationName === 'ticker-scroll');
              if (a) { a.pause(); a.currentTime = 1500; }
              window.__s7pausedPx = a ? -new DOMMatrix(getComputedStyle(t).transform).m41 : null;
            }, 0);
          });
        };

        // The same late font while an announcement hides the track: the re-measure is
        // swapped in at once, with no fade (the track is out of sight), and with nothing
        // new its loop keeps the crawl's place (the owner's ruling of 2026-11-27). The announcement is started the
        // moment the first crawl is built, before the font comes, right after the first loop
        // is paused 120 px in (pauseFirstLoop's timer is set first, so it runs first).
        const announceFirst = () => {
          window.addEventListener('ticker:update', () => {
            if (window.__s7announced) return;
            window.__s7announced = true;
            setTimeout(() => window.dispatchEvent(new CustomEvent('ticker:announce', { detail: { kind: 'custom', text: 'Sprint 7 font test', duration: 15 } })), 0);
          });
        };
        const p2 = await openPage('late-font announcement overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: [crawlProbe, fontCss, pauseFirstLoop, announceFirst], setup });
        await p2.page.waitForSelector('.track .seg .item', { state: 'attached', timeout: 8000 }); // hidden by the announcement
        const early2 = await zt(p2.page, () => { const f = window.__zt.frames()[0]; return { segW: f ? f.w : null, loaded: window.__fontAtFirstCrawl ?? null }; });
        await p2.page.waitForFunction(() => Array.from(document.fonts).some((f) => f.family.includes('ZipperS7Late') && f.status === 'loaded'), null, { timeout: 6000 }).catch(() => {});
        await sleep(600);
        const r2 = await zt(p2.page, () => ({ segW: window.__zt.segW(), widths: window.__zt.widths(), fading: window.__zt.fading(), fades: window.__zt.fades(), announcing: document.querySelector('.zipper').classList.contains('announcing'), pausedPx: window.__s7pausedPx ?? null, first: window.__zt.frames()[0] || null, start: window.__zt.startOf() }));
        check('crawl (ZIP-19, the announcement path): a web font that arrives while an announcement hides the track re-measures the loop at once (its end is again the first copy\'s width), with no fade, and with nothing new the new loop starts at the 120 px the first loop was paused at, not at offset 0',
          early2.loaded === false && r2.announcing === true && Math.abs(r2.segW - early2.segW) > 1 && Math.abs(r2.segW - r2.widths[0]) <= 0.5
            && r2.fades === 0 && r2.fading === false
            && Math.abs(r2.pausedPx - 120) <= 0.5 && !!r2.first && !!r2.start && r2.start.id !== r2.first.id && Math.abs(r2.start.px - (r2.pausedPx % r2.start.w)) <= 0.5,
          JSON.stringify({ early2, r2 }));
        await closePage(p2);

        // The same late font, with the first loop paused 120 px in: the re-measure's swap
        // under the fade keeps the crawl's place too (the owner's ruling of 2026-11-26).
        const p3 = await openPage('late-font place overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: [crawlProbe, fontCss, pauseFirstLoop], setup });
        await p3.page.waitForSelector('.track .seg .item', { timeout: 8000 });
        await p3.page.waitForFunction(() => Array.from(document.fonts).some((f) => f.family.includes('ZipperS7Late') && f.status === 'loaded'), null, { timeout: 6000 }).catch(() => {});
        await sleep(900); // the re-measure, and the fade it takes
        const r3 = await zt(p3.page, () => ({ pausedPx: window.__s7pausedPx ?? null, loaded: window.__fontAtFirstCrawl ?? null, first: window.__zt.frames()[0] || null, start: window.__zt.startOf(), fades: window.__zt.fades(), fading: window.__zt.fading(), segW: window.__zt.segW(), widths: window.__zt.widths() }));
        check('crawl (ZIP-19, the owner\'s ruling of 2026-11-26): a web font served late, with the first loop paused 120 px in, is re-measured under one fade that keeps the crawl\'s place: the new loop, with the new copy\'s width as its end, starts at 120 px',
          r3.loaded === false && !!r3.first && !!r3.start && r3.start.id !== r3.first.id && Math.abs(r3.segW - r3.first.w) > 1 && Math.abs(r3.segW - r3.widths[0]) <= 0.5
            && r3.fades === 1 && r3.fading === false && Math.abs(r3.pausedPx - 120) <= 0.5 && Math.abs(r3.start.px - (r3.pausedPx % r3.start.w)) <= 0.5,
          JSON.stringify(r3));
        await closePage(p3);

        // The same late font, with the first loop paused 120 px in, and an announcement the
        // moment the re-measure's fade starts (a MutationObserver sees .swap-fade go on the
        // track; no new results anywhere): the announcement cuts the fade short and swaps
        // the re-measured copies in at once, and with nothing new that loop starts at the
        // crawl's place, not at offset 0 (the owner's ruling of 2026-11-27).
        const announceOnFade = () => {
          document.addEventListener('readystatechange', () => {
            const t = document.getElementById('track');
            if (!t || t.__s7cutWatched) return;
            t.__s7cutWatched = true;
            new MutationObserver(() => {
              if (window.__s7cut || !t.classList.contains('swap-fade')) return;
              window.__s7cut = true;
              window.dispatchEvent(new CustomEvent('ticker:announce', { detail: { kind: 'custom', text: 'Sprint 7 fade cut', duration: 15 } }));
            }).observe(t, { attributes: true, attributeFilter: ['class'] });
          });
        };
        const p4 = await openPage('late-font fade-cut overlay page', { width: 1920, height: 72 }, overlayUrl(), { init: [crawlProbe, fontCss, pauseFirstLoop, announceOnFade], setup });
        await p4.page.waitForSelector('.track .seg .item', { state: 'attached', timeout: 8000 }); // hidden by the announcement once the font is in
        await p4.page.waitForFunction(() => Array.from(document.fonts).some((f) => f.family.includes('ZipperS7Late') && f.status === 'loaded'), null, { timeout: 6000 }).catch(() => {});
        await sleep(900); // the re-measure, and a fade's swap if one were left to run (it is not)
        const r4 = await zt(p4.page, () => ({ pausedPx: window.__s7pausedPx ?? null, loaded: window.__fontAtFirstCrawl ?? null, cut: window.__s7cut === true, announcing: document.querySelector('.zipper').classList.contains('announcing'), first: window.__zt.frames()[0] || null, start: window.__zt.startOf(), fades: window.__zt.fades(), fading: window.__zt.fading(), segW: window.__zt.segW(), widths: window.__zt.widths(), copies: window.__zt.copies() }));
        check('crawl (ZIP-19, the announcement path): a late web font\'s re-measure fade that an announcement cuts short (no new results, the first loop paused 120 px in) is swapped in at once under the bar: one fade started and is over, the loop ends at the new copy\'s width, every copy holds the same items, and the new loop starts at 120 px, the crawl\'s place, not at offset 0',
          r4.loaded === false && r4.cut === true && r4.announcing === true && r4.fades === 1 && r4.fading === false
            && !!r4.first && !!r4.start && r4.start.id !== r4.first.id && Math.abs(r4.segW - r4.first.w) > 1 && Math.abs(r4.segW - r4.widths[0]) <= 0.5
            && r4.copies.length >= 2 && r4.copies.every((c) => c === r4.copies[0])
            && Math.abs(r4.pausedPx - 120) <= 0.5 && Math.abs(r4.start.px - (r4.pausedPx % r4.start.w)) <= 0.5,
          JSON.stringify({ ...r4, copies: r4.copies.map((c) => c.slice(0, 30)) }));
        await closePage(p4);
        await fetch(`${HTTP}/mock/push`, { method: 'POST', body: sampleText }); // the store as it was
      }
    }
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
