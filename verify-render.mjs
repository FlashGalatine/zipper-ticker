// Real-pixel render verification — drives the overlay + control page as they ship
// (the out-of-order pages only get a stand-in clock, see skewClock) in a real Chromium (system Edge/Chrome via playwright-core; nothing bundled)
// against the mock SB server, injects the sample fixture, and asserts the DOM.
// Every page it loads also records page errors and console errors, and the run
// fails on any. Writes test-render-1920.png / test-render-640.png / control-render.png
// (gitignored). Run: npm run verify:render
// First time: npm install (playwright-core is a dev dependency)
//
// Uses the mock on HTTP 7476 / WS 8082, and stops with one line, testing nothing,
// when anything already answers on them. The mock is stopped when the run ends.

import { spawn } from 'node:child_process';
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
async function openPage(label, viewport, url, { init } = {}) {
  const page = await browser.newPage({ viewport });
  if (init) await page.addInitScript(init);
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
  // The mock has no sidecar consuming ticker:command, so no __status ever comes
  // back → the liveness heartbeat should surface the "sidecar down" banner.
  await page.waitForSelector('body.no-sidecar', { timeout: 6000 }).catch(() => {});
  const warnShown = await page.locator('#sidecar-note').isVisible();
  check('control: warns when sidecar is absent', warnShown, String(warnShown));
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
} catch (err) {
  fail++;
  console.error(`\nFATAL: ${err.stack || err}`);
} finally {
  try { await browser?.close(); } catch {}
  stopMock();
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : 'FAILURES'} — ${pass} passed, ${fail} failed (screenshots: test-render-1920.png, test-render-640.png, test-render-message.png, control-render.png)`);
process.exit(fail === 0 ? 0 : 1);
