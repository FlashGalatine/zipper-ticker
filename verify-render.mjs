// Real-pixel render verification — drives the untouched overlay + control page
// in a real Chromium (system Edge/Chrome via playwright-core; nothing bundled)
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

const __dirname = dirname(fileURLToPath(import.meta.url));
const HTTP_PORT = 7476;
const WS_PORT = 8082;
const HTTP = `http://127.0.0.1:${HTTP_PORT}`;

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
async function openPage(label, viewport, url) {
  const page = await browser.newPage({ viewport });
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

try {
  mock = spawn(process.execPath, [resolve(__dirname, 'mock-sb-server.mjs')], {
    env: { ...process.env, SB_HTTP_PORT: String(HTTP_PORT), SB_WS_PORT: String(WS_PORT), MOCK_EXIT_WITH_PARENT: '1', MOCK_ARG_BLEED: '' },
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
