// Real-pixel render verification — drives the untouched overlay + control page
// in a real Chromium (system Edge/Chrome via playwright-core; nothing bundled)
// against the mock SB server, injects the sample fixture, and asserts the DOM.
// Writes test-render-1920.png / test-render-640.png / control-render.png
// (gitignored). Run: npm run verify:render
// First time: npm i --no-save playwright-core

import { spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
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

let chromium;
try {
  ({ chromium } = await import('playwright-core'));
} catch {
  console.error('playwright-core is not installed. Run:  npm i --no-save playwright-core');
  process.exit(1);
}

// Mock server
const mock = spawn(process.execPath, [resolve(__dirname, 'mock-sb-server.mjs')], {
  env: { ...process.env, SB_HTTP_PORT: String(HTTP_PORT), SB_WS_PORT: String(WS_PORT) },
  stdio: ['ignore', 'pipe', 'pipe'],
});
for (let i = 0; i < 50; i++) {
  try { if ((await fetch(`${HTTP}/mock/stats`)).ok) break; } catch {}
  await sleep(100);
}

// Preload the sample payload so sync-on-connect paints the strip immediately.
await fetch(`${HTTP}/mock/push`, {
  method: 'POST',
  body: await readFile(resolve(__dirname, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8'),
});

let browser = null;
try {
  for (const channel of ['msedge', 'chrome', undefined]) {
    try { browser = await chromium.launch({ channel }); break; } catch { /* next */ }
  }
  if (!browser) throw new Error('no system Edge/Chrome and no bundled Chromium — run: npx playwright-core install chromium');

  for (const w of [1920, 640]) {
    const page = await browser.newPage({ viewport: { width: w, height: 72 } });
    await page.goto(`${HTTP}/zipper-overlay/ticker.html?w=${w}&sbport=${WS_PORT}`);
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
    await page.close();
  }

  // Announcement takeover: strip pauses, announcement shows, then restores.
  {
    const page = await browser.newPage({ viewport: { width: 1920, height: 72 } });
    await page.goto(`${HTTP}/zipper-overlay/ticker.html?w=1920&sbport=${WS_PORT}`);
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
    await page.close();
  }

  // Control page: preview + status fields render.
  const page = await browser.newPage({ viewport: { width: 700, height: 900 } });
  await page.goto(`${HTTP}/zipper-shared/control.html?sbport=${WS_PORT}`);
  await page.waitForSelector('#preview .m', { timeout: 8000 });
  const rows = await page.locator('#preview .m').count();
  check('control: preview mirrors ticker payload', rows >= 5, String(rows));
  // The mock has no sidecar consuming ticker:command, so no __status ever comes
  // back → the liveness heartbeat should surface the "sidecar down" banner.
  await page.waitForSelector('body.no-sidecar', { timeout: 6000 }).catch(() => {});
  const warnShown = await page.locator('#sidecar-note').isVisible();
  check('control: warns when sidecar is absent', warnShown, String(warnShown));
  await page.screenshot({ path: resolve(__dirname, 'control-render.png') });
  await page.close();
} catch (err) {
  fail++;
  console.error(`\nFATAL: ${err.stack || err}`);
} finally {
  try { await browser?.close(); } catch {}
  try { mock.kill(); } catch {}
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : 'FAILURES'} — ${pass} passed, ${fail} failed (screenshots: test-render-1920.png, test-render-640.png, control-render.png)`);
process.exit(fail === 0 ? 0 : 1);
