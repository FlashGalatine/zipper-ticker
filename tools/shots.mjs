// Regenerates the README/docs screenshots (docs/ticker-1920.png,
// docs/ticker-640.png, docs/control.png) from the sample fixture via the mock
// server. Run: npm run shots   (needs: npm i --no-save playwright-core)

import { spawn } from 'node:child_process';
import { readFile, mkdir } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const DOCS = resolve(ROOT, 'docs');
const HTTP_PORT = 7476;
const WS_PORT = 8082;
const HTTP = `http://127.0.0.1:${HTTP_PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const { chromium } = await import('playwright-core');

const mock = spawn(process.execPath, [resolve(ROOT, 'mock-sb-server.mjs')], {
  env: { ...process.env, SB_HTTP_PORT: String(HTTP_PORT), SB_WS_PORT: String(WS_PORT) },
  stdio: ['ignore', 'pipe', 'pipe'],
});
for (let i = 0; i < 50; i++) {
  try { if ((await fetch(`${HTTP}/mock/stats`)).ok) break; } catch {}
  await sleep(100);
}
await fetch(`${HTTP}/mock/push`, {
  method: 'POST',
  body: await readFile(resolve(ROOT, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8'),
});
await mkdir(DOCS, { recursive: true });

let browser = null;
try {
  for (const channel of ['msedge', 'chrome', undefined]) {
    try { browser = await chromium.launch({ channel }); break; } catch { /* next */ }
  }
  if (!browser) throw new Error('no Chromium available');

  for (const w of [1920, 640]) {
    const page = await browser.newPage({ viewport: { width: w, height: 72 } });
    await page.goto(`${HTTP}/zipper-overlay/ticker.html?w=${w}&sbport=${WS_PORT}`);
    await page.waitForSelector('.track .seg .item', { timeout: 8000 });
    await sleep(600);
    await page.screenshot({ path: resolve(DOCS, `ticker-${w}.png`) });
    await page.close();
    console.log(`docs/ticker-${w}.png`);
  }

  const page = await browser.newPage({ viewport: { width: 700, height: 900 } });
  await page.goto(`${HTTP}/zipper-shared/control.html?sbport=${WS_PORT}`);
  await page.waitForSelector('#preview .m', { timeout: 8000 });
  await page.screenshot({ path: resolve(DOCS, 'control.png') });
  await page.close();
  console.log('docs/control.png');
} finally {
  try { await browser?.close(); } catch {}
  try { mock.kill(); } catch {}
}
