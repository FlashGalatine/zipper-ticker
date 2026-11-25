// Regenerates the README/docs screenshots (docs/ticker-1920.png,
// docs/ticker-640.png, docs/control.png) from the sample fixture via the mock
// server. Run: npm run shots   (needs: npm install; playwright-core is a dev dependency)
//
// Uses the mock on HTTP 7476 / WS 8082, and stops with one line when anything already
// answers on them. The mock is stopped when the run ends.

import { spawn } from 'node:child_process';
import { readFile, mkdir } from 'node:fs/promises';
import { connect } from 'node:net';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const DOCS = resolve(ROOT, 'docs');
const HTTP_PORT = 7476;
const WS_PORT = 8082;
const HTTP = `http://127.0.0.1:${HTTP_PORT}`;
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
    console.error(`shots: something already answers on 127.0.0.1 port ${busy.join(', ')} (most likely a mock left by an earlier run); stop it and run again. No screenshot was taken.`);
    process.exit(1);
  }
}

const { chromium } = await import('playwright-core');

// The mock never outlives this run (see verify-render.mjs).
let mock = null;
const stopMock = () => { if (mock && mock.exitCode === null) { try { mock.kill(); } catch {} } };
process.on('exit', stopMock);
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGBREAK']) process.on(sig, () => { stopMock(); process.exit(130); });

let browser = null;
try {
  mock = spawn(process.execPath, [resolve(ROOT, 'mock-sb-server.mjs')], {
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
  await fetch(`${HTTP}/mock/push`, {
    method: 'POST',
    body: await readFile(resolve(ROOT, 'platforms', 'fixtures', 'sample-update.json'), 'utf-8'),
  });
  await mkdir(DOCS, { recursive: true });

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
} catch (err) {
  console.error(`shots: ${err.stack || err}`);
  process.exitCode = 1;
} finally {
  try { await browser?.close(); } catch {}
  stopMock();
}
