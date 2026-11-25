// The one way the sidecar and the adapters reach the network (ZIP-06). Every request
// goes through guardedFetch, which:
//   • gives each request REQUEST_TIMEOUT_MS (10 s). The timer stays armed until the
//     body has been read, so a body that trickles in cannot escape it.
//   • takes the poll's own AbortSignal (its 45-second deadline, zipper-sidecar.mjs) and
//     joins it to that timer by hand: an AbortController, a timer and an abort
//     listener, all cleared in `finally`. Not AbortSignal.any, which is not in every
//     Node 20.
//   • reads the body itself, capped at MAX_RESPONSE_BYTES, and hands back the text.
//   • reads Retry-After on a 429 or 503, as seconds or an HTTP date.
// Every error it throws is in its own fixed words and names the host (and the limit,
// for a limit), never the address or a header's value: the Challonge key travels in the
// query string, the start.gg key and the StreamElements token in a header, and the error
// reaches the status every page shows. undici's own error text is never passed on.
//
// It is not a platform: platforms/index.js does not list it. The verify suite checks
// that no bare fetch( is left anywhere else in the sidecar or the adapters.

import { readFileSync } from 'node:fs';

export const REQUEST_TIMEOUT_MS = 10000;

// The largest body read, in bytes. The largest answers Zipper reads are the Challonge
// API with every participant and match included, and the Challonge embed page with its
// whole bracket store; a big double-elimination bracket is estimated at a few MB (not
// measured: the repo holds no recorded page yet, its fixtures are trimmed to under 2 KB,
// and row 8.2 checks recorded pages against this figure). 16 MB leaves several times
// that, and bounds what a broken or hostile server can make the sidecar hold.
export const MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

let version = 'unknown';
try {
  version = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf-8')).version || version;
} catch { /* the fallback stands */ }
// start.gg and Matcherino get the plain form. The Challonge embed page gets the
// browser-like form it has always had. The StreamElements request sends neither.
export const USER_AGENT = `Zipper/${version}`;
export const BROWSER_USER_AGENT = `Mozilla/5.0 (compatible; Zipper/${version})`;

// A test override of one of the sidecar's limits (ZIPPER_REQUEST_TIMEOUT_MS here; the
// sidecar reads its own two the same way). It can only shorten the limit: the value is
// clamped to min..fallback, and unset or not a number gives the fallback.
export function envLimit(name, fallback, min) {
  const raw = process.env[name];
  if (raw == null || String(raw).trim() === '') return fallback;
  const n = Math.round(Number(raw));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(fallback, n));
}

export function requestTimeoutMs() {
  return envLimit('ZIPPER_REQUEST_TIMEOUT_MS', REQUEST_TIMEOUT_MS, 50);
}

// "10 s", "0.3 s", "16 MB": how a limit reads in an error.
export function seconds(ms) {
  return `${Number((ms / 1000).toFixed(2))} s`;
}
function megabytes(bytes) {
  return `${Number((bytes / 1048576).toFixed(2))} MB`;
}

// Retry-After as milliseconds from `now`: whole seconds, or an HTTP date (a date already
// past gives 0). Anything else gives null. The caller caps it.
export function parseRetryAfter(value, now = Date.now()) {
  const v = String(value ?? '').trim();
  if (!v) return null;
  if (/^\d+$/.test(v)) return Number(v) * 1000;
  if (!/[a-z]/i.test(v)) return null; // an HTTP date names its day and month
  const t = Date.parse(v);
  return Number.isFinite(t) ? Math.max(0, t - now) : null;
}

// The error an adapter throws for an answer it cannot use, in its own words, carrying
// the answer's status and its Retry-After (null when it has none). The sidecar keeps
// the Retry-After for its next poll.
export function statusError(message, res) {
  const err = new Error(message);
  err.status = res?.status ?? null;
  err.retryAfterMs = res?.retryAfterMs ?? null;
  return err;
}

// fetch, with the limits above. Resolves with
//   { host, status, ok, statusText, headers, text, json(), retryAfterMs }
// once the whole body is in. An answer that is not ok is returned too, for the adapter
// to word: its body serves only as detail, so past the cap it is cut rather than failed.
export async function guardedFetch(url, { method = 'GET', headers, body, signal } = {}) {
  let host;
  try { host = new URL(url).host; }
  catch { throw new Error('a request address could not be read'); }
  const limitMs = requestTimeoutMs();
  const stopped = () => {
    const why = signal?.reason instanceof Error && signal.reason.message ? signal.reason.message : 'the request was stopped';
    return new Error(`${host}: ${why}`);
  };
  if (signal?.aborted) throw stopped();

  const ac = new AbortController();
  let why = null; // 'timeout' | 'signal' | 'done'
  const timer = setTimeout(() => { if (!why) why = 'timeout'; ac.abort(); }, limitMs);
  const onAbort = () => { if (!why) why = 'signal'; ac.abort(); };
  if (signal) signal.addEventListener('abort', onAbort, { once: true });
  // Any other failure (the connection refused or cut, the name not found, a header the
  // request cannot carry) gets fixed words too: the host, and the system's code when there
  // is one. undici's message says only "fetch failed" for a network failure, and quotes a
  // header's whole value, key and all, when that value holds a line break or a NUL.
  const failed = (err) => {
    if (why === 'timeout') return new Error(`${host} took longer than ${seconds(limitMs)} to answer`);
    if (why === 'signal') return stopped();
    const code = [err?.cause?.code, err?.code].find((c) => typeof c === 'string' && /^[A-Z][A-Z0-9_]{1,39}$/.test(c));
    return new Error(`${host}: the request failed${code ? ` (${code})` : ''}`);
  };
  const tooBig = () => new Error(`${host} sent more than ${megabytes(MAX_RESPONSE_BYTES)}`);
  // Ends the connection without leaving a reason behind.
  const drop = () => { if (!why) why = 'done'; ac.abort(); };

  try {
    let res;
    try { res = await fetch(url, { method, headers, body, signal: ac.signal }); }
    catch (err) { throw failed(err); }

    const declared = Number(res.headers.get('content-length'));
    if (res.ok && Number.isFinite(declared) && declared > MAX_RESPONSE_BYTES) { drop(); throw tooBig(); }

    const chunks = [];
    let total = 0;
    if (res.body) {
      const reader = res.body.getReader();
      try {
        for (;;) {
          let step;
          try { step = await reader.read(); }
          catch (err) { throw failed(err); }
          if (step.done) break;
          const chunk = step.value;
          if (total + chunk.byteLength > MAX_RESPONSE_BYTES) {
            if (res.ok) { drop(); throw tooBig(); }
            chunks.push(chunk.subarray(0, MAX_RESPONSE_BYTES - total));
            total = MAX_RESPONSE_BYTES;
            drop();
            break;
          }
          chunks.push(chunk);
          total += chunk.byteLength;
        }
      } finally {
        try { reader.releaseLock(); } catch { /* already released */ }
      }
    }
    const text = new TextDecoder('utf-8').decode(Buffer.concat(chunks, total));
    return {
      host,
      status: res.status,
      ok: res.ok,
      statusText: res.statusText,
      headers: res.headers,
      text,
      json: () => JSON.parse(text),
      retryAfterMs: res.status === 429 || res.status === 503 ? parseRetryAfter(res.headers.get('retry-after')) : null,
    };
  } finally {
    clearTimeout(timer);
    if (signal) signal.removeEventListener('abort', onAbort);
  }
}
