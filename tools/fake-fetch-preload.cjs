// Development only (verify.mjs, section 6j); nothing deploys it. Preloaded into a test
// sidecar with `node --require`, it replaces fetch for that whole process, so nothing the
// sidecar asks for leaves the machine: no request is made, no name is looked up. That is
// how the suite drives the real request paths (a Challonge API URL with its key in the
// query string, StreamElements with its token in a header) without any outside host.
//
// Every call answers as the file named by ZIPPER_TEST_FAKE_FETCH_FILE says, read at each
// call (no file, or an unknown word: "fail"):
//   fail     throws as undici does for a connection that fails, but with the whole
//            address and every header quoted in the message, the worst a library could
//            do; its cause carries the code ZIPPER_FAKE_FETCH, which the fetch wrapper
//            passes on, so a test can tell this stand-in answered and which host was asked
//   hang     never answers; ends only when the request's own signal aborts
//   big      an ok answer that declares a body far over the wrapper's size cap
//   echo401  a 401 whose body quotes the api_key of the address, as a site that names a
//            rejected key in its error might
//   cut401   the same, with the key starting at character 190, so an adapter that cuts the
//            body at 200 characters cuts the key in two
//   echo401raw  a 401 whose body quotes the address's query string as it was sent, so a
//            key appears as the address encodes it (%2B for +, and so on)
//   json:<path>  an ok answer whose body is that file (a recorded fixture), so a poll
//            succeeds and its result is pushed
'use strict';

const { readFileSync } = require('node:fs');

const modeFile = process.env.ZIPPER_TEST_FAKE_FETCH_FILE;
function mode() {
  if (!modeFile) return 'fail';
  try { return readFileSync(modeFile, 'utf-8').trim() || 'fail'; } catch { return 'fail'; }
}

globalThis.fetch = async function fakeFetch(input, init = {}) {
  const url = typeof input === 'string' ? input : input instanceof URL ? input.href : String(input?.url ?? input);
  const how = mode();
  if (how === 'hang') {
    return new Promise((_, reject) => {
      const signal = init.signal;
      const stop = () => reject(Object.assign(new Error(`aborted ${url}`), { name: 'AbortError' }));
      if (!signal) return;
      if (signal.aborted) stop(); else signal.addEventListener('abort', stop, { once: true });
    });
  }
  if (how === 'big') return new Response('x', { status: 200, headers: { 'content-length': String(64 * 1024 * 1024) } });
  if (how.startsWith('json:')) {
    return new Response(readFileSync(how.slice(5), 'utf-8'), { status: 200, headers: { 'content-type': 'application/json' } });
  }
  if (how === 'echo401raw') {
    let search = '';
    try { search = new URL(url).search; } catch { /* no query */ }
    return new Response(`invalid request ${search}`, { status: 401 });
  }
  if (how === 'echo401' || how === 'cut401') {
    let key = '';
    try { key = new URL(url).searchParams.get('api_key') || ''; } catch { /* no key */ }
    const body = how === 'echo401' ? `invalid api_key ${key} for this tournament` : 'y'.repeat(190) + key;
    return new Response(body, { status: 401 });
  }
  const err = new TypeError(`fetch failed: ${url} ${JSON.stringify(init.headers || {})}`);
  err.cause = Object.assign(new Error(`connect failed ${url}`), { code: 'ZIPPER_FAKE_FETCH' });
  throw err;
};
