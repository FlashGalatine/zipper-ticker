// Development only (verify.mjs, section 6j); nothing deploys it. Preloaded into a test
// sidecar with `node --require`, it makes that process crash when the test asks, from
// outside every try in the sidecar, so the sidecar's own crash handlers are what run.
// Once the file named by ZIPPER_TEST_CRASH_FILE exists, its text is read:
//   throw:<message>   a timer throws an Error with that message (uncaughtException)
//   reject:<message>  a promise is rejected with it and never handled (unhandledRejection)
// Anything else is ignored, and the file is looked for again.
// With ZIPPER_TEST_CRASH_AT=guard it instead throws from the callback of the first server's
// listen, which is the sidecar's guard port, while main() waits for it: a crash before the
// config is read, which reaches the log only if main() installed its handlers first.
'use strict';

const { existsSync, readFileSync } = require('node:fs');

if (process.env.ZIPPER_TEST_CRASH_AT === 'guard') {
  const net = require('node:net');
  const listen = net.Server.prototype.listen;
  net.Server.prototype.listen = function listenThenCrash(...args) {
    net.Server.prototype.listen = listen; // only the first server
    const at = args.findIndex((a) => typeof a === 'function');
    if (at >= 0) args[at] = () => { throw new Error('zipverify crash in the guard port\'s callback'); };
    return listen.apply(this, args);
  };
}

const file = process.env.ZIPPER_TEST_CRASH_FILE;
if (file) {
  const timer = setInterval(() => {
    if (!existsSync(file)) return;
    let text = '';
    try { text = readFileSync(file, 'utf-8'); } catch { return; }
    const at = text.indexOf(':');
    const how = at < 0 ? text.trim() : text.slice(0, at);
    const message = at < 0 ? '' : text.slice(at + 1);
    if (how !== 'throw' && how !== 'reject') return;
    clearInterval(timer);
    if (how === 'throw') setTimeout(() => { throw new Error(message); }, 0);
    else Promise.reject(new Error(message));
  }, 50);
  timer.unref();
}
