// Development only (verify.mjs, section 6j); nothing deploys it. Preloaded into a test
// sidecar with `node --require`, it makes the first server's listen, which is the sidecar's
// guard port, fail with the error code named by ZIPPER_TEST_GUARD_ERROR (EACCES, say)
// instead of listening, as a port Windows has reserved for something else does. With any
// code but EADDRINUSE the sidecar goes on without the single-instance guard, which is
// what the test looks at.
'use strict';

const code = process.env.ZIPPER_TEST_GUARD_ERROR;
if (code) {
  const net = require('node:net');
  const listen = net.Server.prototype.listen;
  net.Server.prototype.listen = function listenFails() {
    net.Server.prototype.listen = listen; // only the first server
    const err = Object.assign(new Error(`listen ${code}: zipverify guard port error`), { code, syscall: 'listen' });
    process.nextTick(() => this.emit('error', err));
    return this;
  };
}
