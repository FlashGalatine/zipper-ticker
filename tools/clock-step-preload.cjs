// Development only (verify.mjs, section 6h); nothing deploys it. Preloaded into one test
// sidecar with `node --require`, it sets that process's wall clock back or forward, as a
// change by hand or a time sync would: once the file named by ZIPPER_TEST_CLOCK_STEP_FILE
// exists, Date.now() and `new Date()` are moved by the number of milliseconds the file
// holds. performance.now(), the monotonic clock, is left alone. It prints one line when
// the step is taken, so the test knows when it has happened.
'use strict';

const { existsSync, readFileSync } = require('node:fs');

const file = process.env.ZIPPER_TEST_CLOCK_STEP_FILE;
if (file) {
  const RealDate = Date;
  let shift = 0;
  class SteppedDate extends RealDate {
    constructor(...args) {
      if (args.length === 0) super(RealDate.now() + shift);
      else super(...args);
    }
    static now() { return RealDate.now() + shift; }
  }
  globalThis.Date = SteppedDate;
  const timer = setInterval(() => {
    if (!existsSync(file)) return;
    let n = NaN;
    try { n = Number(readFileSync(file, 'utf-8').trim()); } catch { return; }
    if (!Number.isFinite(n)) return;
    shift = n;
    clearInterval(timer);
    console.log(`[clock-step] the wall clock was set by ${n} ms`);
  }, 50);
  timer.unref();
}
