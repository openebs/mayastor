// Common utility functions used throughout the tests

'use strict';

const sleep = require('sleep-promise');

// Wait until the test function yields true, calling it in exponential
// backoff intervals.
async function waitUntil (test, timeout, reason) {
  let delay = 1;
  if (typeof timeout === 'string') {
    reason = timeout;
    timeout = undefined;
  }
  timeout = timeout || 1024;
  reason = reason || 'something';

  while (true) {
    const done = await test();
    if (done) {
      return;
    }
    if (timeout <= 0) {
      throw new Error(`Timed out waiting for ${reason}`);
    }
    await sleep(delay);
    timeout -= delay;
    delay *= 2;
    if (delay > 100) {
      delay = 100;
    }
  }
}

// Check that the test callback which should return a future fails with
// given grpc error code.
//
// TODO: Combination of external function that is awaited here and wrapped
// inside try-catch, results in freaky behaviour sometimes like printing
// error to output.
async function shouldFailWith (code, test) {
  try {
    await test();
  } catch (err) {
    if (err.code !== code) {
      throw new Error(`Expected error code ${code} but got: ${err}`);
    }
    return;
  }
  throw new Error('Expected error');
}

module.exports = {
  shouldFailWith,
  waitUntil
};
