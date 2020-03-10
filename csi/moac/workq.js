'use strict';

const assert = require('assert');

// Implementation of a simple work queue which takes a task, puts it to the
// queue and processes the task when all other tasks that were queued before
// have completed. This is useful if the task consists of async steps and one
// wants to be sure that at any given time only one task is being processed
// not to interfere with the other tasks.
class Workq {
  constructor() {
    this.queue = [];
    this.inprog = false;
  }

  // Put a task to the queue for processing.
  //
  // Since the method is async the caller can decide if she wants to block
  // waiting until the task is processed or continue immediately.
  //
  // @param {*}        arg   Opaque context parameter passed to the func.
  // @param {function} func  Async function returning a promise.
  // @returns {*} A promise fulfilled when the task is done.
  //              The value of the promise is the value returned by the func.
  async push(arg, func) {
    assert(typeof func, 'function');

    var resolveCb;
    var rejectCb;
    var promise = new Promise((resolve, reject) => {
      resolveCb = resolve;
      rejectCb = reject;
    });
    var task = { func, arg, resolveCb, rejectCb };

    this.queue.push(task);
    if (!this.inprog) {
      this.inprog = true;
      this._nextTask();
    }
    return promise;
  }

  // Pick and dispatch next task from the queue.
  _nextTask() {
    var self = this;

    var task = this.queue.shift();
    if (!task) {
      self.inprog = false;
      return;
    }

    task
      .func(task.arg)
      .then(res => task.resolveCb(res))
      .catch(err => task.rejectCb(err))
      .finally(() => self._nextTask());
  }
}

module.exports = Workq;
