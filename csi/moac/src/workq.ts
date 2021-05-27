
import assert from 'assert';
import { Logger } from './logger';

const log = Logger('workq');

type Task<A, R> = {
  func: (arg: A) => Promise<R>;
  arg: A;
  resolveCb: (res: R) => void;
  rejectCb: (err: any) => void;
}

// Implementation of a simple work queue which takes a task, puts it to the
// queue and processes the task when all other tasks that were queued before
// have completed. This is useful if the task consists of async steps and one
// wants to be sure that at any given time only one task is being processed
// not to interfere with the other tasks.
export class Workq {
  private name: string;
  private queue: Task<any, any>[];
  private inprog: boolean;

  constructor (name?: string) {
    this.name = name || '';
    this.queue = [];
    this.inprog = false;
  }

  // Put a task to the queue for processing.
  //
  // Since the method is async the caller can decide if she wants to block
  // waiting until the task is processed or continue immediately.
  //
  // @param arg   Opaque context parameter passed to the func.
  // @param func  Async function returning a promise.
  // @returns A promise fulfilled when the task is done.
  //          The value of the promise is the value returned by the func.
  async push<A, R> (arg: A, func: (arg: A) => Promise<R>): Promise<R> {
    assert.strictEqual(typeof func, 'function');

    return new Promise((resolve, reject) => {
      let resolveCb = resolve;
      let rejectCb = reject;
      let task: Task<A, R> = { func, arg, resolveCb, rejectCb };

      this.queue.push(task);
      if (!this.inprog) {
        this.inprog = true;
        this._nextTask();
      } else {
        log.trace(`${this.name} task has been queued for later`);
      }
    });
  }

  // Pick and dispatch next task from the queue.
  _nextTask () {
    var self = this;

    var task = this.queue.shift();
    if (!task) {
      self.inprog = false;
      return;
    }

    log.trace(`Dispatching a new ${this.name} task`);
    task
      .func(task.arg)
      .then((res: any) => task!.resolveCb(res))
      .catch((err: any) => task!.rejectCb(err))
      .finally(() => self._nextTask());
  }
}
