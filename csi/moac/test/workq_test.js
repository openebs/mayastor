// Unit tests for the work queue class

'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const Workq = require('../workq');

class Task {
  constructor(id, delay) {
    this.id = id;
    this.delay = delay || 1;
  }

  async doIt(arg) {
    if (arg == 'throw here') {
      throw new Error('Testing exception in sync context');
    }
    await sleep(this.delay);
    if (arg == 'throw there') {
      throw new Error('Testing exception in async context');
    }
    return {
      id: this.id,
      arg: arg,
      timestamp: Date.now(),
    };
  }
}

module.exports = function() {
  var clock;

  beforeEach(() => {
    clock = sinon.useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
  });

  it('should execute a task that is a closure', async () => {
    let wq = new Workq();
    let result = await wq.push(100, async arg => {
      expect(arg).to.equal(100);
      return arg;
    });
    expect(result).to.equal(100);
  });

  it('should execute a task that is a bound method', done => {
    let task = new Task(0);
    let wq = new Workq();

    wq.push(100, task.doIt.bind(task)).then(result => {
      expect(result.id).to.equal(0);
      expect(result.arg).to.equal(100);
      done();
    });
    clock.tick(1);
  });

  it('should propagate an exception from sync context', done => {
    let task = new Task(0);
    let wq = new Workq();

    wq.push('throw here', task.doIt.bind(task))
      .then(res => done(new Error('it should have thrown the exception')))
      .catch(err => done());
    clock.tick(1);
  });

  it('should propagate an exception from async context', done => {
    let task = new Task(0);
    let wq = new Workq();

    wq.push('throw there', task.doIt.bind(task))
      .then(res => done(new Error('it should have thrown the exception')))
      .catch(err => done());
    clock.tick(1);
  });

  it('should finish tasks in the same order they were pushed', async () => {
    let task1 = new Task(1, 10);
    let task2 = new Task(2, 10);
    let task3 = new Task(3, 10);
    let wq = new Workq();

    let promise1 = wq.push(100, task1.doIt.bind(task1));
    let promise2 = wq.push(100, task2.doIt.bind(task2));
    let promise3 = wq.push(100, task3.doIt.bind(task3));

    clock.tick(10);
    let res = await promise1;
    expect(res.id).to.equal(1);
    clock.tick(10);
    res = await promise2;
    expect(res.id).to.equal(2);
    clock.tick(10);
    res = await promise3;
    expect(res.id).to.equal(3);
  });

  it('should put a new task on hold if a previous task is in progress', async () => {
    let task1 = new Task(1, 100);
    let task2 = new Task(2);
    let wq = new Workq();

    let promise1 = wq.push(100, task1.doIt.bind(task1));
    clock.tick(50);
    let promise2 = wq.push(100, task2.doIt.bind(task2));
    clock.tick(50);
    let res1 = await promise1;
    expect(res1.id).to.equal(1);
    clock.tick(50);
    let res2 = await promise2;
    expect(res2.id).to.equal(2);
    expect(res1.timestamp).to.be.below(res2.timestamp);
  });

  it('should continue with the next task even if previous one failed', done => {
    let task1 = new Task(1);
    let task2 = new Task(2);
    let task3 = new Task(3);
    let wq = new Workq();
    let wasException = false;

    clock.restore();

    let promise1 = wq.push('throw here', task1.doIt.bind(task1));
    let promise2 = wq.push('throw there', task2.doIt.bind(task2));
    let promise3 = wq.push(100, task3.doIt.bind(task3));

    promise1
      .then(res => done(new Error('it should have thrown the exception')))
      .catch(e => {
        promise2
          .then(res => done(new Error('it should have thrown the exception')))
          .catch(e => {
            promise3
              .then(res => {
                expect(res.id).to.equal(3);
                expect(res.arg).to.equal(100);
                done();
              })
              .catch(e => done(e));
          });
      });
  });
};
