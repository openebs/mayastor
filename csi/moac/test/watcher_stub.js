// Fake watcher which simulates the real one.

'use strict';

const assert = require('assert');
const EventEmitter = require('events');

// It can be used instead of real watcher in tests of other classes depending
// on the watcher.
class Watcher extends EventEmitter {
  // Construct a watcher with initial set of objects passed in arg.
  constructor(filterCb, objects) {
    super();
    this.filterCb = filterCb;
    this.objects = {};
    for (let i = 0; i < objects.length; i++) {
      this.objects[objects[i].metadata.name] = objects[i];
    }
  }

  injectObject(obj) {
    this.objects[obj.metadata.name] = obj;
  }

  newObject(obj) {
    this.objects[obj.metadata.name] = obj;
    this.emit('new', this.filterCb(obj));
  }

  delObject(name) {
    var obj = this.objects[name];
    assert(obj);
    delete this.objects[name];
    this.emit('del', this.filterCb(obj));
  }

  modObject(obj) {
    this.objects[obj.metadata.name] = obj;
    this.emit('mod', this.filterCb(obj));
  }

  async start() {
    var self = this;
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        for (let name in self.objects) {
          // real objects coming from GET method also don't have kind and
          // apiVersion attrs so strip these props to mimic the real case.
          delete self.objects[name].kind;
          delete self.objects[name].apiVersion;
          self.emit('new', self.filterCb(self.objects[name]));
        }
        resolve();
      }, 0);
    });
  }

  async stop() {}

  getRaw(name) {
    let obj = this.objects[name];
    if (!obj) {
      return null;
    } else {
      return JSON.parse(JSON.stringify(obj));
    }
  }

  list() {
    var self = this;
    return Object.values(this.objects).map(ent => self.filterCb(ent));
  }
}

module.exports = Watcher;
