// Stream of events from registry.

const _ = require('lodash');
const assert = require('assert');
const { Readable } = require('stream');

// Stream of events from registry. Each event object retrieved from the stream is
// in the following form:
//
//  {
//     eventType: "sync", "new", "mod", "del"
//     kind: "node", "pool", "replica" or "nexus"
//     object: node, pool, replica or nexus object
//  }
//
// When reading the first time all node objects that exist in the cache before
// the stream was created are returned using the "new" event. That makes the
// stream suitable for populating the caches at the beginning.
//
// The primary motivation for introducing the class is to have a common code
// buffering registry events without duplicating it in all event consumers.
//
// TODO: end the stream when registry is stopped (requires new registry event).
class EventStream extends Readable {
  // Create the stream.
  //
  // @param {object} registry    Registry object.
  // @param {object} [opts]      nodejs stream options.
  //
  constructor(registry, opts) {
    assert(registry);
    super(_.assign({ objectMode: true }, opts || {}));
    this.events = [];
    this.waiting = false;
    this.started = false;
    this.destroyed = false;
    this.registry = registry;
    // we save the listener functions to be able to clear them later
    this.eventListeners = {
      node: this._onEvent.bind(this, 'node'),
      nexus: this._onEvent.bind(this, 'nexus'),
      pool: this._onEvent.bind(this, 'pool'),
      replica: this._onEvent.bind(this, 'replica'),
    };
  }

  // Start listeners and emit events about existing objects.
  _start() {
    assert(!this.waiting);
    assert(this.events.length == 0);
    this.started = true;
    for (let kind in this.eventListeners) {
      this.registry.on(kind, this.eventListeners[kind]);
    }
    // Populate stream with objects which already exists but for consumer
    // they appear new.
    var self = this;
    self.registry.getNode().forEach(node => {
      node.pools.forEach(obj => {
        self.events.push({
          kind: 'pool',
          eventType: 'new',
          object: obj,
        });
        obj.replicas.forEach(obj => {
          self.events.push({
            kind: 'replica',
            eventType: 'new',
            object: obj,
          });
        });
      });
      node.nexus.forEach(obj => {
        self.events.push({
          kind: 'nexus',
          eventType: 'new',
          object: obj,
        });
      });
      // generate artificial 'sync' event for the node so that the reader knows
      // that all "new" events for initial objects have been generated.
      self.events.push({
        kind: 'node',
        eventType: 'sync',
        object: node,
      });
    });
    if (self.waiting) {
      self.waiting = false;
      self._read();
    }
  }

  _onEvent(kind, ev) {
    this.events.push({
      kind: kind,
      eventType: ev.eventType,
      object: ev.object,
    });
    if (this.waiting) {
      this.waiting = false;
      this._read();
    }
  }

  _read(size) {
    if (!this.started) {
      this._start();
    }
    let cont = true;
    while (cont) {
      let ev = this.events.shift();
      if (ev) {
        cont = this.push(ev);
      } else {
        this.waiting = true;
        cont = false;
        if (this.destroyed) {
          this.push(null);
        }
      }
    }
  }

  _destroy(err, cb) {
    if (this.started) {
      for (let kind in this.eventListeners) {
        this.registry.removeListener(kind, this.eventListeners[kind]);
      }
    }
    this.destroyed = true;
    // end the stream if it is waiting for more data but there are none
    if (this.waiting) {
      this.push(null);
    }
    cb(err);
  }
}

module.exports = EventStream;
