'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const JSONStream = require('json-stream');
const log = require('./logger').Logger('watcher');

// in case of permanent k8s api server failure we retry with max interval
// of this # of seconds
const MAX_RECONNECT_DELAY = 30;

// This is a classic operator loop design as seen in i.e. operator-sdk (golang)
// to watch a k8s resource. We combine http GET with watcher events to do
// it in an efficient way. First we do GET to populate the cache and then
// maintain it using watch events. When the watch connection is closed by
// the server (happens every minute or so), we do GET and continue watch again.
//
// It is a general implementation of watcher which can be used for any resource
// operator. The operator should subscribe to "new", "mod" and "del" events
// which all pass object parameter and are triggered when a resource is
// added, modified or deleted.
//
class Watcher extends EventEmitter {
  // Construct a watcher for resource.
  //   name: name of the watched resource
  //   getEp: k8s api endpoint with .get() method to get the objects
  //   streamEp: k8s api endpoint with .getObjectStream() method to obtain
  //             stream of watch events
  //   filterCb: converts k8s object to representation understood by the
  //             operator. Or returns null if object should be ignored.
  constructor(name, getEp, streamEp, filterCb) {
    super();
    this.name = name;
    this.getEp = getEp;
    this.streamEp = streamEp;
    this.filterCb = filterCb;
    this.objects = null; // the cache of objects being watched
    this.noRestart = false; // do not renew watcher connection
    this.startResolve = null; // start promise in case of delayed start due
    // to an error
    this.objectStream = null; // non-null if watch connection is active
    this.getInProg = false; // true if GET objects query is in progress
    this.reconnectDelay = 0; // Exponential backoff in case of api server
    // failures (in secs)
    this.pendingEvents = null; // watch events while sync is in progress
    // (if not null -> GET is in progress)
  }

  // Start asynchronously the watcher
  async start() {
    var self = this;
    self.objectStream = await self.streamEp.getObjectStream();

    // TODO: missing upper bound on exponential backoff
    self.reconnectDelay = Math.min(
      Math.max(2 * self.reconnectDelay, 1),
      MAX_RECONNECT_DELAY
    );
    self.pendingEvents = [];
    assert(!self.getInProg);
    self.getInProg = true;
    // start the stream of events before GET query so that we don't miss any
    // event while performing the GET.
    self.objectStream.on('data', ev => {
      log.trace(
        `Event ${ev.type} in ${self.name} watcher: ${JSON.stringify(ev.object)}`
      );

      // if update of the node list is in progress, queue the event for later
      if (self.pendingEvents != null) {
        log.trace(`Event deferred until ${self.name} watcher is synced`);
        self.pendingEvents.push(ev);
        return;
      }

      self._processEvent(ev);
    });

    self.objectStream.on('error', err => {
      log.error(`stream error in ${self.name} watcher: ${err}`);
    });

    // k8s api server disconnects watcher after a timeout. If that happens
    // reconnect and start again.
    self.objectStream.once('end', () => {
      self.objectStream = null;
      if (self.getInProg) {
        // if watcher disconnected before we finished syncing, we have
        // to wait for the GET request to finish and then start over
        log.error(`${self.name} watch stream closed before the sync completed`);
      } else {
        // reconnect and start watching again
        log.debug(`${self.name} watch stream disconnected`);
      }
      self.scheduleRestart();
    });

    var items;
    try {
      let res = await self.getEp.get();
      items = res.body.items;
    } catch (err) {
      log.error(
        `Failed to get list of ${self.name} objects: HTTP ${err.statusCode}`
      );
      self.getInProg = false;
      self.scheduleRestart();
      return self.delayedStart();
    }

    // if watcher did end before we retrieved list of objects then start over
    self.getInProg = false;
    if (!self.objectStream) {
      self.scheduleRestart();
      return self.delayedStart();
    }

    log.trace(`List of watched ${self.name} objects: ${JSON.stringify(items)}`);

    // filter the obtained objects
    var objects = {};
    for (let i = 0; i < items.length; i++) {
      let obj = this.filterCb(items[i]);
      if (obj != null) {
        objects[items[i].metadata.name] = {
          object: obj,
          k8sObject: items[i],
        };
      }
    }

    let origObjects = self.objects;
    self.objects = {};

    if (origObjects == null) {
      // the first time all objects appear to be new
      for (let name in objects) {
        self.objects[name] = objects[name].k8sObject;
        self.emit('new', objects[name].object);
      }
    } else {
      // Merge old node list with the new node list
      // First delete objects which no longer exist
      for (let name in origObjects) {
        if (!(name in objects)) {
          self.emit('del', self.filterCb(origObjects[name]));
        }
      }
      // Second detect new objects and modified objects
      for (let name in objects) {
        let k8sObj = objects[name].k8sObject;
        let obj = objects[name].object;
        let origObj = origObjects[name];

        self.objects[name] = k8sObj;

        if (origObj) {
          let generation = k8sObj.metadata.generation;
          // Some objects don't have generation #
          if (!generation || generation > origObj.metadata.generation) {
            self.emit('mod', obj);
          }
        } else {
          self.emit('new', obj);
        }
      }
    }

    var ev;
    while ((ev = self.pendingEvents.pop())) {
      self._processEvent(ev);
    }
    self.pendingEvents = null;
    self.reconnectDelay = 0;

    log.info(`${self.name} watcher sync completed`);

    // if the start was delayed, then resolve the promise now
    if (self.startResolve) {
      self.startResolve();
      self.startResolve = null;
    }

    // this event is for test cases
    self.emit('sync');
  }

  // Stop does not mean stopping watcher immediately, but rather not restarting
  // it again when watcher connection is closed.
  // TODO:  find out how to reset the watcher connection
  async stop() {
    this.noRestart = true;
  }

  // Return k8s object(s) from the cache or null if it does not exist.
  getRaw(name) {
    var obj = this.objects[name];
    if (!obj) {
      return null;
    } else {
      return JSON.parse(JSON.stringify(obj));
    }
  }

  // Return the collection of objects
  list() {
    return Object.values(this.objects).map(ent => this.filterCb(ent));
  }

  delayedStart() {
    var self = this;

    if (self.startResolve) {
      return self.startResolve;
    } else {
      return new Promise((resolve, reject) => {
        self.startResolve = resolve;
      });
    }
  }

  // Restart the watching process after a timeout
  scheduleRestart() {
    // We cannot restart while either watcher connection or GET query is still
    // in progress. We will get called again when either of them terminates.
    // TODO: How to terminate the watcher connection?
    // Now we simply rely on server to close the conn after timeout
    if (!this.objectStream && !this.getInProg) {
      if (!this.noRestart) {
        setTimeout(this.start.bind(this), 1000 * this.reconnectDelay);
      }
    }
  }

  // Invoked when there is a watch event (a resource has changed).
  _processEvent(ev) {
    const k8sObj = ev.object;
    const name = k8sObj.metadata.name;
    const generation = k8sObj.metadata.generation;
    const type = ev.type;

    let obj = this.filterCb(k8sObj);
    if (obj == null) {
      return; // not interested in this object
    }
    let oldObj = this.objects[name];

    if (type === 'ADDED' || type === 'MODIFIED') {
      this.objects[name] = k8sObj;
      if (!oldObj) {
        // it is a new object with no previous history
        this.emit('new', obj);
        // Some objects don't have generation #
      } else if (!generation || oldObj.metadata.generation < generation) {
        // we assume that if generation # remained the same => no change
        // TODO: add 64-bit integer overflow protection
        this.emit('mod', obj);
      } else {
        log.debug(`Ignoring stale ${this.name} object event`);
      }

      // TODO: subtle race condition when delete event is related to object which
      // existed before we populated the cache..
    } else if (type === 'DELETED') {
      if (oldObj) {
        delete this.objects[name];
        this.emit('del', obj);
      }
    } else if (type === 'ERROR') {
      log.error(`Error event in ${this.name} watcher: ${JSON.stringify(ev)}`);
    } else {
      log.error(`Unknown event in ${this.name} watcher: ${JSON.stringify(ev)}`);
    }
  }
}

module.exports = Watcher;
