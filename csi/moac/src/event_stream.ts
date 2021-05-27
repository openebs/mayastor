// Stream of events from registry and/or volume manager.
//
// The implementation is not as clean as it should be because there can be two
// type of objects serving as a source of events: registry and volume manager.
//
// TODO: Solution #1: make volume objects part of registry (though that bears
//       its own problems).
// TODO: Solution #2: abstract event stream from source object type by providing
//       hooks with source specific code when calling the constructor (the hooks
//       for registry source object need to be shared to avoid code duplication)

import * as _ from 'lodash';
import assert from 'assert';
import { Readable } from 'stream';

import { Node } from './node';
import { Pool } from './pool';
import { Replica } from './replica';
import { Nexus } from './nexus';
import { Volume } from './volume';
import { Volumes } from './volumes';
import { Registry } from './registry';

type ReceivedEventObject = {
  eventType: string,
  object: any,
};

type EventObject = {
  kind: string,
  eventType: string,
  object: any,
};

type EventSource = {
  registry?: Registry, // Registry object.
  volumes?: Volumes,   // Volume manager.
};

// Stream of events from registry and/or volume manager. Each event object
// retrieved from the stream is in the following form:
//
//  {
//     eventType: "sync", "new", "mod", "del"
//     kind: "node", "pool", "replica", "nexus" or "volume"
//     object: node, pool, replica, nexus or volume object
//  }
//
// When reading the first time all node objects that exist in the cache before
// the stream was created are returned using the "new" event. That makes the
// stream suitable for populating the caches at the beginning.
//
// The primary motivation for introducing the class is to have a common code
// buffering registry events without duplicating it in all event consumers.
//
// TODO: End the stream when registry is stopped (requires new registry event).
//       Is there equivalent for the volume manager?
//
export class EventStream extends Readable {
  events: EventObject[];
  waiting: boolean;
  started: boolean;
  destroyed: boolean;
  registry?: Registry;
  volumes?: Volumes;
  registryEventListeners: Record<string, (ev: ReceivedEventObject) => void>;
  volumesEventListeners: Record<string, (ev: ReceivedEventObject) => void>;

  // Create the stream.
  //
  // @param source   Source object for the events.
  // @param [opts]   nodejs stream options.
  //
  constructor (source: EventSource, opts?: any) {
    assert(source);
    super(_.assign({ objectMode: true }, opts || {}));
    this.events = [];
    this.waiting = false;
    this.started = false;
    this.destroyed = false;
    if (source.registry) {
      this.registry = source.registry;
    }
    if (source.volumes) {
      this.volumes = source.volumes;
    }
    assert(this.registry || this.volumes);
    // we save the listener functions in order to clear them at the end
    this.registryEventListeners = {
      node: this._onEvent.bind(this, 'node'),
      nexus: this._onEvent.bind(this, 'nexus'),
      pool: this._onEvent.bind(this, 'pool'),
      replica: this._onEvent.bind(this, 'replica')
    };
    this.volumesEventListeners = {
      volume: this._onEvent.bind(this, 'volume')
    };
  }

  // Start listeners and emit events about existing objects.
  _start () {
    assert(!this.waiting);
    assert(this.events.length === 0);
    this.started = true;
    if (this.registry) {
      for (const kind in this.registryEventListeners) {
        this.registry.on(kind, this.registryEventListeners[kind]);
      }
    }
    if (this.volumes) {
      for (const kind in this.volumesEventListeners) {
        this.volumes.on(kind, this.volumesEventListeners[kind]);
      }
    }
    // Populate stream with objects which already exist but for consumer
    // they appear as new.
    const self = this;
    if (self.registry) {
      self.registry.getNodes().forEach((node: Node) => {
        self.events.push({
          kind: 'node',
          eventType: 'new',
          object: node
        });
        // First we emit replica and then pool events. Otherwise volume manager
        // could start creating new volume on imported pool although that the
        // volume is already there.
        node.pools.forEach((obj: Pool) => {
          obj.replicas.forEach((obj: Replica) => {
            self.events.push({
              kind: 'replica',
              eventType: 'new',
              object: obj
            });
          });
          self.events.push({
            kind: 'pool',
            eventType: 'new',
            object: obj
          });
        });
        node.nexus.forEach((obj: Nexus) => {
          self.events.push({
            kind: 'nexus',
            eventType: 'new',
            object: obj
          });
        });
        // generate artificial 'sync' event for the node so that the reader knows
        // that all "new" events for initial objects have been generated.
        self.events.push({
          kind: 'node',
          eventType: 'sync',
          object: node
        });
      });
    }
    if (self.volumes) {
      self.volumes.list().forEach((volume: Volume) => {
        self.events.push({
          kind: 'volume',
          eventType: 'new',
          object: volume
        });
      });
    }
    if (self.waiting) {
      self.waiting = false;
      self._read();
    }
  }

  _onEvent (kind: string, ev: ReceivedEventObject) {
    this.events.push({
      kind: kind,
      eventType: ev.eventType,
      object: ev.object
    });
    if (this.waiting) {
      this.waiting = false;
      this._read();
    }
  }

  _read (_size?: number) {
    if (!this.started) {
      this._start();
    }
    let cont = true;
    while (cont) {
      const ev = this.events.shift();
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

  _destroy (err: Error, cb: (err: Error) => void) {
    if (this.started) {
      if (this.registry) {
        for (const kind in this.registryEventListeners) {
          this.registry.removeListener(kind, this.registryEventListeners[kind]);
        }
      }
      if (this.volumes) {
        for (const kind in this.volumesEventListeners) {
          this.volumes.removeListener(kind, this.volumesEventListeners[kind]);
        }
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
