// Fake node object with altered connect/call/disconnect method to
// prevent any over the wire calls when testing.

'use strict';

const { Node } = require('../dist/node');

// It can be used instead of real node object in tests of components that
// depend on the Node.
class NodeStub extends Node {
  // Construct a node object.
  // Compared to the real constructor it accepts additional "pools" arg,
  // that is used to set pool list to initial value.
  constructor (name, opts, pools, nexus) {
    super(name, opts);

    if (pools) {
      this.pools = pools.map((p) => {
        p.node = this;
        return p;
      });
    }
    if (nexus) {
      this.nexus = nexus.map((n) => {
        n.node = this;
        return n;
      });
    }
    // keep existing behaviour and set the fake node to synced by default
    this.syncFailed = 0;
  }

  connect (endpoint) {
    this.syncFailed = 0;
    if (this.endpoint === endpoint) {
      // nothing changed
      return;
    } else if (this.endpoint) {
      this.emit('node', {
        eventType: 'mod',
        object: this
      });
    }
    this.endpoint = endpoint;
  }

  disconnect () {
    this.syncFailed = this.syncBadLimit + 1;
    this.endpoint = null;
    this.client = null;
    this._offline();
  }
}

module.exports = NodeStub;
