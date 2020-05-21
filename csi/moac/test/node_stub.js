// Fake node object with altered connect/call/disconnect method to
// prevent any over the wire calls when testing.

'use strict';

const Node = require('../node');

// It can be used instead of real node object in tests of components that
// depend on the Node.
class NodeStub extends Node {
  // Construct a node object.
  // Compared to the real constructor it accepts additional "pools" arg,
  // that is used to set pool list to initial value.
  constructor (name, opts, pools, nexus) {
    super(name, opts);

    var self = this;
    if (pools) {
      self.pools = pools.map((p) => {
        p.node = self;
        return p;
      });
    }
    if (nexus) {
      self.nexus = nexus.map((n) => {
        n.node = self;
        return n;
      });
    }
  }

  connect (endpoint) {
    this.endpoint = endpoint;
  }

  disconnect () {
    this.endpoint = null;
  }

  // the fake connect does not kick off sync so we pretend we are always in sync
  isSynced () {
    return true;
  }
}

module.exports = NodeStub;
