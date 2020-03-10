// Fake node object which simulates the real one.
//
// TODO: Use a function that returns the real node and replaces connect
// and disconnect functions by stubs.

'use strict';

const assert = require('assert');
const EventEmitter = require('events');

// It can be used instead of real node object in tests of components that
// depend on the Node.
class Node extends EventEmitter {
  // Construct a node object.
  // Compared to the real constructor it accepts additional "pools" arg,
  // that is used to set pool list to initial value.
  constructor(name, opts, pools, nexus) {
    super();
    this.name = name;
    this.endpoint = null;

    var self = this;
    pools = pools || [];
    nexus = nexus || [];
    self.pools = pools.map(p => {
      p.node = self;
      return p;
    });
    self.replicas = [];
    self.nexus = nexus.map(n => {
      n.node = self;
      return n;
    });
  }

  connect(endpoint) {
    this.endpoint = endpoint;
  }

  disconnect() {
    this.endpoint = null;
  }

  async call(method, args) {
    // this method should typically be replaced by a sinon stub for testing
  }

  async createPool(name, disks) {
    // this method should typically be replaced by a sinon stub for testing
  }
}

module.exports = Node;
