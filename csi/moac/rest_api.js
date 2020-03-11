// moac REST API server
//
// Auxilliary interface for all stuff for which using k8s resources would be
// awkward. Currently we use it only for exposing stats to decouple
// the way of storing and presenting the stats from the mayastor
// implementation.
//
// TODO: However in future it will be used for obtaining detailed (more
// detailed than k8s api server or kubectl tools allow) information about
// internal state of moac and its objects.

'use strict';

const express = require('express');
const log = require('./logger').Logger('api');

class ApiServer {
  constructor(registry) {
    var self = this;
    this.registry = registry;
    this.app = express();
    this.app.get('/stats', (req, res) => {
      self.getStats().then(
        stats => res.json(stats),
        err => res.status(500).send(err.toString())
      );
    });
  }

  async start(port) {
    return new Promise((resolve, reject) => {
      this.server = this.app.listen(port, () => {
        log.info('API server listening on port ' + port);
        resolve();
      });
    });
  }

  stop() {
    if (this.server) this.server.close();
  }

  // TODO: should return stats for nexus rather than for replica
  async getStats() {
    var self = this;
    var vols = [];
    var nodes = self.registry.getNode();

    // TODO: stats can be retrieved in parallel
    for (let i = 0; i < nodes.length; i++) {
      let node = nodes[i];
      let timestamp = new Date().toISOString();

      // Lint does not like using for-loop variable in a function defined
      // in the loop. But we know it's ok in this case.
      // jshint ignore:start
      let replicaStats;
      try {
        replicaStats = await node.getStats();
      } catch (err) {
        log.error(`Failed to retrieve stats from "${node}": ${err}`);
        continue;
      }

      vols = vols.concat(
        replicaStats.map(r => {
          return {
            timestamp,
            // tags
            uuid: r.uuid,
            node: node.name,
            pool: r.pool,
            // counters
            num_read_ops: r.stats.numReadOps,
            num_write_ops: r.stats.numWriteOps,
            bytes_read: r.stats.bytesRead,
            bytes_written: r.stats.bytesWritten,
          };
        })
      );
      // jshint ignore:end
    }

    return vols;
  }
}

module.exports = ApiServer;
