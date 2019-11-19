// moac REST API server
//
// Auxilliary interface for all stuff which using k8s resources would be
// awkward for. Currently we use it only for exposing stats to decouple
// the way of storing and presenting the stats from the mayastor
// implementation.

'use strict';

const express = require('express');
const log = require('./logger').Logger('api');

class ApiServer {
  constructor(volumeOperator) {
    var self = this;
    this.volumes = volumeOperator;
    this.app = express();
    this.app.get('/stats', (req, res) => {
      self.volumes.getStats().then(
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

  async stop() {
    if (this.server) this.server.close();
  }
}

module.exports = {
  ApiServer,
};
