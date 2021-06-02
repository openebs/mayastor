// moac REST API server
//
// Auxilliary interface for two purposes:
//
// 1. All stuff for which using k8s custom resources would be too awkward.
//    Currently we use it only for exposing stats.
// 2. Interface that other components can use to interact with the control
//    plane. Currently used only for liveness and readiness probes.

import express from 'express';
import { Server } from 'http';
import { Registry } from './registry';
import { Node, ReplicaStat } from './node';
import { Logger } from './logger';

const log = Logger('api');

export class ApiServer {
  private registry: Registry;
  private app: express.Express;
  private server: Server | undefined;

  constructor (registry: Registry) {
    const self = this;
    this.registry = registry;
    this.app = express();
    // for liveness & readiness probes
    this.app.get('/', (req, res) => {
      res.json({});
    });
    // for obtaining volume stats
    this.app.get('/stats', (req, res) => {
      self.getStats().then(
        (stats) => res.json(stats),
        (err) => res.status(500).send(err.toString())
      );
    });
  }

  async start (port: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = this.app.listen(port, () => {
        log.info('API server listening on port ' + port);
        resolve();
      });
    });
  }

  stop () {
    if (this.server) {
      this.server.close();
      this.server = undefined;
    }
  }

  // TODO: should return stats for nexus rather than for replica
  async getStats (): Promise<ReplicaStat[]> {
    const self = this;
    let stats: ReplicaStat[] = [];
    const nodes: Node[] = self.registry.getNodes();

    // TODO: stats can be retrieved in parallel
    for (let i = 0; i < nodes.length; i++) {
      const node = nodes[i];
      const timestamp = new Date().toISOString();

      let replicaStats: ReplicaStat[];
      try {
        replicaStats = await node.getStats();
      } catch (err) {
        log.error(`Failed to retrieve stats from "${node}": ${err}`);
        continue;
      }

      stats = stats.concat(replicaStats);
    }

    return stats;
  }
}
