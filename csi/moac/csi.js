'use strict';

const assert = require('assert');
const fs = require('fs').promises;
const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc-uds');
const log = require('./logger').Logger('csi');
const {
  PLUGIN_NAME,
  GrpcError,
  parseMayastorNodeId,
  isPoolAccessible,
} = require('./common');

const PROTO_PATH = __dirname + '/proto/csi.proto';
// TODO: can we generate version with commit SHA dynamically?
const VERSION = '0.1';
const PVC_RE = /pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/;

// Load csi proto file with controller and identity services
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  // this is to load google/descriptor.proto, otherwise you would see error:
  // unresolvable extensions: 'extend google.protobuf.FieldOptions' in .csi.v1
  includeDirs: [__dirname + '/node_modules/protobufjs/src'],
});
const csi = grpc.loadPackageDefinition(packageDefinition).csi.v1;

// Check that the list of volume capabilities does not contain unsupported
// capability.
function checkCapabilities(caps) {
  if (!caps) {
    throw new GrpcError(
      grpc.status.INVALID_ARGUMENT,
      'Missing volume capabilities'
    );
  }
  for (let i = 0; i < caps.length; i++) {
    let cap = caps[i];

    // TODO: Check that FS type is supported and mount options?
    if (cap.accessMode.mode != 'SINGLE_NODE_WRITER') {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `Access mode ${cap.accessMode.mode} not supported`
      );
    }
  }
}

// Create k8s volume object as returned by CSI list volumes method.
// Input is nexus object returned by volume operator.
function createK8sVolumeObject(nexus) {
  if (!nexus) return nexus;
  return {
    volumeId: nexus.uuid,
    capacityBytes: nexus.size,
    accessibleTopology: [
      {
        segments: { 'kubernetes.io/hostname': nexus.node },
      },
    ],
  };
}

// CSI Controller implementation.
//
// It implements Identity and Controller grpc services from csi proto file.
// It relies on pool operator, when serving incoming CSI requests, which holds
// the information about available storage pools.
class CsiServer {
  // Creates new csi server
  constructor(sockPath) {
    assert.equal(typeof sockPath, 'string');
    this.server = new grpc.Server();
    this.ready = false;
    this.pools = null;
    this.sockPath = sockPath;
    this.nextListContextId = 1;
    this.listContexts = {};

    // The data returned by identity service should be kept in sync with
    // responses for the same methods on storage node.
    this.server.addService(csi.Identity.service, {
      getPluginInfo: this.getPluginInfo.bind(this),
      getPluginCapabilities: this.getPluginCapabilities.bind(this),
      probe: this.probe.bind(this),
    });

    // Wrap all controller methods by a check for readiness of the csi server
    // and request/response logging to avoid repeating code.
    var self = this;
    var controllerMethods = {};
    var methodNames = [
      'createVolume',
      'deleteVolume',
      'controllerPublishVolume',
      'controllerUnpublishVolume',
      'validateVolumeCapabilities',
      'listVolumes',
      'getCapacity',
      'controllerGetCapabilities',
    ];
    methodNames.forEach(name => {
      controllerMethods[name] = function checkReady(args, cb) {
        log.trace('CSI ' + name + ' request: ' + JSON.stringify(args));

        if (!self.ready) {
          return cb(
            new GrpcError(
              grpc.status.UNAVAILABLE,
              'Not ready for serving requests'
            )
          );
        }
        return self[name](args, (err, resp) => {
          if (err) {
            log.error('CSI ' + name + ' failed: ' + err);
          } else {
            log.trace('CSI ' + name + ' response: ' + JSON.stringify(resp));
          }
          cb(err, resp);
        });
      };
    });
    // unimplemented methods
    methodNames = [
      'createSnapshot',
      'deleteSnapshot',
      'listSnapshots',
      'controllerExpandVolume',
    ];
    methodNames.forEach(name => {
      controllerMethods[name] = function notImplemented(_, cb) {
        let msg = `CSI method ${name} not implemented`;
        log.error(msg);
        cb(new GrpcError(grpc.status.UNIMPLEMENTED, msg));
      };
    });
    this.server.addService(csi.Controller.service, controllerMethods);
  }

  // Listen on UDS
  async start() {
    try {
      await fs.lstat(this.sockPath);
      log.info('Removing stale socket file ' + this.sockPath);
      await fs.unlink(this.sockPath);
    } catch (err) {
      // the file does not exist which is ok
    }
    let ok = this.server.bind(
      this.sockPath,
      grpc.ServerCredentials.createInsecure()
    );
    if (!ok) {
      log.error('CSI server failed to bind at ' + this.sockPath);
      throw new Error('Bind failed');
    }
    log.info('CSI server listens at ' + this.sockPath);
    this.server.start();
  }

  async stop() {
    var self = this;
    return new Promise((resolve, reject) => {
      log.info('Shutting down grpc server');
      self.server.tryShutdown(resolve);
    });
  }

  // Switch csi server to ready state (returned by identity.probe method).
  // This will enable serving controller grpc service requests.
  makeReady(poolOperator, volumeOperator) {
    this.ready = true;
    this.pools = poolOperator;
    this.volumes = volumeOperator;
  }

  // Stop serving controller requests, but the identity service still works.
  // This is usually preparation for a shutdown.
  undoReady() {
    this.ready = false;
  }

  // Return list of storage pools sorted by preference where a new volume
  // can be provisioned.
  //
  // The rules are simple:
  //   1) must be online (or degraded if there are no online pools)
  //   2) must have sufficient space
  //   3) least busy pools first
  choosePools(requiredBytes, mustNodes, shouldNodes) {
    let replicas = this.volumes.getReplica();
    let pools = this.pools.get().filter(p => {
      return (
        isPoolAccessible(p) &&
        p.capacity - p.used >= requiredBytes &&
        (mustNodes.length == 0 || mustNodes.indexOf(p.node) >= 0)
      );
    });
    // construct a map of how many volumes has each pool (how busy it is)
    let busy = {};
    pools.forEach(p => (busy[p.name] = 0));
    replicas.forEach(r => {
      if (busy[r.pool] != null) {
        busy[r.pool]++;
      }
    });

    pools.sort((a, b) => {
      // Rule #1: User preference
      if (shouldNodes.length > 0) {
        if (
          shouldNodes.indexOf(a.node) >= 0 &&
          shouldNodes.indexOf(b.node) < 0
        ) {
          return -1;
        } else if (
          shouldNodes.indexOf(a.node) < 0 &&
          shouldNodes.indexOf(b.node) >= 0
        ) {
          return 1;
        }
      }

      // Rule #2: Avoid degraded pools whenever possible
      if (a.state == 'ONLINE' && b.state == 'DEGRADED') {
        return -1;
      } else if (a.state == 'DEGRADED' && b.state == 'ONLINE') {
        return 1;
      }

      // Rule #3: Use the least busy pool in terms of number of volumes
      if (busy[a.name] < busy[b.name]) {
        return -1;
      } else if (busy[a.name] > busy[b.name]) {
        return 1;
      }

      // Rule #4: Pools with more free space take precedence
      let aFree = a.capacity - a.used;
      let bFree = b.capacity - b.used;
      return bFree - aFree;
    });

    return pools;
  }

  //
  // Implementation of CSI identity methods
  //

  getPluginInfo(_, cb) {
    log.debug(
      `getPluginInfo request (name=${PLUGIN_NAME}, version=${VERSION})`
    );
    cb(null, {
      name: PLUGIN_NAME,
      vendorVersion: VERSION,
      manifest: {},
    });
  }

  getPluginCapabilities(_, cb) {
    var caps = ['CONTROLLER_SERVICE', 'VOLUME_ACCESSIBILITY_CONSTRAINTS'];
    log.debug('getPluginCapabilities request: ' + caps.join(', '));
    cb(null, {
      capabilities: caps.map(c => {
        return { service: { type: c } };
      }),
    });
  }

  probe(_, cb) {
    log.debug(`probe request (ready=${this.ready})`);
    cb(null, { ready: { value: this.ready } });
  }

  //
  // Implementation of CSI controller methods
  //

  async controllerGetCapabilities(_, cb) {
    var caps = [
      'CREATE_DELETE_VOLUME',
      'PUBLISH_UNPUBLISH_VOLUME',
      'LIST_VOLUMES',
      'GET_CAPACITY',
    ];
    log.debug('get capabilities request: ' + caps.join(', '));
    cb(null, {
      capabilities: caps.map(c => {
        return { rpc: { type: c } };
      }),
    });
  }

  async createVolume(call, cb) {
    var args = call.request;

    log.debug(
      `Request to create volume "${args.name}" with size ` +
        args.capacityRange.requiredBytes +
        ` (limit ${args.capacityRange.limitBytes})`
    );

    if (args.volumeContentSource) {
      return cb(
        new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          'Source for create volume is not supported'
        )
      );
    }
    // k8s uses names pvc-{uuid} and we use uuid further as ID in SPDK so we
    // must require it.
    let m = args.name.match(PVC_RE);
    if (!m) {
      return cb(
        new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          'Expected the volume name in pvc-{uuid} format: ' + args.name
        )
      );
    }
    let uuid = m[1];
    try {
      checkCapabilities(args.volumeCapabilities);
    } catch (err) {
      return cb(err);
    }
    let mustNodes = [];
    let shouldNodes = [];

    if (args.accessibilityRequirements) {
      for (
        let i = 0;
        i < args.accessibilityRequirements.requisite.length;
        i++
      ) {
        let reqs = args.accessibilityRequirements.requisite[i];
        for (let key in reqs.segments) {
          // We are not able to evaluate any other topology requirements than
          // the hostname req. Reject all others.
          if (key != 'kubernetes.io/hostname') {
            return cb(
              new GrpcError(
                grpc.status.INVALID_ARGUMENT,
                'Volume topology other than hostname not supported'
              )
            );
          } else {
            mustNodes.push(reqs.segments[key]);
          }
        }
      }
      for (
        let i = 0;
        i < args.accessibilityRequirements.preferred.length;
        i++
      ) {
        let reqs = args.accessibilityRequirements.preferred[i];
        for (let key in reqs.segments) {
          // ignore others than hostname (it's only preferred)
          if (key == 'kubernetes.io/hostname') {
            shouldNodes.push(reqs.segments[key]);
          }
        }
      }
    }
    // check if the nexus already exists
    let nexus = this.volumes.getNexus(uuid);
    if (nexus) {
      // see if the volume is compatible in which case it is ok (be idempotent)
      if (
        nexus.size < args.capacityRange.requiredBytes ||
        (args.capacityRange.limitBytes != 0 &&
          nexus.size > args.capacityRange.limitBytes)
      ) {
        return cb(
          new GrpcError(
            grpc.status.ALREADY_EXISTS,
            `A different volume with name "${args.name}" already exists`
          )
        );
      }
      // check if the replica belonging to nexus already exists
      let replica = this.volumes.getReplica(uuid);
      if (!replica) {
        // TODO: recover from this by syncing the nexus configuration
        return cb(
          new GrpcError(
            grpc.status.ALREADY_EXISTS,
            `The volume "${args.name}" exists but it is missing a replica`
          )
        );
      } else {
        return cb(null, nexus);
      }
    }

    // limitBytes is 0 if not set, so fix it to be at least what is required
    if (args.capacityRange.requiredBytes > args.capacityRange.limitBytes) {
      args.capacityRange.limitBytes = args.capacityRange.requiredBytes;
    }

    // sync used and capacity pool properties before making the decision
    // of where to provision the volume
    await this.pools.syncNode();
    let pools = this.choosePools(
      args.capacityRange.requiredBytes,
      mustNodes,
      shouldNodes
    );
    if (pools.length == 0) {
      log.error(
        'No suitable pool for the volume "' +
          args.name +
          '" with capacity range ' +
          args.capacityRange.requiredBytes +
          ' - ' +
          args.capacityRange.limitBytes
      );

      return cb(
        new GrpcError(
          grpc.status.RESOURCE_EXHAUSTED,
          'Cannot find suitable storage pool for the volume'
        )
      );
    }

    // we record all failures as we try to create the volume on pools
    // to return them to user at the end
    var errors = [];
    // try one pool after another until success
    for (let i = 0; i < pools.length; i++) {
      let pool = pools[i];

      // calculate a size of the volume
      let free = pool.capacity - pool.used;
      let size;
      if (free > args.capacityRange.limitBytes) {
        size = args.capacityRange.limitBytes;
      } else {
        size = Math.max(free, args.capacityRange.requiredBytes);
      }
      if (size <= 0) {
        // No point in trying other pools if even with the better pool the size is 0
        return cb(
          new GrpcError(
            grpc.status.INVALID_ARGUMENT,
            'Cannot create zero sized volume'
          )
        );
      }

      try {
        await this.volumes.createReplica(pool.node, pool.name, uuid, size);
      } catch (err) {
        log.error(err.message);
        errors.push(err.message);
        continue;
      }

      try {
        await this.volumes.createNexus(pool.node, uuid, size, [
          'bdev:///' + uuid,
        ]);
      } catch (err) {
        log.error(err.message);
        errors.push(err.message);
        // undo the replica creation
        try {
          await this.volumes.destroyReplica(pool.node, uuid);
        } catch (err) {
          let msg = `Failed to destroy partially instantiated volume "${args.name}"`;
          log.error(msg);
          errors.push(msg);
          break; // unrecoverable error
        }
        continue;
      }

      log.info(
        `Volume "${args.name}" with size ${size} created on pool "${pool.name}"`
      );

      return cb(null, {
        volume: {
          capacityBytes: size,
          volumeId: uuid,
          // enfore local access to the volume
          accessibleTopology: [
            {
              segments: { 'kubernetes.io/hostname': pool.node },
            },
          ],
        },
      });
    }

    cb(new GrpcError(grpc.status.INTERNAL, errors.join('\n')));
  }

  async deleteVolume(call, cb) {
    var args = call.request;

    log.debug(`Request to destroy volume "${args.volumeId}"`);

    let nexus = this.volumes.getNexus(args.volumeId);
    let replica = this.volumes.getReplica(args.volumeId);
    if (!nexus && !replica) {
      // most likely already deleted
      return cb();
    }

    if (nexus) {
      try {
        await this.volumes.destroyNexus(nexus.node, args.volumeId);
      } catch (err) {
        return cb(err);
      }
    }
    if (replica) {
      let pool = this.pools.get(replica.pool);
      assert(pool, 'Volume exists but pool does not');
      if (!isPoolAccessible(pool)) {
        return cb(
          new GrpcError(
            grpc.status.INTERNAL,
            `Storage pool "${pool.name}" not accessible`
          )
        );
      }
      try {
        await this.volumes.destroyReplica(pool.node, args.volumeId);
      } catch (err) {
        return cb(err);
      }
    }
    log.info(`Volume "${args.volumeId}" destroyed`);
    cb();
  }

  async listVolumes(call, cb) {
    var args = call.request;
    var ctx = {};

    if (args.startingToken) {
      ctx = this.listContexts[args.startingToken];
      delete this.listContexts[args.startingToken];
      if (!ctx) {
        return cb(
          new GrpcError(
            grpc.status.INVALID_ARGUMENT,
            'Paging context for list volumes is gone'
          )
        );
      }
    } else {
      log.debug('Request to list volumes');
      ctx = {
        volumes: this.volumes
          .getNexus()
          .map(createK8sVolumeObject)
          .map(v => {
            return { volume: v };
          }),
      };
    }
    // default max entries
    if (!args.maxEntries) {
      args.maxEntries = 1000;
    }

    var entries = ctx.volumes.splice(0, args.maxEntries);

    // TODO: purge list contexts older than .. (1 min)
    if (ctx.volumes.length > 0) {
      let ctxId = this.nextListContextId++;
      this.listContexts[ctxId] = ctx;
      cb(null, {
        entries: entries,
        nextToken: ctxId.toString(),
      });
    } else {
      cb(null, { entries: entries });
    }
  }

  async controllerPublishVolume(call, cb) {
    var args = call.request;

    log.debug(
      `Request to publish volume "${args.volumeId}" on "${args.nodeId}"`
    );

    let nexus = this.volumes.getNexus(args.volumeId);
    if (!nexus) {
      return cb(
        new GrpcError(
          grpc.status.NOT_FOUND,
          `Volume "${args.volumeId}" does not exist`
        )
      );
    }
    var nodeId;
    try {
      nodeId = parseMayastorNodeId(args.nodeId);
    } catch (err) {
      return cb(err);
    }
    if (nodeId.node != nexus.node) {
      return cb(
        new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          `Cannot publish the volume "${args.volumeId}" on a different ` +
            `node "${nodeId.node}" than it was created "${nexus.node}"`
        )
      );
    }
    if (args.readonly) {
      return cb(
        new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          'readonly volumes are unsupported'
        )
      );
    }
    if (!args.volumeCapability) {
      return cb(
        new GrpcError(grpc.status.INVALID_ARGUMENT, 'missing volume capability')
      );
    }
    try {
      checkCapabilities([args.volumeCapability]);
    } catch (err) {
      return cb(err);
    }

    try {
      await this.volumes.publishNexus(nexus.uuid);
    } catch (err) {
      if (err.code === grpc.status.ALREADY_EXISTS) {
        log.debug(`Volume "${args.volumeId}" already published on this node`);
        cb(null, {});
      } else {
        cb(err);
      }
      return;
    }

    log.info(`Published volume "${args.volumeId}"`);
    cb(null, {});
  }

  async controllerUnpublishVolume(call, cb) {
    var args = call.request;

    log.debug(`Request to unpublish volume "${args.volumeId}"`);

    let nexus = this.volumes.getNexus(args.volumeId);
    if (!nexus) {
      return cb(
        new GrpcError(
          grpc.status.NOT_FOUND,
          `Volume "${args.volumeId}" does not exist`
        )
      );
    }
    var nodeId;
    try {
      nodeId = parseMayastorNodeId(args.nodeId);
    } catch (err) {
      return cb(err);
    }
    if (nodeId.node != nexus.node) {
      // we unpublish the volume anyway but at least we log a message
      log.warn(
        `Request to unpublish volume "${args.volumeId}" from a node ` +
          `"${nodeId.node}" when it was published on the node "${nexus.node}"`
      );
    }

    try {
      await this.volumes.unpublishNexus(nexus.uuid);
    } catch (err) {
      return cb(err);
    }
    log.info(`Unpublished volume "${args.volumeId}"`);
    cb(null, {});
  }

  async validateVolumeCapabilities(call, cb) {
    var args = call.request;

    log.debug(`Request to validate volume capabilities for "${args.volumeId}"`);

    if (!this.volumes.getNexus(args.volumeId)) {
      return cb(
        new GrpcError(
          grpc.status.NOT_FOUND,
          `Volume "${args.volumeId}" does not exist`
        )
      );
    }
    let caps = args.volumeCapabilities.filter(
      cap => cap.accessMode.mode == 'SINGLE_NODE_WRITER'
    );
    let resp = {};
    if (caps.length > 0) {
      resp.confirmed = { volumeCapabilities: caps };
    } else {
      resp.message = 'The only supported capability is SINGLE_NODE_WRITER';
    }
    cb(null, resp);
  }

  // We understand just one topology segment type and that is hostname.
  // So if it is specified we return capacity of storage pools on the node
  // or capacity of all pools in the cluster.
  // The value we return is actual (not cached).
  //
  // XXX Is the caller interested in total capacity (sum of all pools) or
  // a capacity usable by a single volume?
  async getCapacity(call, cb) {
    var args = call.request;

    if (args.volumeCapabilities) {
      try {
        checkCapabilities(args.volumeCapabilities);
      } catch (err) {
        return cb(err);
      }
    }
    if (args.accessibleTopology) {
      for (let key in args.accessibleTopology.segments) {
        if (key == 'kubernetes.io/hostname') {
          let nodeName = args.accessibleTopology.segments[key];
          let capacity = 0;
          await this.pools.syncNode(nodeName);
          // jshint ignore:start
          capacity = this.pools
            .get()
            .filter(p => p.node == nodeName)
            .reduce((acc, p) => {
              return isPoolAccessible(p) ? acc + (p.capacity - p.used) : 0;
            }, 0);
          // jshint ignore:end
          log.debug(`Get capacity of node "${nodeName}": ${capacity} bytes`);
          return cb(null, { availableCapacity: capacity });
        }
      }
    }

    // refresh pool info from all nodes
    await this.pools.syncNode();
    let capacity = this.pools
      .get()
      .filter(p => isPoolAccessible(p))
      .reduce((acc, p) => {
        return acc + (p.capacity - p.used);
      }, 0);

    log.debug(`Get total capacity: ${capacity} bytes`);
    cb(null, { availableCapacity: capacity });
  }
}

module.exports = {
  CsiServer,
  // the rest is exported for tests
  csi,
  GrpcError,
};
