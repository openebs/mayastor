// Implementation of K8S CSI controller interface which is mostly
// about volume creation and destruction and few other methods.

'use strict';

const assert = require('assert');
const fs = require('fs').promises;
const path = require('path');
const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc-uds');
const log = require('./logger').Logger('csi');
const { GrpcError } = require('./grpc_client');

const PLUGIN_NAME = 'io.openebs.csi-mayastor';
const PROTO_PATH = path.join(__dirname, '/proto/csi.proto');
// TODO: can we generate version with commit SHA dynamically?
const VERSION = '0.1';
const PVC_RE = /pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/;

// Load csi proto file with controller and identity services
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
  // this is to load google/descriptor.proto, otherwise you would see error:
  // unresolvable extensions: 'extend google.protobuf.FieldOptions' in .csi.v1
  includeDirs: [path.join(__dirname, '/node_modules/protobufjs')]
});
const csi = grpc.loadPackageDefinition(packageDefinition).csi.v1;

// Parse mayastor node ID (i.e. mayastor://node-name) and return the node name.
function parseMayastorNodeId (nodeId) {
  const parts = nodeId.split('/');

  if (
    parts.length !== 3 ||
    parts[0] !== 'mayastor:' ||
    parts[1] !== '' ||
    !parts[2]
  ) {
    throw new GrpcError(
      grpc.status.INVALID_ARGUMENT,
      'Invalid mayastor node ID: ' + nodeId
    );
  }
  return parts[2];
}

// Check that the list of volume capabilities does not contain unsupported
// capability. Throws grpc error if a capability is not supported.
//
// @param {string[]} caps    Volume capabilities as described in CSI spec.
function checkCapabilities (caps) {
  if (!caps) {
    throw new GrpcError(
      grpc.status.INVALID_ARGUMENT,
      'Missing volume capabilities'
    );
  }
  for (let i = 0; i < caps.length; i++) {
    const cap = caps[i];

    // TODO: Check that FS type is supported and mount options?
    if (cap.accessMode.mode !== 'SINGLE_NODE_WRITER') {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `Access mode ${cap.accessMode.mode} not supported`
      );
    }
  }
}

// Create k8s volume object as returned by CSI list volumes method.
//
// @param   {object} volume   Volume object.
// @returns {object} K8s CSI volume object.
function createK8sVolumeObject (volume) {
  const obj = {
    volumeId: volume.uuid,
    capacityBytes: volume.getSize(),
    accessibleTopology: []
  };
  if (volume.protocol.toLowerCase() === 'nbd') {
    obj.accessibleTopology.push({
      segments: { 'kubernetes.io/hostname': volume.getNodeName() }
    });
  }
  return obj;
}

// CSI Controller implementation.
//
// It implements Identity and Controller grpc services from csi proto file.
// It relies on volume manager, when serving incoming CSI requests, that holds
// information about volumes and provides methods to manipulate them.
class CsiServer {
  // Creates new csi server
  //
  // @param {string} sockPath   Unix domain socket for csi server to listen on.
  constructor (sockPath) {
    assert.strictEqual(typeof sockPath, 'string');
    this.server = new grpc.Server();
    this.ready = false;
    this.registry = null;
    this.volumes = null;
    this.sockPath = sockPath;
    this.nextListContextId = 1;
    this.listContexts = {};

    // The data returned by identity service should be kept in sync with
    // responses for the same methods on storage node.
    this.server.addService(csi.Identity.service, {
      getPluginInfo: this.getPluginInfo.bind(this),
      getPluginCapabilities: this.getPluginCapabilities.bind(this),
      probe: this.probe.bind(this)
    });

    // Wrap all controller methods by a check for readiness of the csi server
    // and request/response logging to avoid repeating code.
    const self = this;
    const controllerMethods = {};
    let methodNames = [
      'createVolume',
      'deleteVolume',
      'controllerPublishVolume',
      'controllerUnpublishVolume',
      'validateVolumeCapabilities',
      'listVolumes',
      'getCapacity',
      'controllerGetCapabilities'
    ];
    methodNames.forEach((name) => {
      controllerMethods[name] = function checkReady (args, cb) {
        log.trace(`CSI ${name} request: ${JSON.stringify(args)}`);

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
            if (!(err instanceof GrpcError)) {
              err = new GrpcError(
                grpc.status.UNKNOWN,
                `Unexpected error in ${name} method: ` + err.stack
              );
            }
            log.error(`CSI ${name} failed: ${err}`);
          } else {
            log.trace(`CSI ${name} response: ${JSON.stringify(resp)}`);
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
      'controllerExpandVolume'
    ];
    methodNames.forEach((name) => {
      controllerMethods[name] = function notImplemented (_, cb) {
        const msg = `CSI method ${name} not implemented`;
        log.error(msg);
        cb(new GrpcError(grpc.status.UNIMPLEMENTED, msg));
      };
    });
    this.server.addService(csi.Controller.service, controllerMethods);
  }

  // Listen on UDS
  async start () {
    try {
      await fs.lstat(this.sockPath);
      log.info('Removing stale socket file ' + this.sockPath);
      await fs.unlink(this.sockPath);
    } catch (err) {
      // the file does not exist which is ok
    }
    const ok = this.server.bind(
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

  // Stop the grpc server.
  async stop () {
    const self = this;
    return new Promise((resolve, reject) => {
      log.info('Shutting down grpc server');
      self.server.tryShutdown(resolve);
    });
  }

  // Switch csi server to ready state (returned by identity.probe() method).
  // This will enable serving grpc controller service requests.
  //
  // @param {object} registry Object holding node, replica, pool and nexus objects.
  // @param {object} volumes  Volume manager.
  makeReady (registry, volumes) {
    this.ready = true;
    this.registry = registry;
    this.volumes = volumes;
  }

  // Stop serving controller requests, but the identity service still works.
  // This is usually preparation for a shutdown.
  undoReady () {
    this.ready = false;
  }

  //
  // Implementation of CSI identity methods
  //

  getPluginInfo (_, cb) {
    log.debug(
      `getPluginInfo request (name=${PLUGIN_NAME}, version=${VERSION})`
    );
    cb(null, {
      name: PLUGIN_NAME,
      vendorVersion: VERSION,
      manifest: {}
    });
  }

  getPluginCapabilities (_, cb) {
    const caps = ['CONTROLLER_SERVICE', 'VOLUME_ACCESSIBILITY_CONSTRAINTS'];
    log.debug('getPluginCapabilities request: ' + caps.join(', '));
    cb(null, {
      capabilities: caps.map((c) => {
        return { service: { type: c } };
      })
    });
  }

  probe (_, cb) {
    log.debug(`probe request (ready=${this.ready})`);
    cb(null, { ready: { value: this.ready } });
  }

  //
  // Implementation of CSI controller methods
  //

  async controllerGetCapabilities (_, cb) {
    const caps = [
      'CREATE_DELETE_VOLUME',
      'PUBLISH_UNPUBLISH_VOLUME',
      'LIST_VOLUMES',
      'GET_CAPACITY'
    ];
    log.debug('get capabilities request: ' + caps.join(', '));
    cb(null, {
      capabilities: caps.map((c) => {
        return { rpc: { type: c } };
      })
    });
  }

  async createVolume (call, cb) {
    const args = call.request;

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
    const m = args.name.match(PVC_RE);
    if (!m) {
      return cb(
        new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          `Expected the volume name in pvc-{uuid} format: ${args.name}`
        )
      );
    }
    const uuid = m[1];
    try {
      checkCapabilities(args.volumeCapabilities);
    } catch (err) {
      return cb(err);
    }

    // Storage protocol for accessing nexus is a required parameter
    const protocol = args.parameters && args.parameters.protocol;
    if (!protocol) {
      return cb(
        new GrpcError(grpc.status.INVALID_ARGUMENT, 'missing storage protocol')
      );
    }

    const mustNodes = [];
    const shouldNodes = [];

    if (args.accessibilityRequirements) {
      for (
        let i = 0;
        i < args.accessibilityRequirements.requisite.length;
        i++
      ) {
        const reqs = args.accessibilityRequirements.requisite[i];
        for (const key in reqs.segments) {
          // We are not able to evaluate any other topology requirements than
          // the hostname req. Reject all others.
          if (key !== 'kubernetes.io/hostname') {
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
        const reqs = args.accessibilityRequirements.preferred[i];
        for (const key in reqs.segments) {
          // ignore others than hostname (it's only preferred)
          if (key === 'kubernetes.io/hostname') {
            shouldNodes.push(reqs.segments[key]);
          }
        }
      }
    }

    let count = args.parameters.repl;
    if (count) {
      count = parseInt(count);
      if (isNaN(count) || count <= 0) {
        return cb(
          new GrpcError(grpc.status.INVALID_ARGUMENT, 'Invalid replica count')
        );
      }
    } else {
      count = 1;
    }

    // create the volume
    let volume;
    try {
      volume = await this.volumes.createVolume(uuid, {
        replicaCount: count,
        preferredNodes: shouldNodes,
        requiredNodes: mustNodes,
        requiredBytes: args.capacityRange.requiredBytes,
        limitBytes: args.capacityRange.limitBytes,
        protocol: protocol
      });
    } catch (err) {
      return cb(err);
    }

    // Enforce local access to the volume for NBD protocol
    const accessibleTopology = [];
    if (protocol.toLowerCase() === 'nbd') {
      accessibleTopology.push({
        segments: { 'kubernetes.io/hostname': volume.getNodeName() }
      });
    }
    cb(null, {
      volume: {
        capacityBytes: volume.getSize(),
        volumeId: uuid,
        accessibleTopology,
        // parameters defined in the storage class are only presented
        // to the CSI driver createVolume method.
        // Propagate them to other CSI driver methods involved in
        // standing up a volume, using the volume context.
        volumeContext: args.parameters
      }
    });
  }

  async deleteVolume (call, cb) {
    const args = call.request;

    log.debug(`Request to destroy volume "${args.volumeId}"`);

    try {
      await this.volumes.destroyVolume(args.volumeId);
    } catch (err) {
      return cb(err);
    }
    log.info(`Volume "${args.volumeId}" destroyed`);
    cb();
  }

  async listVolumes (call, cb) {
    const args = call.request;
    let ctx = {};

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
          .list()
          .map(createK8sVolumeObject)
          .map((v) => {
            return { volume: v };
          })
      };
    }
    // default max entries
    if (!args.maxEntries) {
      args.maxEntries = 1000;
    }

    const entries = ctx.volumes.splice(0, args.maxEntries);

    // TODO: purge list contexts older than .. (1 min)
    if (ctx.volumes.length > 0) {
      const ctxId = this.nextListContextId++;
      this.listContexts[ctxId] = ctx;
      cb(null, {
        entries: entries,
        nextToken: ctxId.toString()
      });
    } else {
      cb(null, { entries: entries });
    }
  }

  async controllerPublishVolume (call, cb) {
    const args = call.request;

    log.debug(
      `Request to publish volume "${args.volumeId}" on "${args.nodeId}"`
    );

    const volume = this.volumes.get(args.volumeId);
    if (!volume) {
      return cb(
        new GrpcError(
          grpc.status.NOT_FOUND,
          `Volume "${args.volumeId}" does not exist`
        )
      );
    }
    let nodeId;
    try {
      nodeId = parseMayastorNodeId(args.nodeId);
    } catch (err) {
      return cb(err);
    }
    // Storage protocol for accessing nexus is a required parameter
    const protocol = args.volumeContext && args.volumeContext.protocol;
    if (!protocol) {
      return cb(
        new GrpcError(grpc.status.INVALID_ARGUMENT, 'missing storage protocol')
      );
    }
    if (protocol.toLowerCase() === 'nbd') {
      const nodeName = volume.getNodeName();
      if (nodeId !== nodeName) {
        return cb(
          new GrpcError(
            grpc.status.INVALID_ARGUMENT,
            `Cannot publish the volume "${args.volumeId}" on a different ` +
              `node "${nodeId}" than it was created "${nodeName}" when using ` +
              `local access protocol ${protocol}`
          )
        );
      }
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

    const publishContext = {};
    try {
      publishContext.uri = await volume.publish(protocol);
      log.debug(
        `"${args.volumeId}" published, got uri ${publishContext.uri} `
      );
    } catch (err) {
      if (err.code === grpc.status.ALREADY_EXISTS) {
        log.debug(`Volume "${args.volumeId}" already published on this node`);
        cb(null, { publishContext });
      } else {
        cb(err);
      }
      return;
    }

    log.info(`Published volume "${args.volumeId}" over ${protocol}`);
    cb(null, { publishContext });
  }

  async controllerUnpublishVolume (call, cb) {
    const args = call.request;

    log.debug(`Request to unpublish volume "${args.volumeId}"`);

    const volume = this.volumes.get(args.volumeId);
    if (!volume) {
      log.warn(
        `Request to unpublish volume "${args.volumeId}" which does not exist`
      );
      return cb(null, {});
    }
    try {
      parseMayastorNodeId(args.nodeId);
    } catch (err) {
      return cb(err);
    }
    try {
      await volume.unpublish();
    } catch (err) {
      return cb(err);
    }
    log.info(`Unpublished volume "${args.volumeId}"`);
    cb(null, {});
  }

  async validateVolumeCapabilities (call, cb) {
    const args = call.request;

    log.debug(`Request to validate volume capabilities for "${args.volumeId}"`);

    if (!this.volumes.get(args.volumeId)) {
      return cb(
        new GrpcError(
          grpc.status.NOT_FOUND,
          `Volume "${args.volumeId}" does not exist`
        )
      );
    }
    const caps = args.volumeCapabilities.filter(
      (cap) => cap.accessMode.mode === 'SINGLE_NODE_WRITER'
    );
    const resp = {};
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
  //
  // XXX Is the caller interested in total capacity (sum of all pools) or
  // a capacity usable by a single volume?
  async getCapacity (call, cb) {
    let nodeName;
    const args = call.request;

    if (args.volumeCapabilities) {
      try {
        checkCapabilities(args.volumeCapabilities);
      } catch (err) {
        return cb(err);
      }
    }
    if (args.accessibleTopology) {
      for (const key in args.accessibleTopology.segments) {
        if (key === 'kubernetes.io/hostname') {
          nodeName = args.accessibleTopology.segments[key];
          break;
        }
      }
    }

    const capacity = this.registry.getCapacity(nodeName);
    log.debug(`Get total capacity of node "${nodeName}": ${capacity} bytes`);
    cb(null, { availableCapacity: capacity });
  }
}

module.exports = {
  CsiServer,
  // this is exported for the tests
  csi
};
