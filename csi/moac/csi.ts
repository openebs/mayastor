// Implementation of K8S CSI controller interface which is mostly
// about volume creation and destruction and few other methods.

'use strict';

import assert from 'assert';
import * as _ from 'lodash';
import * as path from 'path';
import { Volume } from './volume';
import { Volumes } from './volumes';

const fs = require('fs').promises;
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

// Done callback in CSI methods
type CsiDoneCb = (err: any, resp?: any) => void;
// CSI method signature
type CsiMethod = (args: any, cb: CsiDoneCb) => void;

// Limited definition of topology key from CSI spec.
type TopologyKeys = {
  segments: Record<string, string>
};

// Simplified definition of K8s object as defined in the CSI spec.
type K8sVolume = {
  volumeId: string,
  capacityBytes: number,
  accessibleTopology: TopologyKeys[],
};

// When list volumes method does not fit into one reply we store the context
// for the next retrieval.
type ListContext = {
  volumes: {
    volume: K8sVolume
  }[]
};

// Parse mayastor node ID (i.e. mayastor://node-name) and return the node name.
function parseMayastorNodeId (nodeId: string) {
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
// @param caps    Volume capabilities as described in CSI spec.
function checkCapabilities (caps: any[]) {
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
function createK8sVolumeObject (volume: Volume): K8sVolume {
  const obj: K8sVolume = {
    volumeId: volume.uuid,
    capacityBytes: volume.getSize(),
    accessibleTopology: []
  };
  return obj;
}

// Duplicate request cache entry helps to detect retransmits of the same request
//
// This may seem like a useless thing but k8s is agressive on retransmitting
// requests. The first retransmit happens just a tens of ms after the original
// request. Having many requests that are the same in progress creates havoc
// and forces mayastor to execute repeating code.
//
// NOTE: Assumption is that k8s doesn't submit duplicate request for the same
// volume (the same uuid) with different parameters.
//
class Request {
  uuid: string; // ID of the object in the operation
  op: string; // name of the operation
  callbacks: CsiDoneCb[]; // callbacks to call when done

  constructor (uuid: string, op: string, cb: CsiDoneCb) {
    this.uuid = uuid;
    this.op = op;
    this.callbacks = [cb];
  }

  wait (cb: CsiDoneCb) {
    this.callbacks.push(cb);
  }

  done (err: any, resp?: any) {
    this.callbacks.forEach((cb) => cb(err, resp));
  }
}

// CSI Controller implementation.
//
// It implements Identity and Controller grpc services from csi proto file.
// It relies on volume manager, when serving incoming CSI requests, that holds
// information about volumes and provides methods to manipulate them.
class CsiServer {
  private server: any;
  private ready: boolean;
  private registry: any;
  private volumes: Volumes | null;
  private sockPath: string;
  private nextListContextId: number;
  private listContexts: Record<string, ListContext>;
  private duplicateRequestCache: Request[];

  // Creates new csi server
  //
  // @param sockPath   Unix domain socket for csi server to listen on.
  constructor (sockPath: string) {
    this.server = new grpc.Server();
    this.ready = false;
    this.registry = null;
    this.volumes = null;
    this.sockPath = sockPath;
    this.nextListContextId = 1;
    this.listContexts = {};
    this.duplicateRequestCache = [];

    // The data returned by identity service should be kept in sync with
    // responses for the same methods on storage node.
    this.server.addService(csi.Identity.service, {
      getPluginInfo: this.getPluginInfo.bind(this),
      getPluginCapabilities: this.getPluginCapabilities.bind(this),
      probe: this.probe.bind(this)
    });

    // Wrap all controller methods by a check for readiness of the csi server
    // and request/response logging to avoid repeating code.
    const controllerMethods: Record<string, CsiMethod> = {};
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
    // Note: what used to be elegant in JS is a type disaster in TS.
    // Dynamic wrapper for calling methods defined on an object.
    methodNames.forEach((name) => {
      controllerMethods[name] = (args, cb) => {
        log.trace(`CSI ${name} request: ${JSON.stringify(args)}`);

        if (!this.ready) {
          return cb(
            new GrpcError(
              grpc.status.UNAVAILABLE,
              'Not ready for serving requests'
            )
          );
        }
        let csiMethod = <CsiMethod> this[name as keyof CsiServer].bind(this);
        return csiMethod(args, (err: any, resp: any) => {
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
    return new Promise((resolve, reject) => {
      log.info('Shutting down grpc server');
      this.server.tryShutdown(resolve);
    });
  }

  // Switch csi server to ready state (returned by identity.probe() method).
  // This will enable serving grpc controller service requests.
  //
  // @param registry Object holding node, replica, pool and nexus objects.
  // @param volumes  Volume manager.
  makeReady (registry: any, volumes: Volumes) {
    this.ready = true;
    this.registry = registry;
    this.volumes = volumes;
  }

  // Stop serving controller requests, but the identity service still works.
  // This is usually preparation for a shutdown.
  undoReady () {
    this.ready = false;
  }

  // Find outstanding request by uuid and operation type.
  _findRequest (uuid: string, op: string): Request | undefined {
    return this.duplicateRequestCache.find((e) => e.uuid === uuid && e.op === op);
  }

  _beginRequest (uuid: string, op: string, cb: CsiDoneCb): Request | undefined {
    let request = this._findRequest(uuid, op);
    if (request) {
      log.debug(`Duplicate ${op} volume request detected`);
      request.wait(cb);
      return;
    }
    request = new Request(uuid, op, cb);
    this.duplicateRequestCache.push(request);
    return request;
  }

  // Remove request entry from the cache and call done callbacks.
  _endRequest (request: Request, err: any, resp?: any) {
    let idx = this.duplicateRequestCache.indexOf(request);
    if (idx >= 0) {
      this.duplicateRequestCache.splice(idx, 1);
    }
    request.done(err, resp);
  }

  //
  // Implementation of CSI identity methods
  //

  getPluginInfo (_: any, cb: CsiDoneCb) {
    log.debug(
      `getPluginInfo request (name=${PLUGIN_NAME}, version=${VERSION})`
    );
    cb(null, {
      name: PLUGIN_NAME,
      vendorVersion: VERSION,
      manifest: {}
    });
  }

  getPluginCapabilities (_: any, cb: CsiDoneCb) {
    const caps = ['CONTROLLER_SERVICE', 'VOLUME_ACCESSIBILITY_CONSTRAINTS'];
    log.debug('getPluginCapabilities request: ' + caps.join(', '));
    cb(null, {
      capabilities: caps.map((c) => {
        return { service: { type: c } };
      })
    });
  }

  probe (_: any, cb: CsiDoneCb) {
    log.debug(`probe request (ready=${this.ready})`);
    cb(null, { ready: { value: this.ready } });
  }

  //
  // Implementation of CSI controller methods
  //

  async controllerGetCapabilities (_: any, cb: CsiDoneCb) {
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

  async createVolume (call: any, cb: CsiDoneCb) {
    const args = call.request;
    assert(this.volumes);

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
    const ioTimeout = args.parameters.ioTimeout;
    if (ioTimeout !== undefined) {
      if (protocol !== 'nvmf') {
        return cb(new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          'ioTimeout is valid only for nvmf protocol'
        ));
      }
      if (Object.is(parseInt(ioTimeout), NaN)) {
        return cb(new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          'ioTimeout must be an integer'
        ));
      }
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

    // If this is a duplicate request then assure it is executed just once.
    let request = this._beginRequest(uuid, 'create', cb);
    if (!request) {
      return;
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
      this._endRequest(request, err);
      return;
    }

    // This was used in the old days for NBD protocol
    const topologies: TopologyKeys[] = [];

    this._endRequest(request, null, {
      volume: {
        capacityBytes: volume.getSize(),
        volumeId: uuid,
        accessibleTopology: topologies,
        // parameters defined in the storage class are only presented
        // to the CSI driver createVolume method.
        // Propagate them to other CSI driver methods involved in
        // standing up a volume, using the volume context.
        volumeContext: args.parameters
      }
    });
  }

  async deleteVolume (call: any, cb: CsiDoneCb) {
    const args = call.request;
    assert(this.volumes);

    log.debug(`Request to destroy volume "${args.volumeId}"`);

    // If this is a duplicate request then assure it is executed just once.
    let request = this._beginRequest(args.volumeId, 'delete', cb);
    if (!request) {
      return;
    }

    try {
      await this.volumes.destroyVolume(args.volumeId);
    } catch (err) {
      return this._endRequest(request, err);
    }
    log.info(`Volume "${args.volumeId}" destroyed`);
    this._endRequest(request, null);
  }

  async listVolumes (call: any, cb: CsiDoneCb) {
    assert(this.volumes);
    const args = call.request;
    let ctx: ListContext;

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
      const ctxId = (this.nextListContextId++).toString();
      this.listContexts[ctxId] = ctx;
      cb(null, {
        entries: entries,
        nextToken: ctxId,
      });
    } else {
      cb(null, { entries: entries });
    }
  }

  async controllerPublishVolume (call: any, cb: CsiDoneCb) {
    assert(this.volumes);
    const args = call.request;
    const publishContext: any = {};

    log.debug(
      `Request to publish volume "${args.volumeId}" for "${args.nodeId}"`
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
    const ioTimeout = args.volumeContext?.ioTimeout;
    if (ioTimeout !== undefined) {
      // The value has been checked during the createVolume
      publishContext.ioTimeout = ioTimeout;
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

    // If this is a duplicate request then assure it is executed just once.
    let request = this._beginRequest(args.volumeId, 'publish', cb);
    if (!request) {
      return;
    }

    try {
      publishContext.uri = await volume.publish(nodeId);
    } catch (err) {
      if (err.code === grpc.status.ALREADY_EXISTS) {
        log.debug(`Volume "${args.volumeId}" already published on this node`);
        this._endRequest(request, null, { publishContext });
      } else {
        cb(err);
        this._endRequest(request, err);
      }
      return;
    }

    log.info(`Published "${args.volumeId}" at ${publishContext.uri}`);
    this._endRequest(request, null, { publishContext });
  }

  async controllerUnpublishVolume (call: any, cb: CsiDoneCb) {
    assert(this.volumes);
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

    // If this is a duplicate request then assure it is executed just once.
    let request = this._beginRequest(args.volumeId, 'unpublish', cb);
    if (!request) {
      return;
    }

    try {
      await volume.unpublish();
    } catch (err) {
      return this._endRequest(request, err);
    }
    log.info(`Unpublished volume "${args.volumeId}"`);
    this._endRequest(request, null, {});
  }

  async validateVolumeCapabilities (call: any, cb: CsiDoneCb) {
    assert(this.volumes);
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
      (cap: any) => cap.accessMode.mode === 'SINGLE_NODE_WRITER'
    );
    const resp: any = {};
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
  async getCapacity (call: any, cb: CsiDoneCb) {
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
