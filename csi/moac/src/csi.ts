// Implementation of K8S CSI controller interface which is mostly
// about volume creation and destruction and few other methods.

import assert from 'assert';
import * as _ from 'lodash';
import * as path from 'path';
import { grpcCode, GrpcError } from './grpc_client';
import { Volume } from './volume';
import { Volumes } from './volumes';
import { Logger } from './logger';
import * as grpc from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { Workq } from './workq';

const log = Logger('csi');

const fs = require('fs').promises;

const PLUGIN_NAME = 'io.openebs.csi-mayastor';
const PROTO_PATH = path.join(__dirname, '../proto/csi.proto');
// TODO: can we generate version with commit SHA dynamically?
const VERSION = '0.1';
const PVC_RE = /pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/;
const YAML_TRUE_VALUE = [
  'y', 'Y', 'yes', 'Yes', 'YES',
  'true', 'True', 'TRUE',
  'on', 'On', 'ON',
];

// Load csi proto file with controller and identity services
const packageDefinition = loadSync(PROTO_PATH, {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
  // this is to load google/descriptor.proto, otherwise you would see error:
  // unresolvable extensions: 'extend google.protobuf.FieldOptions' in .csi.v1
  includeDirs: [path.join(__dirname, '/node_modules/protobufjs')]
});
// TODO: figure out how to remove any
const csi = (<any> grpc.loadPackageDefinition(packageDefinition).csi).v1;

// Done callback in CSI methods
type CsiDoneCb = (err: any, resp?: any) => void;
// CSI method signature
type CsiMethod = (args: any, cb: CsiDoneCb) => void;
type CsiMethodImpl = (args: any) => Promise<any>;

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
      grpcCode.INVALID_ARGUMENT,
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
      grpcCode.INVALID_ARGUMENT,
      'Missing volume capabilities'
    );
  }
  for (let i = 0; i < caps.length; i++) {
    const cap = caps[i];

    // TODO: Check that FS type is supported and mount options?
    if (cap.accessMode.mode !== 'SINGLE_NODE_WRITER') {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        `Access mode ${cap.accessMode.mode} not supported`
      );
    }
  }
}

// Generate CSI access constraits for the volume.
function getAccessibleTopology (volume: Volume): TopologyKeys[] {
  if (volume.spec.local) {
    // We impose a hard requirement on k8s to schedule the app to one of the
    // nodes with replica to make use of the locality. The nexus will follow
    // the app during the publish.
    return volume.getReplicas().map((r) => {
      return {
        segments: { 'kubernetes.io/hostname': r.pool!.node!.name }
      };
    });
  } else {
    // access from anywhere
    return [];
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
    accessibleTopology: getAccessibleTopology(volume),
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
  args: string; // stringified method args
  method: string; // CSI method name
  callbacks: CsiDoneCb[]; // callbacks to call when done

  constructor (args: any, method: string, cb: CsiDoneCb) {
    this.args = JSON.stringify(args);
    this.method = method;
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
export class CsiServer {
  private server: any;
  private ready: boolean;
  private registry: any;
  private volumes: Volumes | null;
  private sockPath: string;
  private nextListContextId: number;
  private listContexts: Record<string, ListContext>;
  private duplicateRequestCache: Request[];
  private serializationQueue: Workq;

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
    this.serializationQueue = new Workq('serial-csi');

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
      controllerMethods[name] = (
        call: grpc.ServerUnaryCall<any, any>,
        cb: (err: Error | undefined, resp?: any,
      ) => void) => {
        const args = call.request;
        log.trace(`CSI ${name} request: ${JSON.stringify(args)}`);

        if (!this.ready) {
          return cb(
            new GrpcError(
              grpcCode.UNAVAILABLE,
              'Not ready for serving requests'
            )
          );
        }

        // detect duplicate method
        let request = this._beginRequest(args, name, cb);
        if (!request) {
          // cb will be called when the original request completes - nothing to do
          return;
        }

        let csiMethodImpl = (args: any) => {
          return (<CsiMethodImpl> this[name as keyof CsiServer].bind(this))(args)
            .then((resp: any) => {
              log.trace(`CSI ${name} response: ${JSON.stringify(resp)}`);
              assert(request);
              this._endRequest(request, undefined, resp);
            })
            .catch((err: any) => {
              if (!(err instanceof GrpcError)) {
                err = new GrpcError(
                  grpcCode.UNKNOWN,
                  `Unexpected error in ${name} method: ` + err.stack
                );
              }
              log.error(`CSI ${name} failed: ${err}`);
              assert(request);
              this._endRequest(request, err);
            });
        };

        // We have to serialize create and publish volume requests because:
        // 1. create requests create havoc in space accounting
        // 2. concurrent publish reqs triggers blocking connect bug in mayastor
        // 3. they make the log file difficult to follow
        if (['createVolume', 'controllerPublishVolume'].indexOf(name) >= 0) {
          this.serializationQueue.push(args, (args) => csiMethodImpl(args));
        } else {
          csiMethodImpl(args);
        }
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
        cb(new GrpcError(grpcCode.UNIMPLEMENTED, msg));
      };
    });
    this.server.addService(csi.Controller.service, controllerMethods);
  }

  // Listen on UDS
  async start (): Promise<void> {
    try {
      await fs.lstat(this.sockPath);
      log.info('Removing stale socket file ' + this.sockPath);
      await fs.unlink(this.sockPath);
    } catch (err) {
      // the file does not exist which is ok
    }
    return new Promise((resolve, reject) => {
      this.server.bindAsync(
        'unix://' + this.sockPath,
        grpc.ServerCredentials.createInsecure(),
        (err: Error) => {
          if (err) {
            log.error(`CSI server failed to bind at ${this.sockPath}`);
            reject(new Error(`Bind failed: ${err}`));
          } else {
            log.info('CSI server listens at ' + this.sockPath);
            this.server.start();
            resolve();
          }
        }
      );
    });
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
  _findRequest (args: any, method: string): Request | undefined {
    args = JSON.stringify(args);
    return this.duplicateRequestCache.find(
      (e) => e.args === args && e.method === method
    );
  }

  _beginRequest (args: any, method: string, cb: CsiDoneCb): Request | undefined {
    let request = this._findRequest(args, method);
    if (request) {
      log.debug(`Duplicate ${method} volume request detected`);
      request.wait(cb);
      return;
    }
    request = new Request(args, method, cb);
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

  async controllerGetCapabilities (_: any) {
    const caps = [
      'CREATE_DELETE_VOLUME',
      'PUBLISH_UNPUBLISH_VOLUME',
      'LIST_VOLUMES',
      'GET_CAPACITY'
    ];
    log.debug('get capabilities request: ' + caps.join(', '));
    return {
      capabilities: caps.map((c) => {
        return { rpc: { type: c } };
      })
    };
  }

  async createVolume (args: any): Promise<any> {
    assert(this.volumes);

    log.debug(
      `Request to create volume "${args.name}" with size ` +
        args.capacityRange.requiredBytes +
        ` (limit ${args.capacityRange.limitBytes})`
    );

    if (args.volumeContentSource) {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        'Source for create volume is not supported'
      );
    }
    // k8s uses names pvc-{uuid} and we use uuid further as ID in SPDK so we
    // must require it.
    const m = args.name.match(PVC_RE);
    if (!m) {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        `Expected the volume name in pvc-{uuid} format: ${args.name}`
      );
    }
    const uuid = m[1];
    checkCapabilities(args.volumeCapabilities);

    // Storage protocol for accessing nexus is a required parameter
    const protocol = args.parameters && args.parameters.protocol;
    if (!protocol) {
      throw new GrpcError(grpcCode.INVALID_ARGUMENT, 'missing storage protocol');
    }
    const ioTimeout = args.parameters.ioTimeout;
    if (ioTimeout !== undefined) {
      if (protocol !== 'nvmf') {
        throw new GrpcError(
          grpcCode.INVALID_ARGUMENT,
          'ioTimeout is valid only for nvmf protocol'
        );
      }
      if (Object.is(parseInt(ioTimeout), NaN)) {
        throw new GrpcError(
          grpcCode.INVALID_ARGUMENT,
          'ioTimeout must be an integer'
        );
      }
    }

    // For exaplanation of accessibilityRequirements refer to a table at
    // https://github.com/kubernetes-csi/external-provisioner.
    // Our case is WaitForFirstConsumer = true, strict-topology = false.
    //
    // The first node in preferred array the node that was chosen for running
    // the app by the k8s scheduler. The rest of the entries are in random
    // order and perhaps don't even run mayastor csi node plugin.
    //
    // The requisite array contains all nodes in the cluster irrespective
    // of what node was chosen for running the app.
    //
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
            throw new GrpcError(
              grpcCode.INVALID_ARGUMENT,
              'Volume topology other than hostname not supported'
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
        throw new GrpcError(grpcCode.INVALID_ARGUMENT, 'Invalid replica count');
      }
    } else {
      count = 1;
    }

    // create the volume
    let volume = await this.volumes.createVolume(uuid, {
      replicaCount: count,
      local: YAML_TRUE_VALUE.indexOf(args.parameters.local) >= 0,
      preferredNodes: shouldNodes,
      requiredNodes: mustNodes,
      requiredBytes: args.capacityRange.requiredBytes,
      limitBytes: args.capacityRange.limitBytes,
      protocol: protocol
    });

    return {
      volume: {
        capacityBytes: volume.getSize(),
        volumeId: uuid,
        accessibleTopology: getAccessibleTopology(volume),
        // parameters defined in the storage class are only presented
        // to the CSI driver createVolume method.
        // Propagate them to other CSI driver methods involved in
        // standing up a volume, using the volume context.
        volumeContext: args.parameters
      }
    };
  }

  async deleteVolume (args: any) {
    assert(this.volumes);

    log.debug(`Request to destroy volume "${args.volumeId}"`);

    await this.volumes.destroyVolume(args.volumeId);
    log.info(`Volume "${args.volumeId}" destroyed`);
  }

  async listVolumes (args: any) {
    assert(this.volumes);
    let ctx: ListContext;

    if (args.startingToken) {
      ctx = this.listContexts[args.startingToken];
      delete this.listContexts[args.startingToken];
      if (!ctx) {
        throw new GrpcError(
          grpcCode.INVALID_ARGUMENT,
          'Paging context for list volumes is gone'
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
      return {
        entries: entries,
        nextToken: ctxId,
      };
    } else {
      return { entries: entries };
    }
  }

  async controllerPublishVolume (args: any): Promise<any> {
    assert(this.volumes);
    const publishContext: any = {};

    log.debug(
      `Request to publish volume "${args.volumeId}" for "${args.nodeId}"`
    );

    const volume = this.volumes.get(args.volumeId);
    if (!volume) {
      throw new GrpcError(
        grpcCode.NOT_FOUND,
        `Volume "${args.volumeId}" does not exist`
      );
    }
    let nodeId;
    nodeId = parseMayastorNodeId(args.nodeId);
    const ioTimeout = args.volumeContext?.ioTimeout;
    if (ioTimeout !== undefined) {
      // The value has been checked during the createVolume
      publishContext.ioTimeout = ioTimeout;
    }
    if (args.readonly) {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        'readonly volumes are unsupported'
      );
    }
    if (!args.volumeCapability) {
      throw new GrpcError(grpcCode.INVALID_ARGUMENT, 'missing volume capability');
    }
    checkCapabilities([args.volumeCapability]);

    try {
      publishContext.uri = await volume.publish(nodeId);
    } catch (err) {
      if (err.code !== grpcCode.ALREADY_EXISTS) {
        throw err;
      }
      log.debug(`Volume "${args.volumeId}" already published on this node`);
      return { publishContext };
    }

    log.info(`Published "${args.volumeId}" at ${publishContext.uri}`);
    return { publishContext };
  }

  async controllerUnpublishVolume (args: any) {
    assert(this.volumes);

    log.debug(`Request to unpublish volume "${args.volumeId}"`);

    const volume = this.volumes.get(args.volumeId);
    if (!volume) {
      log.warn(
        `Request to unpublish volume "${args.volumeId}" which does not exist`
      );
      return;
    }
    parseMayastorNodeId(args.nodeId);

    await volume.unpublish();
    log.info(`Unpublished volume "${args.volumeId}"`);
  }

  async validateVolumeCapabilities (args: any): Promise<any> {
    assert(this.volumes);

    log.debug(`Request to validate volume capabilities for "${args.volumeId}"`);

    if (!this.volumes.get(args.volumeId)) {
      throw new GrpcError(
        grpcCode.NOT_FOUND,
        `Volume "${args.volumeId}" does not exist`
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
    return resp;
  }

  // We understand just one topology segment type and that is hostname.
  // So if it is specified we return capacity of storage pools on the node
  // or capacity of all pools in the cluster.
  //
  // XXX Is the caller interested in total capacity (sum of all pools) or
  // a capacity usable by a single volume?
  async getCapacity (args: any) {
    let nodeName;

    if (args.volumeCapabilities) {
      checkCapabilities(args.volumeCapabilities);
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
    return { availableCapacity: capacity };
  }
}

module.exports = {
  CsiServer,
  // this is exported for the tests
  csi
};
