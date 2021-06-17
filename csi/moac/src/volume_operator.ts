// Volume operator managing volume k8s custom resources.
//
// Primary motivation for the resource is to provide information about
// existing volumes. Other actions and their consequences follow:
//
// * destroying the resource implies volume destruction (not advisable)
// * creating the resource implies volume import (not advisable)
// * modification of "preferred nodes" property influences scheduling of new replicas
// * modification of "required nodes" property moves the volume to different nodes
// * modification of replica count property changes redundancy of the volume
//
// Volume operator stands between k8s custom resource (CR) describing desired
// state and volume manager reflecting the actual state. It gets new/mod/del
// events from both, from the world of ideas and from the world of material
// things. It's task which is not easy, is to restore harmony between them:
//
// +---------+ new/mod/del  +----------+  new/mod/del  +-----------+
// | Volumes +--------------> Operator <---------------+  Watcher  |
// +------^--+              ++--------++               +---^-------+
//        |                  |        |                    |
//        |                  |        |                    |
//        +------------------+        +--------------------+
//       create/modify/destroy         create/modify/destroy
//
//
//  real object event  |    CR exists    |  CR does not exist
// ------------------------------------------------------------
//        new          |      --         |   create CR
//        mod          |    modify CR    |      --
//        del          |    delete CR    |      --
//
//
//      CR event       |  volume exists  |  volume does not exist
// ---------------------------------------------------------------
//        new          |  modify volume  |   create volume
//        mod          |  modify volume  |      --
//        del          |  delete volume  |      --
//

const yaml = require('js-yaml');

import assert from 'assert';
import * as fs from 'fs';
import * as _ from 'lodash';
import * as path from 'path';
import {
  ApiextensionsV1Api,
  KubeConfig,
} from '@kubernetes/client-node';
import {
  CustomResource,
  CustomResourceCache,
  CustomResourceMeta,
} from './watcher';
import { EventStream } from './event_stream';
import { protocolFromString } from './nexus';
import { Replica } from './replica';
import { Volume } from './volume';
import { Volumes } from './volumes';
import { VolumeSpec, VolumeState, volumeStateFromString } from './volume';
import { Workq } from './workq';
import { Logger } from './logger';

const log = Logger('volume-operator');

const RESOURCE_NAME: string = 'mayastorvolume';
const crdVolume = yaml.load(
  fs.readFileSync(path.join(__dirname, '../crds/mayastorvolume.yaml'), 'utf8')
);
// lower-case letters uuid pattern
const uuidRegexp = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/;

// Optional status part in volume resource
export type VolumeStatus = {
  size: number,
  state: VolumeState,
  reason?: string,
  targetNodes?: string[], // node name of nexus if the volume is published
  replicas: {
    node: string,
    pool: string,
    uri: string,
    offline: boolean,
  }[],
  nexus?: {
    node: string,
    deviceUri?: string,
    state: string,
    children: {
      uri: string,
      state: string,
    }[]
  }
};

// Object defines properties of node resource.
export class VolumeResource extends CustomResource {
  apiVersion?: string;
  kind?: string;
  metadata: CustomResourceMeta;
  spec: VolumeSpec;
  status?: VolumeStatus;

  constructor(cr: CustomResource) {
    super();
    this.apiVersion = cr.apiVersion;
    this.kind = cr.kind;
    if (cr.metadata?.name === undefined) {
      throw new Error('Missing name attribute');
    }
    this.metadata = cr.metadata;
    if (!cr.metadata.name.match(uuidRegexp)) {
      throw new Error(`Invalid UUID`);
    }
    let spec = cr.spec as any;
    if (spec === undefined) {
      throw new Error('Missing spec section');
    }
    if (!spec.requiredBytes) {
      throw new Error('Missing requiredBytes');
    }
    this.spec = <VolumeSpec> {
      replicaCount: spec.replicaCount || 1,
      local: spec.local || false,
      preferredNodes: [].concat(spec.preferredNodes || []),
      requiredNodes: [].concat(spec.requiredNodes || []),
      requiredBytes: spec.requiredBytes,
      limitBytes: spec.limitBytes || 0,
      protocol: protocolFromString(spec.protocol)
    };
    let status = cr.status as any;
    if (status !== undefined) {
      this.status = <VolumeStatus> {
        size: status.size || 0,
        state: volumeStateFromString(status.state),
        // sort the replicas according to node name to have deterministic order
        replicas: []
        .concat(status.replicas || [])
        .sort((a: any, b: any) => a.node.localeCompare(b.node)),
      };
      if (status.targetNodes && status.targetNodes.length > 0) {
        this.status.targetNodes = [].concat(status.targetNodes).sort();
      }
      if (status.nexus) {
        this.status.nexus = status.nexus;
      }
    }
  }

  getUuid(): string {
    let uuid = this.metadata.name;
    if (uuid === undefined) {
      throw new Error('Volume resource without UUID');
    } else {
      return uuid;
    }
  }
}

// Volume operator managing volume k8s custom resources.
export class VolumeOperator {
  namespace: string;
  volumes: Volumes; // Volume manager
  eventStream: any; // A stream of node, replica and nexus events.
  watcher: CustomResourceCache<VolumeResource>; // volume resource watcher.
  workq: Workq; // Events from k8s are serialized so that we don't flood moac by
                // concurrent changes to volumes.

  // Create volume operator object.
  //
  // @param namespace     Namespace the operator should operate on.
  // @param kubeConfig    KubeConfig.
  // @param volumes       Volume manager.
  // @param [idleTimeout] Timeout for restarting watcher connection when idle.
  constructor (
    namespace: string,
    kubeConfig: KubeConfig,
    volumes: Volumes,
    idleTimeout: number | undefined,
  ) {
    this.namespace = namespace;
    this.volumes = volumes;
    this.eventStream = null;
    this.workq = new Workq('mayastorvolume');
    this.watcher = new CustomResourceCache(
      this.namespace,
      RESOURCE_NAME,
      kubeConfig,
      VolumeResource,
      { idleTimeout }
    );
  }

  // Create volume CRD if it doesn't exist.
  //
  // @param kubeConfig  KubeConfig.
  async init (kubeConfig: KubeConfig) {
    log.info('Initializing volume operator');
    let k8sExtApi = kubeConfig.makeApiClient(ApiextensionsV1Api);
    try {
      await k8sExtApi.createCustomResourceDefinition(crdVolume);
      log.info(`Created CRD ${RESOURCE_NAME}`);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
  }

  // Start volume operator's watcher loop.
  //
  // NOTE: Not getting the start sequence right can have catastrophic
  // consequence leading to unintended volume destruction and data loss.
  //
  async start () {
    var self = this;

    // install event handlers to follow changes to resources.
    this._bindWatcher(this.watcher);
    await this.watcher.start();

    // This will start async processing of volume events.
    this.eventStream = new EventStream({ volumes: this.volumes });
    this.eventStream.on('data', async (ev: any) => {
      // the only kind of event that comes from the volumes source
      assert(ev.kind === 'volume');
      self.workq.push(ev, self._onVolumeEvent.bind(self));
    });
  }

  async _onVolumeEvent (ev: any) {
    const uuid = ev.object.uuid;

    if (ev.eventType === 'new' || ev.eventType === 'mod') {
      const origObj = this.watcher.get(uuid);
      const spec = <VolumeSpec> ev.object.spec;
      const status = this._volumeToStatus(ev.object);

      if (origObj !== undefined) {
        await this._updateSpec(uuid, origObj, spec);
      } else if (ev.eventType === 'new') {
        try {
          await this._createResource(uuid, spec);
        } catch (err) {
          log.error(`Failed to create volume resource "${uuid}": ${err}`);
          return;
        }
      }
      await this._updateStatus(uuid, status);
    } else if (ev.eventType === 'del') {
      await this._deleteResource(uuid);
    } else {
      assert(false);
    }
  }

  // Transform volume to status properties used in k8s volume resource.
  //
  // @param   volume   Volume object.
  // @returns Status properties.
  //
  _volumeToStatus (volume: Volume): VolumeStatus {
    const st: VolumeStatus = {
      size: volume.getSize(),
      state: volume.state,
      replicas: volume.getReplicas()
        // ignore replicas that are being removed (disassociated from node)
        .filter((r: Replica) => !!r.pool?.node)
        .map((r: Replica) => {
          return {
            node: r.pool!.node!.name,
            pool: r.pool!.name,
            uri: r.uri,
            offline: r.isOffline()
          };
        })
        // enforce consistent order - important when comparing status objects
        .sort((r1, r2) => r1.node.localeCompare(r2.node))
    };
    const nodeName = volume.getNodeName();
    if (nodeName) {
      // NOTE: sort it when we have more than just one entry
      st.targetNodes = [ nodeName ];
    }
    const nexus = volume.getNexus();
    if (nexus && nexus.node) {
      st.nexus = {
        node: nexus.node.name,
        state: nexus.state,
        children: nexus.children.map((ch: any) => {
          return {
            uri: ch.uri,
            state: ch.state
          };
        })
      };
      if (nexus.deviceUri) {
        st.nexus.deviceUri = nexus.deviceUri;
      }
    }
    return st;
  }

  // Create k8s CRD object.
  //
  // @param uuid       ID of the created volume.
  // @param spec       New volume spec.
  //
  async _createResource (uuid: string, spec: VolumeSpec) {
    await this.watcher.create({
      apiVersion: 'openebs.io/v1alpha1',
      kind: 'MayastorVolume',
      metadata: {
        name: uuid,
        namespace: this.namespace
      },
      spec
    });
  }

  // Update properties of k8s CRD object or create it if it does not exist.
  //
  // @param uuid       ID of the updated volume.
  // @param origObj    Existing k8s resource object.
  // @param spec       New volume spec.
  //
  async _updateSpec (uuid: string, origObj: VolumeResource, spec: VolumeSpec) {
    try {
      await this.watcher.update(uuid, (orig: VolumeResource) => {
        // Update object only if it has really changed
        if (_.isEqual(origObj.spec, spec)) {
          return;
        }
        log.info(`Updating spec of volume resource "${uuid}"`);
        return {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorVolume',
          metadata: orig.metadata,
          spec,
        };
      });
    } catch (err) {
      log.error(`Failed to update volume resource "${uuid}": ${err}`);
      return;
    }
  }

  // Update status of the volume based on real data obtained from storage node.
  //
  // @param uuid    UUID of the resource.
  // @param status  Status properties.
  //
  async _updateStatus (uuid: string, status: VolumeStatus) {
    try {
      await this.watcher.updateStatus(uuid, (orig: VolumeResource) => {
        if (_.isEqual(orig.status, status)) {
          // avoid unnecessary status updates
          return;
        }
        log.debug(`Updating status of volume resource "${uuid}"`);
        // merge old and new properties
        return {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorNode',
          metadata: orig.metadata,
          spec: orig.spec,
          status,
        };
      });
    } catch (err) {
      log.error(`Failed to update status of volume resource "${uuid}": ${err}`);
    }
  }

  // Set state and reason not touching the other status fields.
  async _updateState (uuid: string, state: VolumeState, reason: string) {
    try {
      await this.watcher.updateStatus(uuid, (orig: VolumeResource) => {
        if (orig.status?.state === state && orig.status?.reason === reason) {
          // avoid unnecessary status updates
          return;
        }
        log.debug(`Updating state of volume resource "${uuid}"`);
        // merge old and new properties
        let newStatus = _.assign({}, orig.status, { state, reason });
        return {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorNode',
          metadata: orig.metadata,
          spec: orig.spec,
          status: newStatus,
        };
      });
    } catch (err) {
      log.error(`Failed to update status of volume resource "${uuid}": ${err}`);
    }
  }

  // Delete volume resource with specified uuid.
  //
  // @param uuid   UUID of the volume resource to delete.
  //
  async _deleteResource (uuid: string) {
    try {
      log.info(`Deleting volume resource "${uuid}"`);
      await this.watcher.delete(uuid);
    } catch (err) {
      log.error(`Failed to delete volume resource "${uuid}": ${err}`);
    }
  }

  // Stop listening for watcher and node events and reset the cache
  async stop () {
    this.watcher.stop();
    this.watcher.removeAllListeners();
    if (this.eventStream) {
      this.eventStream.destroy();
      this.eventStream = null;
    }
  }

  // Bind watcher's new/mod/del events to volume operator's callbacks.
  //
  // @param watcher   k8s volume resource cache.
  //
  _bindWatcher (watcher: CustomResourceCache<VolumeResource>) {
    watcher.on('new', (obj: VolumeResource) => {
      this.workq.push(obj, this._importVolume.bind(this));
    });
    watcher.on('mod', (obj: VolumeResource) => {
      this.workq.push(obj, this._modifyVolume.bind(this));
    });
    watcher.on('del', (obj: VolumeResource) => {
      this.workq.push(obj.metadata.name!, this._destroyVolume.bind(this));
    });
  }

  // When moac restarts the volume manager does not know which volumes exist.
  // We need to import volumes based on the k8s resources.
  //
  // @param resource    Volume resource properties.
  //
  async _importVolume (resource: VolumeResource) {
    const uuid = resource.getUuid();

    log.debug(`Importing volume "${uuid}" in response to "new" resource event`);
    try {
      this.volumes.importVolume(uuid, resource.spec, resource.status);
    } catch (err) {
      log.error(
        `Failed to import volume "${uuid}" based on new resource: ${err}`
      );
      await this._updateState(uuid, VolumeState.Error, err.toString());
    }
  }

  // Modify volume according to the specification.
  //
  // @param resource    Volume resource properties.
  //
  async _modifyVolume (resource: VolumeResource) {
    const uuid = resource.getUuid();
    const volume = this.volumes.get(uuid);

    if (!volume) {
      log.warn(
        `Volume resource "${uuid}" was modified but the volume does not exist`
      );
      return;
    }
    try {
      volume.update(resource.spec);
    } catch (err) {
      log.error(`Failed to update volume "${uuid}" based on resource: ${err}`);
    }
  }

  // Remove the volume from internal state and if it exists destroy it.
  //
  // @param uuid   ID of the volume to destroy.
  //
  async _destroyVolume (uuid: string) {
    const volume = this.volumes.get(uuid);
    if (!volume) {
      log.warn(
        `Volume resource "${uuid}" was deleted but the volume does not exist`
      );
      return;
    } else if (volume.state === VolumeState.Destroyed) {
      log.warn(`Destruction of volume "${uuid}" is already in progress`);
      return;
    }

    log.debug(
      `Destroying volume "${uuid}" in response to "del" resource event`
    );

    try {
      await this.volumes.destroyVolume(uuid);
    } catch (err) {
      log.error(`Failed to destroy volume "${uuid}": ${err}`);
    }
  }
}
