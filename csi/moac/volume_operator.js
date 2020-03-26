// Volume operator managing volume k8s custom resources.
//
// Primary motivation for the resource is to provide information about
// existing volumes. Other actions and their consequences follow:
//
// * destroying the resource implies volume destruction (not advisable)
// * creating the resource implies volume creation (not advisable)
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

'use strict';

const _ = require('lodash');
const assert = require('assert');
const fs = require('fs');
const yaml = require('js-yaml');
const EventStream = require('./event_stream');
const log = require('./logger').Logger('volume-operator');
const Watcher = require('./watcher');
const Workq = require('./workq');

const crdVolume = yaml.safeLoad(
  fs.readFileSync(__dirname + '/crds/mayastorvolume.yaml', 'utf8')
);
// lower-case letters uuid pattern
const uuidRegexp = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/;

// Volume operator managing volume k8s custom resources.
class VolumeOperator {
  constructor(namespace) {
    this.namespace = namespace;
    this.k8sClient = null; // k8s client
    this.volumes = null; // Volume manager
    this.eventStream = null; // A stream of node, replica and nexus events.
    this.watcher = null; // volume resource watcher.
    this.createdBySelf = []; // UUIDs of volumes created by the operator itself
    // Events from k8s are serialized so that we don't flood moac by
    // concurrent changes to volumes.
    this.workq = new Workq();
  }

  // Create volume CRD if it doesn't exist and augment client object so that CRD
  // can be manipulated as any other standard k8s api object.
  //
  // @param {object} k8sClient   Client for k8s api server.
  // @param {object} volumes     Volume manager.
  //
  async init(k8sClient, volumes) {
    log.info('Initializing volume operator');

    try {
      await k8sClient.apis[
        'apiextensions.k8s.io'
      ].v1beta1.customresourcedefinitions.post({ body: crdVolume });
      log.info('Created CRD ' + crdVolume.spec.names.kind);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
    k8sClient.addCustomResourceDefinition(crdVolume);

    this.k8sClient = k8sClient;
    this.volumes = volumes;

    // Initialize watcher with all callbacks for new/mod/del events
    this.watcher = new Watcher(
      'volume',
      this.k8sClient.apis['openebs.io'].v1alpha1.namespaces(
        this.namespace
      ).mayastorvolumes,
      this.k8sClient.apis['openebs.io'].v1alpha1.watch.namespaces(
        this.namespace
      ).mayastorvolumes,
      this._filterMayastorVolume
    );
  }

  // Normalize k8s mayastor volume resource.
  //
  // @param   {object} msv   MayaStor volume custom resource.
  // @returns {object} Properties defining a volume.
  //
  _filterMayastorVolume(msv) {
    // We should probably validate the whole record using json scheme or
    // something like that, but for now do just the basic check.
    if (!msv.metadata.name.match(uuidRegexp)) {
      log.warn(
        `Ignoring mayastor volume resource with invalid UUID: ${msv.metadata.name}`
      );
      return null;
    }
    if (!msv.spec.requiredBytes) {
      log.warn('Ignoring mayastor volume resource without requiredBytes');
      return null;
    }
    let props = {
      // spec part
      metadata: { name: msv.metadata.name },
      spec: {
        replicaCount: msv.spec.replicaCount || 1,
        preferredNodes: [].concat(msv.spec.preferredNodes || []).sort(),
        requiredNodes: [].concat(msv.spec.requiredNodes || []).sort(),
        requiredBytes: msv.spec.requiredBytes,
        limitBytes: msv.spec.limitBytes || 0,
      },
    };
    // volatile part
    let st = msv.status;
    if (st) {
      props.status = {
        size: st.size,
        state: st.state,
        node: st.node,
        // sort the replicas according to uri to have deterministic order
        replicas: [].concat(st.replicas || []).sort((a, b) => {
          if (a.uri < b.uri) return -1;
          else if (a.uri > b.uri) return 1;
          else return 0;
        }),
      };
    }

    return props;
  }

  // Start volume operator's watcher loop.
  //
  // NOTE: Not getting the start sequence right can have catastrophic
  // consequence leading to unintended volume destruction and data loss.
  // Therefore it's important not to call this function before volume
  // manager and registry have been started up.
  //
  async start() {
    var self = this;

    // install event handlers to follow changes to resources.
    self._bindWatcher(self.watcher);
    await self.watcher.start();

    // This will start async processing of volume events.
    self.eventStream = new EventStream({ volumes: self.volumes });
    self.eventStream.on('data', async ev => {
      // the only kind of event that comes from the volumes source
      assert(ev.kind == 'volume');
      let uuid = ev.object.uuid;

      if (ev.eventType == 'new' || ev.eventType == 'mod') {
        let uuid = ev.object.uuid;
        let k8sVolume = self.watcher.getRaw(uuid);
        let spec = self._volumeToSpec(ev.object);
        let status = self._volumeToStatus(ev.object);

        if (k8sVolume) {
          try {
            await self._updateResource(uuid, k8sVolume, spec);
          } catch (err) {
            log.error(`Failed to update volume resource "${uuid}": ${err}`);
            return;
          }
        } else if (ev.eventType == 'new' && !k8sVolume) {
          try {
            await self._createResource(uuid, spec);
          } catch (err) {
            log.error(`Failed to create volume resource "${uuid}": ${err}`);
            return;
          }
          // Note down that the volume existed so we don't try to create it
          // again when handling watcher new event.
          self.createdBySelf.push(uuid);
        }
        await this._updateStatus(uuid, status);
      } else if (ev.eventType == 'del') {
        await self._deleteResource(uuid);
      } else {
        assert(false);
      }
    });
  }

  // Transform volume to spec properties used in k8s volume resource.
  //
  // @param   {object} volume   Volume object.
  // @returns {object} Spec properties.
  //
  _volumeToSpec(volume) {
    return {
      replicaCount: volume.replicaCount,
      preferredNodes: _.clone(volume.preferredNodes),
      requiredNodes: _.clone(volume.requiredNodes),
      requiredBytes: volume.requiredBytes,
      limitBytes: volume.limitBytes,
    };
  }

  // Transform volume to status properties used in k8s volume resource.
  //
  // @param   {object} volume   Volume object.
  // @returns {object} Status properties.
  //
  _volumeToStatus(volume) {
    return {
      size: volume.getSize(),
      state: volume.state,
      reason: volume.reason,
      node: volume.getNodeName(),
      replicas: Object.values(volume.replicas).map(r => {
        return {
          node: r.pool.node.name,
          pool: r.pool.name,
          uri: r.uri,
          state: r.state,
        };
      }),
    };
  }

  // Create k8s CRD object.
  //
  // @param {string} uuid       ID of the updated volume.
  // @param {object} spec       New volume spec.
  //
  async _createResource(uuid, spec) {
    log.info(`Creating volume resource "${uuid}"`);
    await this.k8sClient.apis['openebs.io'].v1alpha1
      .namespaces(this.namespace)
      .mayastorvolumes.post({
        body: {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorVolume',
          metadata: {
            name: uuid,
            namespace: this.namespace,
          },
          spec,
        },
      });
  }

  // Update properties of k8s CRD object or create it if it does not exist.
  //
  // @param {string} uuid       ID of the updated volume.
  // @param {object} k8sVolume  Existing k8s resource object.
  // @param {object} spec       New volume spec.
  //
  async _updateResource(uuid, k8sVolume, spec) {
    // Update object only if it has really changed
    if (!_.isEqual(k8sVolume.spec, spec)) {
      log.info(`Updating spec of volume resource "${uuid}"`);
      await this.k8sClient.apis['openebs.io'].v1alpha1
        .namespaces(this.namespace)
        .mayastorvolumes(uuid)
        .put({
          body: {
            apiVersion: 'openebs.io/v1alpha1',
            kind: 'MayastorVolume',
            metadata: k8sVolume.metadata,
            spec: _.assign(k8sVolume.spec, spec),
          },
        });
    }
  }

  // Update state and reason of the resource.
  //
  // NOTE: This method does not throw if the operation fails as there is nothing
  // we can do if it fails. Though we log an error message in such a case.
  //
  // @param {string} uuid    UUID of the resource.
  // @param {object} status  Status properties.
  //
  async _updateStatus(uuid, status) {
    var k8sVolume = this.watcher.getRaw(uuid);
    if (!k8sVolume) {
      log.warn(
        `Wanted to update state of volume resource "${uuid}" that disappeared`
      );
      return;
    }
    if (!k8sVolume.status) {
      k8sVolume.status = {};
    }
    if (_.isEqual(k8sVolume.status, status)) {
      // avoid unnecessary status updates
      return;
    }
    log.debug(`Updating status of volume resource "${uuid}"`);
    _.assign(k8sVolume.status, status);
    try {
      await this.k8sClient.apis['openebs.io'].v1alpha1
        .namespaces(this.namespace)
        .mayastorvolumes(uuid)
        .status.put({ body: k8sVolume });
    } catch (err) {
      log.error(`Failed to update status of volume resource "${uuid}": ${err}`);
    }
  }

  // Delete volume resource with specified uuid.
  //
  // @param {string} uuid   UUID of the volume resource to delete.
  //
  async _deleteResource(uuid) {
    var k8sVolume = this.watcher.getRaw(uuid);
    if (k8sVolume) {
      log.info(`Deleting volume resource "${uuid}"`);
      try {
        await this.k8sClient.apis['openebs.io'].v1alpha1
          .namespaces(this.namespace)
          .mayastorvolumes(uuid)
          .delete();
      } catch (err) {
        log.error(`Failed to delete volume resource "${uuid}": ${err}`);
      }
    }
  }

  // Stop listening for watcher and node events and reset the cache
  async stop() {
    this.watcher.removeAllListeners();
    await this.watcher.stop();
    this.eventStream.destroy();
    this.eventStream = null;
  }

  // Bind watcher's new/mod/del events to volume operator's callbacks.
  //
  // @param {object} watcher   k8s volume resource watcher.
  //
  _bindWatcher(watcher) {
    var self = this;
    watcher.on('new', obj => {
      self.workq.push(obj, self._createVolume.bind(self));
    });
    watcher.on('mod', obj => {
      self.workq.push(obj, self._modifyVolume.bind(self));
    });
    watcher.on('del', obj => {
      self.workq.push(obj.metadata.name, self._destroyVolume.bind(self));
    });
  }

  // Create a volume or update its spec if it already exists.
  //
  // @param {object}   resource    Volume resource properties.
  //
  async _createVolume(resource) {
    let uuid = resource.metadata.name;
    let createdIdx = this.createdBySelf.indexOf(uuid);
    if (createdIdx >= 0) {
      // don't react to self
      this.createdBySelf.splice(createdIdx, 1);
      return;
    }
    log.debug(`Creating volume "${uuid}" in response to "new" resource event`);
    try {
      await this.volumes.createVolume(uuid, resource.spec);
    } catch (err) {
      log.error(
        `Failed to create volume "${uuid}" based on new resource: ${err}`
      );
      await this._updateStatus(uuid, {
        state: 'PENDING',
        reason: err.toString(),
      });
    }
  }

  // Modify volume according to the specification.
  //
  // @param {object}   resource    Volume resource properties.
  //
  async _modifyVolume(resource) {
    let uuid = resource.metadata.name;
    let volume = this.volumes.get(uuid);

    if (!volume) {
      log.warn(
        `Volume resource "${uuid}" was modified but the volume does not exist`
      );
      return;
    }
    try {
      if (volume.update(resource.spec)) {
        log.debug(
          `Updating volume "${uuid}" in response to "mod" resource event`
        );
        await volume.ensure();
      }
    } catch (err) {
      log.error(`Failed to update volume "${uuid}" based on resource: ${err}`);
    }
  }

  // Remove the volume from internal state and if it exists destroy it.
  //
  // @param {string} uuid   ID of the volume to destroy.
  //
  async _destroyVolume(uuid) {
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

module.exports = VolumeOperator;
