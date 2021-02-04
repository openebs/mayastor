const log = require('./logger').Logger('pvhandler');

import {
  CoreV1Api,
  KubeConfig,
  V1PersistentVolume,
  Informer,
  makeInformer,
} from 'client-node-fixed-watcher'
import { Volumes } from './volumes';


const kc = new KubeConfig();
kc.loadFromDefault();
const k8sApi = kc.makeApiClient(CoreV1Api);
const listFn = () => k8sApi.listPersistentVolume();

export class PVHandler {
  informer: Informer<V1PersistentVolume>
  listPvFn = k8sApi.listPersistentVolume();
  volumes: Volumes; // Volume manager

  constructor(
    volumes: Volumes,
  ) {
    this.volumes = volumes
    log.debug(`PVHandler, created`);
    this.informer = makeInformer(kc, '/api/v1/persistentvolumes', listFn)
    this.informer.on('delete', async (obj: V1PersistentVolume) => { 
      if (obj.spec?.csi?.driver === 'io.openebs.csi-mayastor') {
        // Initiate deletion of the MSV here.
        // Deletion of an already deleted MSV should not fail,
        // it may have been deleted already if reclaimPolicy is not Retain.
        // FIXME: We need to avoid a race between 2 paths attempting to delete
        // the associated MSV,
        // Possible fixes:
        //  1. Single path, i.e. delegate MSV deletion to this informer only.
        //  2. "Deleting" an MSV => pushing an event on a queue,
        //      the semantics of the event are "delete identified MSV if found"
        //  3. Generate the 'del' event for the volume, this has the desired 
        //    semantics see VolumeOperator._bindWatcher
        // Simplest solution is 3 and the one selected.
        if (obj.spec?.claimRef?.uid != undefined) {
          log.debug(`PV: ${obj.metadata!.name} deleted, MSV uuid is ${obj.spec?.claimRef?.uid}`)
          log.debug(
            `Destroying volume "${obj.spec?.claimRef?.uid}" in response to "del" resource event`
          );
          try {
            // scheduleDestroyVolume has the desired semantics,
            // it does not fail if the volume is not found.
            await this.volumes.scheduleDestroyVolume(obj.spec?.claimRef?.uid);
            log.info(`Scheduled destroy volume "${obj.spec?.claimRef?.uid}"`);
          } catch (err) {
            // The volume was found but there was an error when destroying it.
            log.error(`Failed to destroy volume "${obj.spec?.claimRef?.uid}": ${err}`);
          }
        } else {
          log.info(`PV: ${obj.metadata!.name} deleted, unable to access uid`)
        }
      }
    });

    this.informer.on('error', (err: V1PersistentVolume) => {
      log.error(`${err}, restarting after 5 seconds`);
      // Restart informer after 5sec
      setTimeout(() => { this.informer.start(); }, 5000);
    });

  }

  start() {
    this.informer.start()
  }
}
