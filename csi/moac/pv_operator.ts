const log = require('./logger').Logger('pvoperator');

import {
  CoreV1Api,
  KubeConfig,
  V1PersistentVolume,
  Informer,
  makeInformer,
} from 'client-node-fixed-watcher';
import { Volumes } from './volumes';

export class PvOperator {
  informer: Informer<V1PersistentVolume>;
  listPvFn: any;
  volumes: Volumes; // Volume manager

  constructor(
    kubeConfig: KubeConfig,
    volumes: Volumes,
  ) {
    this.volumes = volumes;
    const k8sApi = kubeConfig.makeApiClient(CoreV1Api);
    this.listPvFn = () => k8sApi.listPersistentVolume();

    this.informer = makeInformer(kubeConfig, '/api/v1/persistentvolumes', this.listPvFn);
    this.informer.on('delete', async (obj: V1PersistentVolume) => { 
      if (obj.spec?.csi?.driver === 'io.openebs.csi-mayastor') {
        if (obj.spec?.claimRef?.uid != undefined) {
          let volUuid = obj.spec?.claimRef?.uid;
          log.debug(`PV: ${obj.metadata!.name} deleted, destroying volume ${volUuid}`)
          try {
            // destroyVolume has the desired semantics,
            // it does not fail if the volume is not found.
            this.volumes.destroyVolume(volUuid);
          } catch (err) {
            // The volume was found but there was an error when destroying it.
            log.error(`Failed to destroy volume "${volUuid}": ${err}`);
          }
        } else {
          log.console.warn(`PV: ${obj.metadata!.name} deleted, unable to access uid`);
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
    this.informer.start();
  }
}
