//
'use strict';

const k8s = require('@kubernetes/client-node');
const log = require('./logger').Logger('finalizer_helper');

export class FinalizerHelper {
  private kubeConfig: any;
  private k8sApi: any;
  private namespace: String;
  private groupname: String;
  private version: String;
  private plural: String;

  constructor (namespace: String, groupname:String, version:String, plural:String) {
      this.namespace = namespace;
      this.groupname = groupname;
      this.version = version;
      this.kubeConfig = new k8s.KubeConfig();
      this.kubeConfig.loadFromDefault();
      this.k8sApi = this.kubeConfig.makeApiClient(k8s.CustomObjectsApi);
      this.plural = plural;
  }

  addFinalizer(body: any, instancename: String, finalizer: String) {
    if (body.metadata.deletionTimestamp != undefined) {
      log.warn(`addFinalizer(${instancename},${finalizer}), deletionTimestamp is set`);
      return;
    }

    if (body.metadata.finalizers != undefined) {
      const index = body.metadata.finalizers.indexOf(finalizer);
      if ( index > -1) {
        log.debug(`@addFinalizer(${instancename},${finalizer}), finalizer already present`);
        return;
      }
      body.metadata.finalizers.push(finalizer);
    } else {
      body.metadata.finalizers = [finalizer];
    }

    // TODO: use patchNamespacedCustomObject
    this.k8sApi.replaceNamespacedCustomObject(
        this.groupname,
        this.version,
        this.namespace,
        this.plural,
        instancename,
        body)
        .then((res:any) => {
          log.debug(`added finalizer:${finalizer} to ${this.plural}:${instancename}`);
        })
        .catch((err:any) => {
         log.error(`add finalizer:${finalizer} to ${this.plural}:${instancename}, update failed: code=${err.body.code}, reason=${err.body.reason}, ${err.body.message}`);
        });
  }

  removeFinalizer(body: any, instancename: String, finalizer: String) {
    if (body.metadata.finalizers == undefined) {
        log.debug(`removeFinalizer(${instancename},${finalizer}), no finalizers defined.`);
        return;
    }

    const index = body.metadata.finalizers.indexOf(finalizer);
    if ( index < 0) {
        log.debug(`removeFinalizer(${instancename},${finalizer}), finalizer not found`);
        return;
    }
    body.metadata.finalizers.splice(index, 1);

    // TODO: use patchNamespacedCustomObject
    this.k8sApi.replaceNamespacedCustomObject(
      this.groupname,
      this.version,
      this.namespace,
      this.plural,
      instancename,
      body).
      then((res:any) => {
        log.debug(`removed finalizer:${finalizer} from ${this.plural}:${instancename}`);
      })
      .catch((err: any) => {
        log.error(`remove finalizer:${finalizer} from ${this.plural}:${instancename}, update failed: code=${err.body.code}, reason=${err.body.reason}, ${err.body.message}`);
      });
  }

  addFinalizerToCR(instancename: String, finalizer: String) {
      this.k8sApi.getNamespacedCustomObject(
          this.groupname,
          this.version,
          this.namespace,
          this.plural,
          instancename)
          .then((customresource:any) => {
              let body = customresource.body;

              if (body.metadata.deletionTimestamp != undefined) {
                log.warn(`addFinalizerToCR(${instancename},${finalizer}), deletionTimestamp is set`);
                return;
              }

              if (body.metadata.finalizers != undefined) {
                const index = body.metadata.finalizers.indexOf(finalizer);
                if ( index > -1) {
                  log.debug(`@addFinalizerToCR(${instancename},${finalizer}), finalizer already present`);
                  return;
                }
                body.metadata.finalizers.splice(-1, 0, finalizer);
              } else {
                body.metadata.finalizers = [finalizer];
              }

              // TODO: use patchNamespacedCustomObject
              this.k8sApi.replaceNamespacedCustomObject(
                  this.groupname,
                  this.version,
                  this.namespace,
                  this.plural,
                  instancename,
                  body)
                  .then((res:any) => {
                    log.debug(`added finalizer:${finalizer} to ${this.plural}:${instancename}`);
                  })
                  .catch((err:any) => {
                   log.error(`add finalizer:${finalizer} to ${this.plural}:${instancename}, update failed: code=${err.body.code}, reason=${err.body.reason}, ${err.body.message}`);
                  });
          })
          .catch((err: any) => {
            log.error(`add finalizer:${finalizer} to ${this.plural}:${instancename}, get failed: code=${err.body.code}, reason=${err.body.reason}, ${err.body.message}`);
          });
  }

  removeFinalizerFromCR(instancename: String, finalizer: String) {
      this.k8sApi.getNamespacedCustomObject(
          this.groupname,
          this.version,
          this.namespace,
          this.plural,
          instancename)
          .then((customresource:any) => {
              let body = customresource.body;
              if (body.metadata.finalizers == undefined) {
                  log.debug(`removeFinalizerFromCR(${instancename},${finalizer}), no finalizers on pool`);
                  return;
              }

              const index = body.metadata.finalizers.indexOf(finalizer);
              if ( index < 0) {
                  log.debug(`removeFinalizerFromCR(${instancename},${finalizer}), finalizer not found`);
                  return;
              }
              body.metadata.finalizers.splice(index, 1);

              // TODO: use patchNamespacedCustomObject
              this.k8sApi.replaceNamespacedCustomObject(
                this.groupname,
                this.version,
                this.namespace,
                this.plural,
                instancename,
                body).
                then((res:any) => {
                  log.debug(`removed finalizer:${finalizer} from ${this.plural}:${instancename}`);
                })
                .catch((err: any) => {
                  log.error(`remove finalizer:${finalizer} from ${this.plural}:${instancename}, update failed: code=${err.body.code}, reason=${err.body.reason}, ${err.body.message}`);
                });
          })
          .catch((err: any) => {
            log.error(`remove finalizer:${finalizer} from ${this.plural}:${instancename}, get failed: code=${err.body.code}, reason=${err.body.reason}, ${err.body.message}`);
          });
  }
}
