#! /usr/bin/python3

import libvirt
import time
import yaml

from kubernetes import config, dynamic
from kubernetes.client import api_client

configuration = config.load_kube_config()
client = dynamic.DynamicClient(api_client.ApiClient(configuration=configuration))


def wait_for(kind, api, name, namespace, timeout, status, ready):
    if status is not None:
        if ready(status):
            return 0, 0
    timeout += 2
    duration = 1
    elapsed = 1
    n = 0
    while elapsed < timeout:
        n += 1
        time.sleep(duration)
        duration, elapsed = elapsed, duration + elapsed
        entity = api.get(name=name, namespace=namespace)
        status = entity.get('status')
        if status is not None:
            if ready(status):
                return n, elapsed - 1
    raise Exception("timeout waiting for %s [%s] in namespace [%s] (checks=%d elapsed=%ds)" % (kind, name, namespace, n, elapsed - 1))


def create_object(kind, api, body, name, namespace, timeout, ready):
    entity = api.create(body=body, namespace=namespace)
    n, elapsed = wait_for(kind, api, name, namespace, timeout, entity.get('status'), ready)
    print("created %s [%s] in namespace [%s] (checks=%d elapsed=%ds)" % (kind, name, namespace, n, elapsed))


def wait_until_deleted(kind, api, name, namespace, timeout):
    timeout += 2
    duration = 1
    elapsed = 1
    n = 0
    while elapsed < timeout:
        n += 1
        time.sleep(duration)
        duration, elapsed = elapsed, duration + elapsed
        try:
            api.get(name=name, namespace=namespace)
        except dynamic.exceptions.NotFoundError:
            return n, elapsed - 1
    raise Exception("timeout waiting for %s [%s] in namespace [%s] (checks=%d elapsed=%ds)" % (kind, name, namespace, n, elapsed - 1))


def delete_object(kind, api, name, namespace, timeout):
    api.delete(name=name, namespace=namespace)
    n, elapsed = wait_until_deleted(kind, api, name, namespace, timeout)
    print("deleted %s [%s] from namespace [%s] (checks=%d elapsed=%ds)" % (kind, name, namespace, n, elapsed))


def wait_for_mayastor_node(name, state):
    version = "openebs.io/v1alpha1"
    kind = "MayastorNode"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "mayastor"
    n, elapsed = wait_for(kind, api, name, namespace, 300, None, lambda status: status == state)
    print("%s [%s] in namespace [%s] is %s (checks=%d elapsed=%ds)" % (kind, name, namespace, state, n, elapsed))


def wait_for_mayastor_pool(name, state):
    version = "openebs.io/v1alpha1"
    kind = "MayastorPool"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "mayastor"
    n, elapsed = wait_for(kind, api, name, namespace, 300, None, lambda status: status.get('state') == state)
    print("%s [%s] in namespace [%s] is %s (checks=%d elapsed=%ds)" % (kind, name, namespace, state, n, elapsed))


def wait_until_persistent_volume_deleted(name):
    version = "v1"
    kind = "PersistentVolume"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"
    n, elapsed = wait_until_deleted(kind, api, name, namespace, 600)
    print("%s [%s] has been deleted from namespace [%s] (checks=%d elapsed=%ds)" % (kind, name, namespace, n, elapsed))


def create_mayastor_pool(name, node, disk):
    version = "openebs.io/v1alpha1"
    kind = "MayastorPool"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "mayastor"
    body = {
        "apiVersion": "openebs.io/v1alpha1",
        "kind": "MayastorPool",
        "metadata": {
            "name": name,
            "namespace": namespace
        },
        "spec": {
            "node": node,
            "disks": [
                disk
            ]
        }
    }
    create_object(kind, api, body, name, namespace, 300, lambda status: status.get('state') == 'online')


def create_storage_class(name):
    version = "storage.k8s.io/v1"
    kind = "StorageClass"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"
    body = {
        "apiVersion": version,
        "kind": kind,
        "metadata": {
            "name": name
        },
        "parameters": {
            "repl": "1",
            "protocol": "nvmf",
            "ioTimeout": "30",
            "fsType": "xfs",
            "local": "false"
        },
        "provisioner": "io.openebs.csi-mayastor",
        "volumeBindingMode": "WaitForFirstConsumer"
    }
    api.create(body=body, namespace=namespace)
    print("created %s [%s] in namespace [%s]" % (kind, name, namespace))


def create_persistent_volume_claim(name, sc_name):
    version = "v1"
    kind = "PersistentVolumeClaim"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"
    body = {
        "apiVersion": version,
        "kind": kind,
        "metadata": {
            "name": name
        },
        "spec": {
            "accessModes": [
                "ReadWriteOnce"
            ],
            "resources": {
                "requests": {
                    "storage": "100Mi"
                }
            },
            "storageClassName": sc_name
        }
    }
    create_object(kind, api, body, name, namespace, 300, lambda status: status.get('phase') in ['Pending', 'Bound'])


def get_persistent_volume_claim(name):
    version = "v1"
    kind = "PersistentVolumeClaim"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"
    return api.get(name=name, namespace=namespace)


def get_persistent_volume_name(pvc_name):
    pvc = get_persistent_volume_claim(pvc_name)
    return pvc.get('spec').get('volumeName')


def create_fio_pod(name, pvc_name):
    version = "v1"
    kind = "Pod"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"
    body = {
        "apiVersion": version,
        "kind": kind,
        "metadata": {
            "name": name
        },
        "spec": {
            "nodeSelector": {
                "openebs.io/workloads": "yes"
            },
            "volumes": [
                {
                    "name": "ms-volume",
                    "persistentVolumeClaim": {
                        "claimName": pvc_name
                    }
                }
            ],
            "containers": [
                {
                    "name": "fio",
                    "image": "mayadata/fio",
                    "args": [
                        "fio",
                        "--name=benchtest",
                        "--size=64m",
                        "--filename=/volume/test",
                        "--direct=1",
                        "--rw=randrw",
                        "--ioengine=libaio",
                        "--bs=4k",
                        "--iodepth=16",
                        "--numjobs=8",
                        "--time_based",
                        "--runtime=600"
                    ],
                    "volumeMounts": [
                        {
                            "mountPath": "/volume",
                            "name": "ms-volume"
                        }
                    ]
                }
            ],
            "restartPolicy": "Never"
        }
    }
    create_object(kind, api, body, name, namespace, 300, lambda status: status.get('phase') == 'Running')


def delete_failed_pod(name):
    version = "v1"
    kind = "Pod"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"

    # wait until pod is in an error state
    n, elapsed = wait_for(kind, api, name, namespace, 600, None, lambda status: status.get('phase') == 'Failed')
    print("%s [%s] in namespace [%s] has failed (checks=%d elapsed=%ds)" % (kind, name, namespace, n, elapsed))

    # delete the pod
    delete_object(kind, api, name, namespace, 600)


def delete_persistent_volume_claim(name):
    version = "v1"
    kind = "PersistentVolumeClaim"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"
    delete_object(kind, api, name, namespace, 300)


def delete_storage_class(name):
    version = "storage.k8s.io/v1"
    kind = "StorageClass"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "default"
    delete_object(kind, api, name, namespace, 300)


def delete_mayastor_pool(name):
    version = "openebs.io/v1alpha1"
    kind = "MayastorPool"
    api = client.resources.get(api_version=version, kind=kind)
    namespace = "mayastor"
    delete_object(kind, api, name, namespace, 300)


# perform initial setup
create_mayastor_pool('ksnode-pool-2', 'ksnode-2', 'malloc:///m0?size_mb=512')
create_storage_class('mayastor-1')
create_persistent_volume_claim('ms-volume-claim', 'mayastor-1')

# create the fio pod
print("creating Pod [fio] ...")
create_fio_pod('fio', 'ms-volume-claim')

# get the name of the PV associated with the PVC
persistent_volume_name = get_persistent_volume_name('ms-volume-claim')
print("PersistentVolumeClaim [%s] has associated PersistentVolume [%s]" % ('ms-volume-claim', persistent_volume_name))

# let fio run for a bit
print("sleeping for 15s ...")
time.sleep(15)

# pull the plug on the node VM
print("destroying Node [ksnode-2]")
with libvirt.open('qemu:///system') as connection:
    connection.lookupByName('ksnode-2').destroy()

# wait for the fio pod to fail and then delete it
print("polling Pod [fio] ...")
delete_failed_pod('fio')

# delete the PVC
delete_persistent_volume_claim('ms-volume-claim')

# wait for the associated PV to be deleted
print("polling PersistentVolume [%s] ..." % persistent_volume_name)
wait_until_persistent_volume_deleted(persistent_volume_name)

# restart the node VM
print("restarting Node [ksnode-2]")
with libvirt.open('qemu:///system') as connection:
    connection.lookupByName('ksnode-2').create()

# wait until node is back online
print("polling MayastorNode [ksnode-2] ...")
wait_for_mayastor_node('ksnode-2', 'online')

# wait until pool is back online
wait_for_mayastor_pool('ksnode-pool-2', 'online')

# perform remaining cleanup
delete_storage_class('mayastor-1')
delete_mayastor_pool('ksnode-pool-2')
