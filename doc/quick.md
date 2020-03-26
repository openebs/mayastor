# Quickstart

This has steps have been tested on:

* kubeadm (vanilla k8s cluster),
* Google Kubernetes Engine (GKE) cluster using Ubuntu image
* Rancher
* RKE + Fedora CoreOS
    * You will need to add following directory mapping to `services_kubelet->extra_binds` in your `cluster.yml`:
     `/opt/rke/var/lib/kubelet/plugins:/var/lib/kubelet/plugins`. Otherwise the CSI socket paths won't match and the CSI
     driver registration process will fail.

### Requirements

 * Only works on Intel Core i7 processor architecture (march=corei7) and higher
 * MayaStor Daemonset (MDS) requires privileged mode
 * MDS requires 2MB hugepages
 * For testing, MDS requires the Network Block Device driver (NBD) and the XFS filesystem kernel module

## Quickstart

1.  Create namespace holding MayaStor resources:
    ```bash
    cd deploy
    kubectl create -f namespace.yaml
    ```

2.  Deploy MayaStor and the CSI components
    ```bash
    kubectl create -f moac-deployment.yaml
    kubectl create -f mayastor-daemonset.yaml
    ```
    Check that MOAC is running:
    ```bash
    kubectl -n mayastor get pod
    ```
    ```
    NAME                   READY   STATUS    RESTARTS   AGE
    moac-5f7cb764d-sshvz   3/3     Running   0          34s
    ```

3.  Prepare the storage nodes which you would like to use for volume
    provisioning. Each storage node needs at least 512MB of mem in hugepages:
    ```bash
    echo 512 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    ```
    If you want, you can make this change persistent across reboots by adding following line
    to `/etc/sysctl.conf`:
    ```
    vm.nr_hugepages = 512
    ```

    After adding the hugepages you *must* restart the kubelet. You can verify that
    hugepages haven been created using:

    ```
    cat /proc/meminfo | grep HugePages
    AnonHugePages:         0 kB
    ShmemHugePages:        0 kB
    HugePages_Total:    1024
    HugePages_Free:      671
    HugePages_Rsvd:        0
    HugePages_Surp:        0

    ```

    Load the NBD and XFS kernel modules which are needed for publishing
    volumes resp. mounting filesystems.
    ```
    modprobe {nbd,xfs}
     ```
    And, if you want, make this change persistent across reboots by adding lines with
    `NBD` and `xfs` to `/etc/modules-load.d/modules.conf`.

4.  Label the storage nodes (here we use node "node1"):
    ```bash
    kubectl label node node1 openebs.io/engine=mayastor
    ```
    Check that MayaStor has been started on the storage node:
    ```bash
    kubectl -n mayastor get pod
    ```
    ```
    NAME                   READY   STATUS    RESTARTS   AGE
    mayastor-gldv8         3/3     Running   0          81s
    moac-5f7cb764d-sshvz   3/3     Running   0          6h46m
    ```

5.  Create a storage pool for volume provisioning (replace `disks` and `node`
    values as appropriate):
    ```bash
    cat <<EOF | kubectl create -f -
    apiVersion: "openebs.io/v1alpha1"
    kind: MayastorPool
    metadata:
      name: pool
    spec:
      node: node1
      disks: ["/dev/vdb"]
    EOF
    ```
    Check that the pool has been created (Note that the `State` *must be* `online`):
    ```bash
    kubectl describe msp pool
    ```
    ```
    Name:         pool
    Namespace:
    Labels:       <none>
    Annotations:  <none>
    API Version:  openebs.io/v1alpha1
    Kind:         MayastorPool
    Metadata:
      Creation Timestamp:  2019-04-09T21:41:47Z
      Generation:          1
      Resource Version:    1281064
      Self Link:           /apis/openebs.io/v1alpha1/mayastorpools/pool
      UID:                 46aa02bf-5b10-11e9-9825-589cfc0d76a7
    Spec:
      Disks:
        /dev/vdb
      Node:  node1
    Status:
      Capacity:  10724835328
      Reason:
      State:     ONLINE
      Used:      0
    Events:      <none>
    ```

6.  Create a Storage Class using MayaStor CSI plugin for volume provisioning:
    ```bash
    cat <<EOF | kubectl create -f -
    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
      name: mayastor
    provisioner: io.openebs.csi-mayastor
    EOF
    ```

7.  Create a Persistent Volume Claim (PVC):
    ```bash
    cat <<EOF | kubectl create -f -
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: ms-volume-claim
    spec:
      accessModes:
      - ReadWriteOnce
      resources:
        requests:
          storage: 1Gi
      storageClassName: mayastor
    EOF
    ```
    Verify that the PVC and Persistent Volume (PV) for the PVC have been
    created:
    ```bash
    kubectl get pvc
    ```
    ```
    NAME              STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
    ms-volume-claim   Bound    pvc-21d56e09-5b78-11e9-905a-589cfc0d76a7   1Gi        RWO            mayastor       22s
    ```
    ```bash
    kubectl get pv
    ```
    ```
    NAME                                       CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                     STORAGECLASS   REASON   AGE
    pvc-21d56e09-5b78-11e9-905a-589cfc0d76a7   1Gi        RWO            Delete           Bound    default/ms-volume-claim   mayastor                27s
    ```

8.  Deploy a pod with fio tool which will be using the PV:
    ```bash
    cat <<EOF | kubectl create -f -
    kind: Pod
    apiVersion: v1
    metadata:
      name: fio
    spec:
      volumes:
        - name: ms-volume
          persistentVolumeClaim:
           claimName: ms-volume-claim
      containers:
        - name: fio
          image: dmonakhov/alpine-fio
          args:
            - sleep
            - "1000000"
          volumeMounts:
            - mountPath: "/volume"
              name: ms-volume
    EOF
    ```
    Check that it has been successfully deployed:
    ```
    kubectl get pod
    ```

9.  Run fio on the volume for 30s:
    ```bash
    kubectl exec -it fio -- fio --name=benchtest --size=800m --filename=/volume/test --direct=1 --rw=randrw --ioengine=libaio --bs=4k --iodepth=16 --numjobs=1 --time_based --runtime=60
    ```

### Know issues and limitations

* The MayaStor service suddenly restarts when mounting a PVC, with exit code `132`

    This is due to a SIGILL, which means the container has not been compiled properly from our CI system for your CPU
    architecture. As a result we ask the CPU something that it does not know how to do.

* Missing finalizers

    Finalizers have not been implemented yet, therefor, it is important to follow
    the proper order of tear down:

     - delete the pods using a mayastor PVC
     - delete the PVC
     - delete the MSP
     - delete the DaemonSet

 * Replication is not part of the container images, for this you need to [build](/doc/build.md) from source
 * snapshot and clones currently not exposed

### Grafana

If you want to set up monitoring for MayaStor which currently shows just two
graphs with IOPS and bandwidth for specified replica, then follow the tutorial
in monitoring [README file](../deploy/monitoring/README.md).
