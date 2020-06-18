# Quickstart

This quickstart guide has been tested against the following platforms and configurations:

- kubeadm (vanilla k8s cluster)
    - k8s version 1.14 or newer

### Requirements

#### General

* 2 x86-64 CPU cores with SSE4.2 instruction support:
  * Intel Nehalem processor (march=nehalem) and newer
  * AMD Bulldozer processor and newer
* 4GB memory
* Mayastor DaemonSet (MDS) requires:
  * Privileged mode
  * 2MB hugepages support
* Where using iSCSI see [Prerequisites (iSCSI client)](https://docs.openebs.io/docs/next/prerequisites.html)

 #### On Microsoft AKS
* It is not necessary to implement the iSCSI prerequisites guide as directed above, since the worker node images provided by Microsoft are already suitably configured as provisioned
* Worker nodes which are to be designated Mayastor "Storage Nodes" may benefit from being scaled to greater than 2 vCPUs.  The Mayastor pod which runs on each such node requires exclusive use of *at least* 1 vCPU.  Therefore these nodes should be scaled to provide sufficient remaining CPU resource for other workloads which may be required to be scheduled on them

### Quickstart

#### Prepare "Storage Nodes"

Within the context of the Mayastor project, a "Storage Node" is a Kubernetes worker node which is capable of hosting a Storage Pool.  By extension, a Storage Node runs an instance of Mayastor as a pod and uses a 'physical' block storage device on that node to contribute Mayastor-managed storage capacity to Persistent Volumes provisioned on the parent cluster.

A worker node which will not host/contribute storage capacity to Mayastor does not need to be a Storage Node (although Storage Node 'group' membership can be expanded at any time).  Such a worker node can still mount Mayastor Persistent Volumes for containers scheduled on it - it does not need to be a Storage Node to be able to support this basic cluster functionalty.

1. 2MB Huge Pages must be supported and enabled on a storage node.  A minimum number of 512 such pages must be available on each node.

    Verify huge page availability using:
    ```
    grep HugePages /proc/meminfo

    AnonHugePages:         0 kB
    ShmemHugePages:        0 kB
    HugePages_Total:    1024
    HugePages_Free:      671
    HugePages_Rsvd:        0
    HugePages_Surp:        0

    ```

    If fewer than 512 pages are available, the page count should be configured as necessary, accounting for any other co-resident workloads which may require them. e.g.

    ```bash
    echo 512 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    ```

    This change can be made persistent across reboots by adding the required value to `/etc/sysctl.conf`, thus:
    ```
    vm.nr_hugepages = 512
    ```

    > If you modify the huge page configuration of a node, you *must* restart the kubelet or reboot the node


2.  Load the Network Block Device (NBD) kernel module.  This is necessary *only* if it is intended to use nbd transport for volume provisioning.

    ```
    modprobe {nbd}
     ```
    To make this change persistent across reboots add the line
    `nbd` to `/etc/modules-load.d/modules.conf`


3.  Label the storage nodes (here we demonstrate labeling of the node named "node1") :
    ```bash
    kubectl label node node1 openebs.io/engine=mayastor
    ```

#### Deploy Mayastor on the Cluster

The YAML files named below are to be found in the `deploy` folder of the Mayastor repository

4.  Create the Mayastor namespace:
    ```bash
    kubectl create -f namespace.yaml
    ```

5.  Deploy the Mayastor and CSI components
    ```bash
    kubectl create -f nats-deployment.yaml
    kubectl create -f mayastorpoolcrd.yaml
    kubectl create -f moac-deployment.yaml
    kubectl create -f mayastor-daemonset.yaml
    ```

6.  Confirm that the MOAC and NATS pods are running:
    ```bash
    kubectl -n mayastor get pod
    ```
    ```
    NAME                   READY   STATUS    RESTARTS   AGE
    nats-5fc4d79d66-lvdcg  1/1     Running   0          31s
    moac-5f7cb764d-sshvz   3/3     Running   0          34s
    ```

7. Confirm that the Mayastor daemonset is fully deployed:
    ```bash
    kubectl -n mayastor get daemonset
    ```
    ```
    NAME       DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE
    mayastor   3         3         3       3            3
    ```
    (in the above example, the point to note is that the READY and AVAILABLE counts equal the DESIRED count)

    ```bash
    kubectl -n mayastor get msn
    ```
    ```
    NAME       STATE     AGE
    node-1     online    112s
    node-2     online    112s
    node-3     online    112s
    ```

8.  Create a Storage Pool(s) for volume provisioning.  Each Storage Node typically hosts a Storage Pool, although a single pool is satisfactory for testing purposes.  (In the following example, replace `disk` and `node` with the appropriate values for your own configuration):
    ```bash
    cat <<EOF | kubectl create -f -
    apiVersion: "openebs.io/v1alpha1"
    kind: MayastorPool
    metadata:
      name: pool-on-node-1
      namespace: mayastor
    spec:
      node: workernode1
      disk: ["/dev/vdb"]
    EOF
    ```
    > Note: Currently, the membership of Mayastor Storage Pools is restricted to a single disk device

    Verify that the Storage Pool(s) has/have been created (note that the value of `State` *must be* `online`):
    ```bash
    kubectl -n mayastor describe msp pool-on-node-1
    ```
    ```
    Name:         pool-on-node-1
    Namespace:    mayastor
    Labels:       <none>
    Annotations:  <none>
    API Version:  openebs.io/v1alpha1
    Kind:         MayastorPool
    Metadata:
      Creation Timestamp:  2019-04-09T21:41:47Z
      Generation:          1
      Resource Version:    1281064
      Self Link:           /apis/openebs.io/v1alpha1/mayastorpools/pool-on-node-1
      UID:                 46aa02bf-5b10-11e9-9825-589cfc0d76a7
    Spec:
      Disks:
        /dev/vdb
      Node:  workernode1
    Status:
      Capacity:  10724835328
      Reason:
      State:     ONLINE
      Used:      0
    Events:      <none>
    ```

#### Testing the Deployment

9.  Create Storage Classes which use the Mayastor CSI plugin as their basis for volume provisioning:

    Currently Mayastor-provisioned Persistent Volumes can made available over iSCSI or NBD, where iSCSI is strongly encouraged as it gives significantly better performance

  * iSCSI
    ```bash
    cat <<EOF | kubectl create -f -
    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
      name: mayastor-iscsi
    parameters:
      repl: '1'
      protocol: 'iscsi'
    provisioner: io.openebs.csi-mayastor
    EOF
    ```
  * NBD (if required)
    ```bash
    cat <<EOF | kubectl create -f -
    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
      name: mayastor-nbd
    parameters:
      repl: '1'
      protocol: 'nbd'
    provisioner: io.openebs.csi-mayastor
    EOF
    ```


10. Creating a Persistent Volume Claim (PVC):
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
      storageClassName: mayastor-iscsi
    EOF
    ```

    Note: Change the value of `storageClassName` as appropriate to use the transport required (nbd, or iSCSI).

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

11. Check that the volume resource has been created and its internal status is `online`:
    ```bash
    kubectl -n mayastor get msv 21d56e09-5b78-11e9-905a-589cfc0d76a7
    ```
    ```
    Name:         21d56e09-5b78-11e9-905a-589cfc0d76a7
    Namespace:    mayastor
    Spec:
      Limit Bytes:  0
      Preferred Nodes:
      Replica Count:   2
      Required Bytes:  1073741824
      Required Nodes:
    Status:
      Node:    node1
      Reason:
      Replicas:
        Node:  node1
        Pool:  pool
        Uri:   bdev:///21d56e09-5b78-11e9-905a-589cfc0d76a7
      Size:    1073741824
      State:   online
    ```

12. Deploy a pod which will mount the volume and which contains the fio test tool:
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

13. Run fio on the volume for 60s and verify that io is handled as expected and without errors:
    ```bash
    kubectl exec -it fio -- fio --name=benchtest --size=800m --filename=/volume/test --direct=1 --rw=randrw --ioengine=libaio --bs=4k --iodepth=16 --numjobs=1 --time_based --runtime=60
    ```

### Known issues and limitations

* The Mayastor service suddenly restarts when mounting a PVC, with exit code `132`

    This is due to a SIGILL, which means the container has not been compiled properly from our CI system for your CPU
    architecture. As a result we ask the CPU something that it does not know how to do.

* Missing finalizers

    Finalizers have not been implemented yet, therefore it is important to follow
    the proper tear down order:

     - delete the pods using a mayastor PVC
     - delete the PVC
     - delete the MSP
     - delete the DaemonSet

 * Snapshot and clones currently not exposed

### Tips
* To deploy on RKE + Fedora CoreOS
    * You will need to add following directory mapping to `services_kubelet->extra_binds` in your `cluster.yml`:
     `/opt/rke/var/lib/kubelet/plugins:/var/lib/kubelet/plugins`. Otherwise the CSI socket paths won't match and the CSI
     driver registration process will fail.

### Monitoring with Grafana

If you want to set up monitoring for Mayastor which currently shows just two
graphs with IOPS and bandwidth for specified replica, then follow the tutorial
in the monitoring [README file](../deploy/monitor/README.md).
