# Pool Operator

Pool operator is responsible for storage pool creation and destruction on
storage nodes. It follows classic k8s custom resource operator model. Custom
resource (CR) managed by the operator is
[MayastorPool](../crd/mayastorpool.yaml). CR objects represent the
*desired state* by a user. Pool operator is responsible for making the desired
state real. *Storage admin* creates and destroys mayastorpool CRs using
`kubectl`. The operator issues create and destroy storage pool commands to
storage nodes to make the state real.

## MayastorPool

MayastorPool custom resource is structured as follows:

* metadata: Includes name of the pool which must be cluster-wide unique.
* spec: Pool parameters including:
  * node: Name of the k8s node where the pool should be created (mayastor needs to be enabled on that node by means of node labels).
  * disks: Disk names (absolute paths to device files in /dev) which comprise the storage pool.
* status:
  * state: State of the pool can be one of:
    * pending: Pool has not been created yet - either due to an error or because the k8s node does not have mayastor enabled.
    * online: Pool has been created and is healthy & usable.
    * degraded: Pool has been created and is usable but has some issue.
    * faulted: Pool has unrecoverable error and is not usable.
    * offline: Pool was created but currently it is not available because mayastor does not run on the k8s node.
  * node: cached value from spec to prevent confusing the operator by changing the value in spec
  * disks: cached value from spec to prevent confusing the operator by changing the value in spec
  * reason: If applicable it provides additional information explaining the state.

MayastorPool CR entries are shared between storage admin and pool operator
with following responsibilities for each stake holder:

* Storage admin:
  1. Is responsible for creation/destruction of the CRs.
  2. Modification of the CR is possible but should not be done.
  3. Status section of the CR is read-only
* Pool operator:
  1. Is responsible for updating status section of the CR.
  2. spec and metadata are read-only
  3. Never creates or destroys any of the MayastorPool CRs.

## List storage pools

```bash
kubectl -n mayastor get msp
```

## Create a storage pool

Example of creating a storage pool `my-new-pool` on disk device
`/dev/loop0` on k8s node `node1`:

```bash
cat <<EOF | kubectl create -f -
apiVersion: "openebs.io/v1alpha1"
kind: MayastorPool
metadata:
  name: my-new-pool
  namespace: mayastor
spec:
  node: node1
  disks: ["/dev/loop0"]
EOF
```

## Destroy storage pool

Example of destroying the storage pool `my-new-pool`:

```bash
kubectl -n mayastor delete msp my-new-pool
```
