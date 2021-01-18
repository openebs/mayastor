# CSI E2E Tests for Mayastor
These tests have been ported from kubernetes CSI NFS driver at https://github.com/kubernetes-csi/csi-driver-nfs

## Prerequisites
`golang` must be installed on the system running the tests

## Environment variables
* `MAYASTOR_CSI_DRIVER` - Override for driver name, defaults to `io.openebs.csi-mayastor`
* `SMALL_CLAIM_SIZE` - Size of small PVCs created by the testsuite, defaults to `50Mi`
* `LARGE_CLAIM_SIZE` - Size of large PVCs created by the testsuite, defaults to `500Mi`

## Changes for mayastor
* Location of the test directory within the repo is `test/e2e/csi`
* Naming from `csi-nfs` to `csi-mayastor`
* Claim sizes have been downsized from
  * `10Gi` to `50Mi`
  * `100Gi` to `500Mi`
* Claim sizes have been made configurable through environment variables.

## Running the testsuite
Kubernetes config from `$HOME/.kube/config` is used.

To run the tests execute `runtests.sh` from this directory.

## Storage requirements
`6 * LARGE_CLAIM_SIZE`

## List of dynamic provisioning tests
*  should create a volume on demand with mount options
*  should create multiple PV objects, bind to PVCs and attach all to different pods on the same node
*  should create a volume on demand and mount it as readOnly in a pod
*  should create a deployment object, write to and read from it, delete the pod and write to and read from it again
*  should delete PV with reclaimPolicy "Delete"
*  should retain PV with reclaimPolicy "Retain"
*  should create a pod with multiple volumes

### TODO
Remove workaround for side effect of running this test, when CAS-566 is fixed.
In `test/e2e/csi/runtest.sh` all Mayastor Volumes are deleted after
the test run. Until CAS-566 is fixed this is required as this will have an
impact on tests run subsequently in particular the uninstall test.
