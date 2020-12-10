# Pre-requisites

The test doesn't yet manage the lifecycle of the cluster being tested,
therefore the test hosts' kubeconfig must point to a Kubernetes cluster.
You can verify that the kubeconfig is setup correctly simply with
`kubectl get nodes`.

The cluster under test must meet the following requirements:
* Have 3 nodes
* Each node must be configured per the quick start:
  * At least 512 2MiB hugepages available
  * Each node must be labelled for use by mayastor (ie "openebs.io/engine=mayastor")

The test host must have the following installed:
* go (>= v1.15)
* ginkgo (tested with v1.2)
* kubectl (tested with v1.18)

# Running the tests
Environment variables
* e2e_docker_registry
  * The IP address:port of the registry to be used.
  * If unspecified then the assumption is that test registry has been deployed in the cluster on port 30291, a suitable IP address is selected.
* e2e_pool_yaml_files
  * The list of yaml files defining pools for the cluster, comma separated, absolute paths.
* e2e_pool_device
  * This environment variable is used if `e2e_pool_yaml_files` is undefined.
  * pools are created for each node running mayastor, using the template file and the specified pool device.
```sh
cd Mayastor/e2e/install
go test
```
Or
```sh
cd Maystor/e2e/install
e2e_docker_registry='192.168.122.1:5000' e2e_pool_device='/dev/nvme1n1' go test
```