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

```sh
cd Mayastor/e2e/install
go test
```