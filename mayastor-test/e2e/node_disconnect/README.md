## Note
The tests in this folder are not currently deployable by the CI system
as the test assumes a vagrant installation

## Pre-requisites for these tests

* A Kubernetes cluster with at least 3 nodes, with mayastor installed.
* The re-assignment test requires at least 4 nodes
* The cluster is deployed using vagrant and KUBESPRAY_REPO is correctly
  defined in ./lib/io_connect_node.sh
