## Note
The tests in directories replica_disconnect and replica_reassign
are not currently deployable by the CI system
as those tests assume a vagrant installation.

## Pre-requisites for replica_pod_remove

* A Kubernetes cluster with 3 nodes, with mayastor installed.

## Pre-requisites for the other directories

* A Kubernetes cluster with at least 3 nodes, with mayastor installed.
* The replica_reassign test requires at least 4 nodes.
* The cluster is deployed using vagrant and KUBESPRAY_REPO is correctly
  defined in ``` ./lib/io_connect_node.sh ```

## Overview
The tests verify the behaviour of the cluster under fault conditions
affecting the availability of resources in the cluster, for example
a missing mayastor pod or a disconnected node.

To run, cd to the test directory then:
```bash
go test
```
