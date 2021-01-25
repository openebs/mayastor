# Pre-requisites

The test doesn't yet manage the lifecycle of the cluster being tested,
therefore the test hosts' kubeconfig must point to a Kubernetes cluster.
You can verify that the kubeconfig is setup correctly simply with
`kubectl get nodes`.

The cluster under test must meet the following requirements:
* Have 3 nodes (4 nodes if running reassignment test)
* Each node must be configured per the quick start:
  * At least 1GiB of hugepages available (e.g. 512 2MiB hugepages)
  * Each node must be labelled for use by mayastor (ie "openebs.io/engine=mayastor")

The test host must have the following installed:
* go (>= v1.15)
* kubectl (tested with v1.18)
* helm

# Setting up the cluster

### Create a cluster in public cloud

Use terraform script in `aws-kubeadm/` from
[terraform repo](https://github.com/mayadata-io/mayastor-terraform-playground) to
set up a cluster in AWS suitable for running the tests. The configuration file for
terraform could look like this (replace docker registry value by yours - must
be reachable from the cluster):

```
cluster_name    = "<NICKNAME>-e2e-test-cluster"
deploy_mayastor = false
num_workers     = 3
ebs_volume_size = 5
mayastor_use_develop_images = true
aws_instance_root_size_gb   = 10
docker_insecure_registry    = "52.58.174.24:5000"
```

### Create local cluster

Many possibilities here. You could use libvirt based cluster created by
terraform (in terraform/ in this repo). Or you could use the script from
`setup` subdirectory using vagrant and kubespray.

# Running the tests

If you'd like to run the tests as a whole (as they are run in our CI/CD
pipeline) then use the script `./scripts/e2e-test.sh`.

To run particular test cd to the directory with tests and type `go test`.
Most of the tests assume that mayastor is already installed. `install` test
can be run to do that.
