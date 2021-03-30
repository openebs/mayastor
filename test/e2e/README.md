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
terraform (in terraform/ in this repo). 

# Running the tests

If you'd like to run the tests as a whole (as they are run in our CI/CD
pipeline) then use the script `../../scripts/e2e-test.sh`.

To run particular test cd to the directory with tests and type `go test`.
Most of the tests assume that mayastor is already installed. `install` test
can be run to do that.

# Configuration
The e2e test suite support runtime configuration to set parameters and variables,
to suit the cluster under test. The configuration file can be specified using the `--config`
option of `../../scripts/e2e-test.sh`

The go package used to read (and write) configuration files supports the following formats
1. `yaml`
2. `json`
3. `toml`

The mayastor e2e project uses `yaml` exclusively.
If a configuration is not specified the configuration is defaulted to the `CI/CD` configuration file.

The contents of the configuration are described in the file
`./common/e2e_config/e2e_config.go` and is subject to change.

The location of the default configuration file is *ALWAYS* defined relative to the root of the `e2e` directory as
`/configurations` and the name of file is `ci_e2e_config.yaml`

Other configuration files may be located in the same directory.

The test scripts use the following algorithm to locate and load configuration files
1. If environment variable `e2e_config_file` is not defined, environment variable `e2e_root_dir` *MUST* be defined. An attempt to load the default configuration as described earlier is made.
2. Otherwise
 * If the value of `e2e_config_file` is a path to a file, an attempt is made to load that file.
 * Otherwise the value is considered to be a name of a file in the configuration directory and an attempt is made to load the configuration accordingly.

The test scripts will fail (panic) if
1. Both `e2e_config_file` and `e2e_root_dir` environment variables are not defined.
2. The specified configuration file does not exist
3. The contents of specified configuration are invalid

Once the configuration has been loaded and all fields resolved, the contents are written out to the artefacts directory.

This file can be used to replicate a configuration for future test runs.
