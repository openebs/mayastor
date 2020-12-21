# Running the install test

Environment variables
* `e2e_image_tag`
  * Docker image tag used for mayastor images (the default is "ci")
* `e2e_docker_registry`
  * The IP address:port of the registry to be used.
  * If unspecified then the assumption is that test registry has been deployed in the cluster on port 30291, a suitable IP address is selected.
* `e2e_pool_yaml_files`
  * The list of yaml files defining pools for the cluster, comma separated, absolute paths.
* `e2e_pool_device`
  * This environment variable is used if `e2e_pool_yaml_files` is undefined.
  * pools are created for each node running mayastor, using the template file and the specified pool device.

```sh
e2e_image_tag="ci" e2e_docker_registry='192.168.122.1:5000' e2e_pool_device='/dev/nvme1n1' go test
```
