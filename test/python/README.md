# requirements

The key thing of the tests is that we want to run an "normal" workload. For this
we use VMs. The fixture `target_vm()` is used in some of the tests and then uses
`async_cmd_run_at()` to execute remote work.


# setup virtual env

Not all packages are available on nix, so one extra step is needed.

```shell
python -m grpc_tools.protoc -i `realpath ../../rpc/proto` --python_out=test/python --grpc_python_out=test/python mayastor.proto
virtualenv --no-setuptools test/python/venv
source test/python/venv/bin/activate
pip install -r test/python/requirements.txt
```

