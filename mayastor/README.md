TODO: As we improve the documentation this doc will become a more complete
description of mayastor program and associated libraries.

# jsonrpc

jsonrpc is a primitive tool for calling jsonrpc methods over unix
domain socket from command line. Method parameters are specified
as raw json on command line. It's main use case is for troubleshooting
mayastor problems as we can call spdk json-rpc methods directly.

Information for SPDK insiders: The tool is equivalent of SPDK's rpc.py
script. Though it is much more flexible at the cost of user friendliness
as it allows you to pass arbitrary json payload and call arbitrary json
method as opposed to rpc.py.

## Example

List SPDK bdevs:

```bash
jsonrpc raw get_bdevs '{}' | jq
```
```json
[]
```