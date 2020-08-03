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
[
  {
    "aliases": [],
    "assigned_rate_limits": {
      "r_mbytes_per_sec": 0,
      "rw_ios_per_sec": 0,
      "rw_mbytes_per_sec": 0,
      "w_mbytes_per_sec": 0
    },
    "block_size": 512,
    "claimed": true,
    "driver_specific": {
      "aio": {
        "filename": "/dev/ram1"
      }
    },
    "name": "/dev/ram1",
    "num_blocks": 2048000,
    "product_name": "AIO disk",
    "supported_io_types": {
      "flush": true,
      "nvme_admin": false,
      "nvme_io": false,
      "read": true,
      "reset": true,
      "unmap": false,
      "write": true,
      "write_zeroes": true
    },
    "uuid": "4f6b7449-886a-4045-96a5-a5626a598302",
    "zoned": false
  }
]
```