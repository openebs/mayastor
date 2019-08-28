## mctl

Please refer to [this](/doc/mcli.md)

## Dumb

Dumb is a primitive tool for calling jsonrpc methods over unix
domain socket from command line. Method parameters are specified
as raw json on command line. It's main use case is for troubleshooting
mayastor problems as we can call mayastor json-rpc methods directly
without mayastor-client server and k8s in the middle.

Example of calling a method without parameters:
```bash
dumb -s /tmp/sock get_bdevs | jq
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
    "claimed": false,
    "driver_specific": {
      "aio": {
        "filename": "/dev/sdb"
      }
    },
    "name": "/dev/sdb",
    "num_blocks": 2097152,
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
    }
  }
]
```

Example of calling a method with json parameters:
```bash
dumb -s /tmp/sock create_or_import_pool '{"name": "ahoj", "disks": ["/dev/sdb"]}'
```
```json
null
```
