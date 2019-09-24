## mctl

Please refer to [this](/doc/mcli.md)

## Dumb

Dumb is a primitive tool for calling jsonrpc methods over unix
domain socket from command line. Method parameters are specified
as raw json on command line. It's main use case is for troubleshooting
mayastor problems as we can call mayastor json-rpc methods directly
without mayastor-client server and k8s in the middle.

Information for SPDK insiders: The tool is equivalent of SPDK's rpc.py
script. Though it is much more flexible at the cost of user friendliness
as it allows you to pass arbitrary json payload and call arbitrary json
method as opposed to rpc.py.

### Examples

1. Create a storage pool "ahoy":

```bash
dumb create_or_import_pool '{"name": "ahoy", "disks": ["/dev/sdb"]}'
```
```json
null
```

2. Create a replica exported using "nvme over tcp" protocol:

```bash
dumb create_replica '{"uuid": "00112233-4455-6677-8899-aabbccddeeff", "pool": "ahoy", "thin_provision": false, "size": 67108864, "share": "Nvmf"}
```
```json
null
```

3. List replicas:

```bash
dumb list_replicas | jq
```
```json
[
  {
    "pool": "ahoy",
    "share": "Nvmf",
    "size": 67108864,
    "thin_provision": false,
    "uuid": "00112233-4455-6677-8899-aabbccddeeff"
  }
]
```
