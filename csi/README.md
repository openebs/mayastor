## About

This crate contains CSI as well as a Mayastor specific API services
It is an asynchronous server implementation making use of tokio.rs.

```
   grpc                    json-rpc
 requests  --------------  requests   -------------
 --------> | grpc proxy | ----------> |  mayastor |
 <-------- |            | <---------- |           |
   grpc    --------------  json-rpc   -------------
 replies                    replies
```

The mayastor-agent implements two gRPC services:

* mayastor service (Any RPC methods other then CSI ones specific ones)
* CSI services (CSI specific RPC methods)

See [grpc proto file](../rpc/proto/mayastor.proto) for the details of the gRPC
interface.

## Building and running the gRPC server

After `cargo build` the binary will be in `target/debug` directory.
To get a debug output just from the server and not other libraries (i.e.
tokio, tower-grpc, etc.), run it as follows:

```bash
RUST_LOG=mayastor_grpc=trace ./target/debug/mayastor-agent
```

# Client

Although that a client for gRPC server is not required for the product,
it is important for testing and troubleshooting. The client
allows you to manage storage pools and replicas and just use `--help`
option if not sure how to use it. CSI services are not covered by the client.

In following example of a client session is assumed that the proxy *and*
mayastor have been started and are running:

```
$ dd if=/dev/zero of=/tmp/disk bs=1024 count=102400
102400+0 records in
102400+0 records out
104857600 bytes (105 MB, 100 MiB) copied, 0.235195 s, 446 MB/s
$ sudo losetup /dev/loop8 /tmp/disk
$ ./mayastor-client pool create tpool /dev/loop8
$ ./mayastor-client pool list
NAME                 STATE        CAPACITY         USED   DISKS
tpool                0            96.0 MiB          0 B   tpool
$ ./mayastor-client replica create tpool replica1 --size=10
$ ./mayastor-client replica create tpool replica2 --size=1000 --thin
$ ./mayastor-client replica list
POOL                 NAME                 THIN           SIZE
tpool                replica1             false       10.0 MiB
tpool                replica2             true         1.0 GiB
$ ./mayastor-client replica destroy tpool replica1
$ ./mayastor-client replica destroy tpool replica2
$ ./mayastor-client replica list
No replicas have been created
$ ./mayastor-client pool destroy tpool
```

# CSI

CSI methods can be tested by official csc tool written in golang. Assuming that golang
is installed and GOPATH set, csc tool can be installed as:

```bash
go get github.com/rexray/gocsi && make -C $GOPATH/src/github.com/rexray/gocsi csi-sp
```

Now assuming that mayastor-client server is running and for example we want to
invoke a probe method from csi identity service, we can type:

```bash
$GOPATH/src/github.com/rexray/gocsi/csc/csc -i -e unix:///var/tmp/csi.sock identity probe
```

# Docker image

For local testing it is often useful to build own mayastor-client image.
First you need to build mayastor-client. And after that from top level directory:

```bash
docker build -t mayadata/mayastor-client:latest -f mayastor-client/Dockerfile .
```

mayastor-client container alone is not very useful. It needs to work together
with mayastor container. The containers need to share a volume with unix domain
socket file so that they can talk to each other.

```bash
docker images
...
docker run -d --privileged --shm-size=1g -v /dev:/dev -v mayastor_sock:/var/tmp mayadata/mayastor:latest
docker run -d -v mayastor:/var/tmp mayadata/mayastor-client:latest
```

Now assuming you want to try out gRPC API, exec `mayastor-client` command
from mayastor-client container:

```
docker ps
...
docker exec -it ${CONTAINER_ID} -- mayastor-client --help
```
