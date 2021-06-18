# MOAC

MOAC is a control plane of MayaStor. It is a NodeJS application written in
TypeScript. The primary interface to k8s is CSI spec (gRPC). The other way of
interacting with k8s is k8s REST API server. Besides implementing the CSI
server it implements following k8s operators:

- _node operator_: keeps track of MayaStor instances in the cluster.
- [pool operator](./doc/pool-operator.md): creates/updates/deletes storage pools on storage nodes as requested by admin by means of msp custom resources.
- [volume operator](./doc/volume-operator.md): informs user about existing volumes on storage nodes by means of msv custom resources and allows simple modifications to them.

## Requirements

- K8s version is 1.20 or later (older versions might or might not work)
- NodeJS v16 (see below)

### Nix

Enter the nix shell and that will pull NodeJS v16 for you:

```bash
nix-shell
```

### Ubuntu

NodeJS v16 may not be available in default package repository on Ubuntu
depending on Ubuntu release. If that's the case, new package source has to be
added for NodeJS:

```bash
curl -sL https://deb.nodesource.com/setup_16.x -o nodesource_setup.sh
sudo bash nodesource_setup.sh
sudo apt install nodejs
```

## Build it

Following commands will:
* download and installs npm dependencies of moac
* download proto files from mayastor repo
* compile TypeScript code to JS

```bash
npm install
npm run compile
```

## Test it

Note that etcd and nats are expected to be available in order to run the unit
tests. The most easy way is to get those by entering nix-shell and run the tests
from nix-shell.

```bash
npm test
```

## Run it

moac relies on following components to be available when running:

* k8s API server
* NATS message passing server
* etcd database in order to consult state of a nexus

### Inside k8s cluster

It is the most straightforward way to run moac. However also the least
convenient for debugging purposes.
See [Mayastor gitbook](https://mayastor.gitbook.io/introduction/) on how to
deploy moac to k8s cluster.  This assumes that you are either fine with using
the official docker image of MOAC or that you run your own private registry and
you modified the deployment yaml file to use the private image instead.

### Outside k8s cluster

You can run MOAC without a K8s cluster with all components that are K8s
specific disabled, though it does not do much because there is neither CSI
nor operators:

```bash
./moac --skip-k8s
```

## Contributing

1. Check your JS code style (sorry, no TS yet): `npm run check`
2. Fix style errors that can be fixed: `npm run fix`
3. Check that TS files compile: `npm run compile`
4. All unit tests must pass: `npm run test`
5. Non-trivial changes should be tested on real k8s cluster.
6. Commit message is expected to be in following form (replace type by `fix`, `feat`, `chore` or other allowed type):
```
type: short description

Long multiline description if needed.
```

## Architecture (slightly obsolete)

Unfortunately ASCII art is not good with colours. Left side of the picture
is reserved for k8s cluster components. Up and right are MOAC components.
Lines denote relations between all components.

```text
                      moac
                     +------------------------------------------------------+
 +------------+      |  +-----------+   +---------+                         |
 | K8S CSI    +------|-->   CSI     +--->         |                         |
 +------------+      |  | controller|   |         |  +--------+             |
                     |  +-----------+   | volumes +--+ volume |             |
 +------------+      |  +-----------+   |         |  +--------+             |
 |            |      |  |   volume  +--->         |                         |
 |            +------|-->  operator |   +---+-----+  +--------+             |
 |    K8S     |      |  +-----------+       |        |  REST  |             |
 | api-server |      |  +-----------+   +---+-----+--+  API   |             |
 |            +------|-->   pool    |   |         |  +--------+             |
 |            |      |  |  operator +---+         |                         |
 |            |      |  +-----------+   |registry |  +------+   +-------+   |
 |            |      |  +-----------+   |         +--+      +---+ nexus |   |
 |            +------|-->   node    +---+         |  | node |   +-------+   |
 |            |      |  |  operator |   |         |  |      |   +------+    |
 +-----+------+      |  +-----------+   +---+-----+  +--+---+---+ pool |    |
       |             |                      |           |       +---+--+    |
       |             |                  +---+-----+     |           |       |
       |             |                  | message |     |       +---+-----+ |
       |             |              +---+   bus   |     |       | replica | |
       |             |              |   +---------+     |       +---------+ |
       |             +--------------|-------------------|-------------------+
       |                            |                   |
       |                            |                   |
       |                            |                   |
   App | Node                 Node  |                   | Storage Node
+------+-----+---+            +-----+---+            +----------------------+
|            |   |            |   NATS  |            |      mayastor        |
|  kubelet   |   |            |  server +------------+                      |
|            |   |            +---------+            +-+--------------------+
+------+-----+   |                                     |
|      |         |                                     |
+------+-----+   |                                     |
|  mayastor  |   |                volume mount         |
| CSI plugin +-+-|-------------------------------------+
+------------+ | |
|              | |
+------------+ | |
|    App     +-+ |
+------------+   |
|                |
+----------------+
```

## Volume states

Volume life cycle can be described by a finite state automaton (FSA). To
understand it read the code in `_fsa()` method in `volume.ts`.

## Troubleshooting

Running moac with trace log level enabled (`-vv`) prints all details about
incoming/outgoing CSI messages, watcher events, etc.

## History

The acronym MOAC comes from "Mother Of All Cases" (CAS means Container Attached
Storage).

## VSCode

VSCode is a perfect choice for developing JS/TS projects. Remote docker plugin
can be used to setup a dev environment for moac in a moment. Example of
`.devcontainer.json` file:

```json
{
    "image": "node:16",
    "workspaceMount": "source=/path/to/repo/on/the/host/Mayastor,target=/workspace,type=bind,consistency=cached",
    "workspaceFolder": "/workspace",
    "extensions": [
        "chenxsan.vscode-standardjs"
    ]
}
```
