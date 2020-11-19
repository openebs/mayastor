# MOAC

MOAC is a control plane of MayaStor. It is a NodeJS application written in
javascript/typescript and makes use of kubernetes-client library to interact
with K8s API server. In a nutshell it has following responsibilities:

- _node operator_: keeps track of nodes with running MayaStor instances.
- [pool operator](/doc/pool-operator.md): creates/updates/deletes storage pools on storage nodes as requested by admin by means of msp custom resources.
- [volume operator](/doc/volume-operator.md): informs user about existing volumes on storage nodes by means of msv custom resources and allows simple modifications to them.
- _CSI controller_: provisions volumes on storage nodes based on requests from k8s through CSI interface.

## Requirements

- required K8s version is 1.14 or newer
- NodeJS v12 (instructions below)
- Nix when building a docker image

### NodeJS on Nix(OS)

Enter a nix shell with NodeJS and python packages:

```bash
nix-shell -p nodejs-12_x python
```

### NodeJS on Ubuntu

NodeJS v12 may not be available in default package repository on Ubuntu
depending on Ubuntu release. If that's the case, new package source has to be
added for NodeJS:

```bash
curl -sL https://deb.nodesource.com/setup_12.x -o nodesource_setup.sh
sudo bash nodesource_setup.sh
sudo apt install nodejs
```

## Build it

Following command downloads and installs npm dependencies of moac and compiles
ts files using typescript compiler:

```bash
npm install
npm run compile
```

## Run it

### Inside k8s cluster

It is the most straightforward way to run moac. However also the least
convenient for debugging issues. Use
[k8s yaml file](/deploy/moac-deployment.yaml) for deploying MOAC to K8s cluster
in usual way. This assumes that you are either fine with using the official
docker image of MOAC or that you run your own private registry and you modified
the deployment yaml file to use the private image instead.

### Outside k8s cluster

You can run MOAC without any K8s cluster with all components that are K8s
specific disabled:

```bash
./index.js --skip-k8s
```

## Contributing

1. Check your code style: `npm run check`
2. Fix style errors that can be fixed: `npm run fix`
3. Check that TS files compile: `npm run compile`
4. All unit tests must pass: `npm run test`
5. Clean generated JS files (optional): `npm run clean`

## Updating the dependencies

Updating npm dependencies in `package-lock.json` is not enough. In order to
update dependencies in built docker images as well, the nix files need to be
updated too;

1. Update npm dependencies:

   ```bash
   npm update
   ```

   NOTE: If you want to update all packages to the very latest major versions
   that will likely include breaking API changes, then install
   `npm-check-updates` npm package and run `npm-check-updates -u` before the
   first step.

2. If not already installed, install a `node2nix` tool which automates nix
   package creation for npm packages. On NixOS that can be done by following
   command:

   ```bash
   nix-env -f '<nixpkgs>' -iA nodePackages.node2nix
   ```

3. Generate nix package build files. The flag `development` is needed for
   typescript to get included in the package, because it is in `devDependencies`
   (common case in JS world). The flag does not influence how things are built
   (debug info, code optimisations, etc.).
   ```bash
   rm -rf node_modules
   node2nix -l package-lock.json --development --nodejs-12 -c node-composition.nix
   ```

4. Patch generated nix files by following patch in order to reduce the size
   of the package. This is a temporary workaround for
   https://github.com/svanderburg/node2nix/issues/187.
   ```diff
   diff --git a/csi/moac/node-composition.nix b/csi/moac/node-composition.nix
   index ac8de82..6441534 100644
   --- a/csi/moac/node-composition.nix
   +++ b/csi/moac/node-composition.nix
   @@ -2,11 +2,12 @@

    {pkgs ? import <nixpkgs> {
        inherit system;
   -  }, system ? builtins.currentSystem, nodejs ? pkgs."nodejs-12_x"}:
   +  }, system ? builtins.currentSystem, nodejs-slim ? pkgs.nodejs-slim-12_x, nodejs ? pkgs."nodejs-12_x"}:
    let
      nodeEnv = import ./node-env.nix {
        inherit (pkgs) stdenv python2 utillinux runCommand writeTextFile;
        inherit nodejs;
   +    inherit nodejs-slim;
        libtool = if pkgs.stdenv.isDarwin then pkgs.darwin.cctools else null;
      };
    in
   diff --git a/csi/moac/node-env.nix b/csi/moac/node-env.nix
   index e1abf53..4d35a5e 100644
   --- a/csi/moac/node-env.nix
   +++ b/csi/moac/node-env.nix
   @@ -1,6 +1,6 @@
    # This file originates from node2nix

   -{stdenv, nodejs, python2, utillinux, libtool, runCommand, writeTextFile}:
   +{stdenv, nodejs-slim, nodejs, python2, utillinux, libtool, runCommand, writeTextFile}:

    let
      python = if nodejs ? python then nodejs.python else python2;
   @@ -395,7 +395,7 @@ let
        in
        stdenv.mkDerivation ({
          name = "node_${name}-${version}";
   -      buildInputs = [ tarWrapper python nodejs ]
   +      buildInputs = [ tarWrapper python nodejs-slim nodejs ]
            ++ stdenv.lib.optional (stdenv.isLinux) utillinux
            ++ stdenv.lib.optional (stdenv.isDarwin) libtool
            ++ buildInputs;
   ```

## Building a Nix Package

All Nix files generated by the `node2nix` tool are part of the repository so if
you just want to build the package without changing or updating dependencies
it is rather simple:

1. Build the Nix MOAC package:

   ```bash
   nix-build default.nix -A package
   ```

2. Run MOAC from the package:
   ```bash
   ./result/...
   ```

## Architecture

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

Volume life cycle can be described by a finite state automaton (FSA). It is
crucial for understanding what and when can happen with the volume. Imperfect
approximation of FSA diagram for the volume follows:

```text
                             new volume
                                +
                                |
                           +----v-----+
                           |          |
                           | pending  |
                           |          |
                           +----+-----+
                                |
                                | set up the volume
                                |
                           +----v-----+
                           |          |
                           | healthy  |
                           |          |
                           +----+-----+
                                |
                                |
                    yes         v             no
                    +--+is volume published?+--+
                    |                          |
                    |                          |                        +----------+
                    v                          v                    no  |          |
          yes  is any replica              any replica online?  +-------> faulted  |
reshare  <---+ unreachable from                +                        |          |
replica           nexus?                       |                        +----------+
                    +                          | yes
                    | no                       |
          yes       v                          v                        +----------+
recreate <---+ is nexus missing?           insufficient # of sound yes  |          |      create new
nexus               +                       replicas?             +-----> degraded +----> replica
                    | no                                                |          |
          no        v                                                   +-----^----+
share    <---+ is nexus exposed?                                              |
nexus               +                                                         |
                    | yes                                                     |
                    v                                                         |
               insufficient # of  yes                                         |
               sound replicas?   +--------------------------------------------+
                    +
                    | no
                    v            +------------+
               volume under yes  |            |
                 rebuild?  +-----> degraded   |
                    +            |            |
                    | no         +------------+
                    v
                +---+-----+
                |         |
                | healthy |
                |         |
                +---+-----+
                    |
                    v               yes
                any replica faulty? +--> remove it
                    +
                    | no
                    v
                more online replicas yes
                than needed?        +---> remove the least
                    +                     preferred replica
                    | no
                    v
                should move
                volume to    yes
                different   +--->  create new replica
                node(s)?
```

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
    "image": "node:12",
    "workspaceMount": "source=/path/to/repo/on/the/host/Mayastor,target=/workspace,type=bind,consistency=cached",
    "workspaceFolder": "/workspace",
    "extensions": [
        "chenxsan.vscode-standardjs"
    ]
}
```

Note that this env is suitable for writing the code and running the tests but
for building nix docker image you need nix package manager to be installed.
