## RUST

We make use of async/await and therefor we *need* a compiler that supports that. We currently
use the nightly compiler. Nightly is required in all of the provided build possibilities listed below

Build options
==================
- [Building with Nix (recommended)](#Building-the-sources-with-nixpkg)
- [Build inside docker](#Build-inside-docker)
- [Building the hard way](#Build-it-the-hard-way)

## Building the sources with nixpkg

As the underlaying distribution you can use nixos or any other linux
distribution if you install a nix package manager on top of it.
Example of nixos system configuration `/etc/nixos/configuration.nix`
suitable for a dev box:

```nix
{ config, pkgs, ... }:

{
  imports =
    [ # Include the results of the hardware scan.
      ./hardware-configuration.nix
    ];
  boot.loader.grub.enable = true;
  boot.loader.grub.version = 2;
  boot.loader.grub.device = "/dev/sda"; # or whatever is appropriate
  boot.kernelModules = ["nbd" "xfs" "nvme_tcp"];
  boot.kernelParams = ["hugepages=512" "hugepagesz=2MB"];
  services.openssh.enable = true;
  virtualisation.docker.enable = true;
  users.users.your_username = {
    isNormalUser = true;
    extraGroups = [ "wheel" "docker" ];
  };
  system.stateVersion = "19.03";
  security.sudo.enable = true;
  security.sudo.wheelNeedsPassword = false;
}
```

Installation of a [nix package manager](https://nixos.org/nix/download.html) on
other distros:

```bash
curl -L https://nixos.org/nix/install | sh
```

We have provided a `shell.nix` file that can be used to build and compile
MayaStor from source without impacting your system.
Follow the short instruction and you should be all set!

```bash
cd MayaStor
nix-shell
git submodule update --init
cargo build --all
```

Binaries will be installed in `$(CURDIR)/target/release` after running the build you can use
`$(CURDIR)/target/release/mayastor-client` to create a Nexus.

## Build inside docker

This is most close to the environment used for building and testing of
MayaStor in our github CI/CD pipeline. But could be used for other purposes
as well. The following command starts a container with build environment
for MayaStor. The sources are mounted from the host at /code/MayaStor.
That is optional. You can clone the sources from github instead.

```bash
docker run -it --privileged -v /dev:/dev:rw -v /dev/shm:/dev/shm:rw \
    -v /dev/hugepages:/dev/hugepages:rw --network host \
    -v /code/MayaStor:/MayaStor mayadata/ms-buildenv:latest /bin/sh
```

Docker image is essentially just a nixos image with a couple of
utilities and pre-built mayastor dependencies. You can just
enter the nix shell and build the mayastor.

```bash
cd MayaStor
nix-shell
git submodule update --init
cargo build --all
```

After that you should be able to start MayaStor:

```bash
$ ./target/debug/mayastor
main.rs:  28:: *NOTICE*: free_pages: 658 nr_pages: 1024
Starting SPDK v19.07 / DPDK 19.05.0 initialization...
[ DPDK EAL parameters: MayaStor --no-shconf -c 0x1 --log-level=lib.eal:6 --log-level=lib.cryptodev:5 --log-level=user1:6 --base-virtaddr=0x200000000000 --match-allocations --file-prefix=spdk_pid57086 ]
app.c: 627:spdk_app_start: *NOTICE*: Total cores available: 1
reactor.c: 251:_spdk_reactor_run: *NOTICE*: Reactor started on core 0
nexus_module.rs: 105:: *NOTICE*: Initializing Nexus CAS Module
cryptodev_aesni_mb_create() line 1304: IPSec Multi-buffer library version used: 0.52.0

executor.rs:  94:: *INFO*: Started future executor on thread ThreadId(1)
iscsi_target.rs:  85:: *INFO*: Created default iscsi portal group
iscsi_target.rs: 100:: *INFO*: Created default iscsi initiator group
nvmf_target.rs: 294:: *NOTICE*: Created nvmf target at 127.0.0.1:4401
tcp.c: 535:spdk_nvmf_tcp_create: *NOTICE*: *** TCP Transport Init ***
nvmf_target.rs: 344:: *NOTICE*: Added tcp nvmf transport 127.0.0.1:4401
tcp.c: 730:spdk_nvmf_tcp_listen: *NOTICE*: *** NVMe/TCP Target Listening on 127.0.0.1 port 4401 ***
nvmf_target.rs: 364:: *NOTICE*: nvmf target listens on 127.0.0.1:4401
nvmf_target.rs: 415:: *NOTICE*: nvmf target 127.0.0.1:4401 accepts new connections
main.rs:  31:: *NOTICE*: MayaStor started (fcaf10b-modified)...
```

Feel free to change the [DockerFile](../Dockerfile) to your convenience.

### Justifications for the volume mounts:

- `/dev` is needed to get access to any raw device you might want to consume as local storage and huge pages
- `/dev/shm` is needed as for a circular buffer that can trace any IO operations as they happen
- `--network host` is needed because we dont not use virtual networks (to reduce latency impact)
- `/code/MayaStor` the host path to your checked out source code

## Running tests within the container

If you wish to run some of our higher-level test cases (like for example CSI), you need to make sure you have the
proper kernel modules loaded (nbd, xfs and nvme_tcp) as well as allocate at least some 2MB hugepages.

```bash
modprobe {nbd,xfs,nvme_tcp}
echo 512 | sudo tee  /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
```

Then, for example:

```bash
root@gilakubuntu:/MayaStor/test/grpc# ./node_modules/mocha/bin/mocha test_csi.js
  csi
    identity
      ✓ probe
      ✓ get plugin info
      ✓ get plugin capabilities
    node
      ✓ get info
      ✓ get capabilities
    stage and unstage xfs volume
      ✓ should be able to stage volume (69ms)
      ✓ get volume stats (62ms)
      ✓ staging the same volume again should return ok (idempotent)
      ✓ staging a volume with a non existing bdev should fail with Internal Error (57ms)
      ✓ staging a volume with the same staging path but with a different bdev should fail
      ✓ should fail to stage a volume with the bdev using a different target path
      ✓ should not unstage a volume with an unknown volumeid and return NOTFOUND error (51ms)
      ✓ should fail to stage a volume with a missing volume ID
      ✓ should fail to stage a volume with a missing stage target path
      ✓ should fail to stage a volume with a missing access type
      ✓ should fail to stage a volume with a missing accces mode
      ✓ should fail to stage a volume with missing volume_capability section
      ✓ should be able to unstage volume
    stage and unstage ext4 volume
      ✓ should be able to stage volume (59ms)
      ✓ should be able to unstage volume (38ms)
    stage misc
      ✓ should fail to stage unsupported fs
      ✓ should clean up nbd dev upon mount failure (47ms)
    publish and unpublish
      MULTI_NODE_READER_ONLY staged volume
        ✓ should publish a volume in ro mode and test it is idempotent op
        ✓ should fail when re-publishing with a different staging path
        ✓ should fail with a missing target path
        ✓ should fail to publish the volume as rw
        ✓ should be able to unpublish ro volume (56ms)
        ✓ should be able to unpublish rw volume
      MULTI_NODE_SINGLE_WRITER staged volume
        ✓ should publish ro volume
        ✓ should publish rw volume (58ms)
        ✓ should be able to unpublish ro volume
        ✓ should be able to unpublish rw volume

```

If you wish to run the MayaStor data path tests, make sure you specify `test-threads=1`

```bash
cargo test  -- --test-threads=1
```

## Build it the hard way

When you really want to build everything manually, the biggest hurdle to overcome is to install the SPDK/DPDK. As these
are not packaged (or not recent) by most distro's its a manual step. We have provided scripts to make this as easy as
possible but they only work for Ubuntu and Fedora.

The basic steps are:

```
git submodule update --init --recursive
sudo ./spdk-sys/spdk/scripts/pkgdep
./spdk-sys/build.sh --enable-debug --without-isal --with-iscsi-initiator --with-rdma \
             --with-internal-vhost-lib --disable-tests \
             --with-crypto
```
At this point you will have a .so file in `spdk-sys/build` you can leave it there and set the run path flag for rustc to find it:

```
export RUSTFLAGS="-C link-args=-Wl,-rpath=$(pwd)/spdk-sys/build"
```

Or, you can copy over the .so to `/usr/local/lib` or something similar.

One this is done, you should be able to run `cargo build --all`

## Building docker images

Use NIX to build the images. Note that the images are based on NIX packages
that are built as part of building the image. The tag of the image will be
short commit hash of the top-most commit or a tag name of the commit if
present. Example of building a moac package:

```bash
nix-build -A images.moac-image
```

At the end of the build is printed path to docker image tar archive. Load the
image into Docker (don't use _import_ command) and run bash to poke around:

```bash
docker load -i /nix/store/hash-docker-image-moac.tar.gz
docker run --rm -it image-hash /bin/bash
```

Mayastor and csi plugin images can have multiple flavours. Production image
name does not contain the flavour name (i.e. `mayastor-image`). Debug image
contains the `dev` in its name (i.e. `mayastor-dev-image`). Mayastor package
has additional flavour called `adhoc` (`mayastor-adhoc-image`), that is handy
for testing because it is not based on mayastor package but rather on whatever
binaries are present in `target/debug` directory.

## Some background information

MayaStor makes use of subsystems that are not yet part of major distributions, for example:

     - nvme-tcp initiator (Linux 5.x and newer)
     - Recent DPDK version (i.e 19.x)

Fortunately, this is something that will be solved over time automatically. In the meantime, we have
tried to make it as simple as possible by providing several options for you.

Mayastor, in all cases, **requires the nightly rust compiler with async support**.
You don't need to have a 5.x kernel unless you want to use NVMF.

If you already have rust installed but not nightly, use rustup to install it before continuing.

### spdk-sys

The crate that provides the glue between SPDK and Mayastor is hosted in this [repo](https://github.com/openebs/spdk-sys)
feel free to go through it and determine if you want to install libspdk using those instructions or directly from
[here](https://github.com/openebs/spdk). If you chose either of these methods, make sure you install such that
during linking, it can be found.
