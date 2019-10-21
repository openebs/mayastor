## Building from source

MayaStor makes use of subsystems that are not yet part of major distributions, for example:

     - nvme-tcp initiator (Linux 5.x and newer)
     - Recent DPDK version (i.e 19.x)

Fortunately, this is something that will be solved over time automatically. In the meantime, we have
tried to make it as simple as possible by providing several options for you.

Mayastor, in all cases, **requires the nightly rust compiler with async support**.

## Installing RUST

Before you can start the build, Rust must be installed on the system. We recommend to use [rustup](https://rustup.rs/).
If you have not installed it, during install it will ask you what what channel to install, type `nightly` there.

After install make sure you source the environment i.e `source ~/.cargo/env`. And run `cargo -V` to see if it reports
a version.

If you already have rust installed but not nightly, use rustup to install it before continuing.

## Building the sources with nixpkg (advised)

To make things easier to install, we have provided a `shell.nix` file that can be used to build and compile MayaStor from source without impacting your system. The only requirement is that you have to have [Nixpkg](https://nixos.org/nix/download.html) installed. Once installed:

```bash
 $ cd MayaStor
 $ nix-shell
 $ cargo build --all
```
Binaries will be installed in `$(CURDIR)/target/release` after running the build you can use `$(CURDIR)/target/release/mctl` to create a Nexus.

## Build in side docker

We provide a [DockerFile](../docker/Dockerfile.ms-buildenv) which you can use to build Mayastor as well. This uses a more
recent userland so some optimizations are enabled.

If you do not want to build it in a container you can  pull `mayadata/ms-buildenv:latest` and use ours. We use it in our
CI/CD on a daily basis but might not be up2date with latest and greatest packages. Note, that the cargo cache will not
be preserved across container instantiation and the resulting binaries are owned by root. There are various ways to get
around that (for example set `$CARGO_HOME` to for example to `$PWD/.cargo`) but we leave that up for yourself to decide
as it is likely varies per system. If you are using the container you can make use of [cargo-make](https://github.com/sagiegurari/cargo-make)

To start the container use something like:

```bash
sudo docker run -it --privileged  -v /dev:/dev:rw -v /dev/shm:/dev/shm:rw --network host -v /code/MayaSto
r:/MayaStor mayadata/ms-buildenv:latest /bin/bash
```
Justifications for the volume mounts:

- `/dev` is needed to get access to any raw device you might want to consume as local storage and huge pages
- `/dev/shm` is needed as for a circular buffer that can trace any IO operations as they happen
- `--network host` is needed because we dont not use virtual networks (to reduce latency impact)
- `/code/MayaStor` the host path to your checked out source code

## Running tests within the container

If you wish to run some of our higher-level test cases (like for example CSI), you need to make sure you have the
proper kernel modules loaded (nbd and xfs) as well as allocate at least some 2MB hugepages.

```bash
modprobe {nbd,xfs}
echo 512 | sudo tee  /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
```

Then, for example:

```bash
root@gilakubuntu:/MayaStor/mayastor-test# ./node_modules/mocha/bin/mocha test_csi.js
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

## Build it the hardway

When you really want to build everything manually, the biggest hurdle to overcome is to install the SPDK/DPDK. As these
are not packaged (or not recent) by most distro's its a manual step. We have provided scripts to make this as easy as 
possible but they only work for Ubuntu and Fedora. 

The basic steps are:

```
git submodule update --init --recursive
sudo ./spdk-sys/spdk/scripts/pkgdep
./spdk-sys/build.sh --without-isal --with-crypto
```
At this point you will have a .so file in `spdk-sys/build` you can leave it there and set a runpath flag for rustc to find it:

```
export RUSTFLAGS="-C link-args=-Wl,-rpath=$(pwd)/spdk-sys/build"
```

Or, you can copy over the .so to `/usr/local/lib` or something similar. 

One this is done, you should be able to run `cargo build --all`

### spdk-sys 
The crate that provides the glue between SPDK and Mayastor is hosted in this [repo](https://github.com/openebs/spdk-sys)
feel free to go through it and determine if you want to install libspdk using those instructions or directly from
[here](https://github.com/openebs/spdk). If you chose either of these methods, make sure you install such that
during linking, it can be found.


