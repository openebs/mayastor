# Running Mayastor

## Hard Requirements

Mayastor supports the following [Instruction Set Architectures (ISA)][isa]:

- x86_64 (Nehalem or later)
- aarch64

Your system will need several [control groups][control-groups] configured.

```nix
# /etc/nixos/configuration.nix
boot.kernelParams = [ "cgroup_enable=cpuset" "cgroup_memory=1" "cgroup_enable=memory" ];
```

It will also need at least 512 2 MB Hugepages configured.

> Learn more about hugepages: [parts 1][hugepages-lwn-one], [2][hugepages-lwn-two],
> [3][hugepages-lwn-three], [4][hugepages-lwn-four], [5][hugepages-lwn-five].

In NixOS:

```nix
# /etc/nixos/configuration.nix
boot.kernelParams = [ "hugepagez=2M" "hugepages=4096" ];
boot.kernel.sysctl = {
  "vm.hugetlb_shm_group" = "6969";
};
systemd.mounts = [
  # disable mounting hugepages by systemd,
  # it doesn't know about 1G pagesize
  { where = "/dev/hugepages";
      enable = false;
  }
  { where = "/dev/hugepages/hugepages-2MB";
      enable  = true;
      what  = "hugetlbfs";
      type  = "hugetlbfs";
      options = "pagesize=2M,gid=6969,mode=0775";
      requiredBy  = [ "basic.target" ];
  }
  { where = "/dev/hugepages/hugepages-1G";
      enable  = true;
      what  = "hugetlbfs";
      type  = "hugetlbfs";
      options = "pagesize=1G,gid=6969,mode=0775";
      requiredBy  = [ "basic.target" ];
  }
];
users = {
  users.${MAYASTOR_USER} = {
    # ...
    extraGroups = [ /* ... */ "hugepages" ];
  };
  groups = {
    hugepages = {
      gid = 6969; # Any ID works.
    };
  };
};
```

**If you changed any of these, reboot after.**

## Optional Prerequisites

In order to use the full feature set of Mayastor, some or all of the following can be met:

- A Linux Kernel 5.1+ (with [`io-uring`][io_uring-intro] support)
- The following kernel modules loaded:

  - `nbd`: Network Block Device support
  - `nvmet`: NVMe Target support
  - `nvmet_rdma`: NVMe Target (rDMA) support
  - `nvme_fabrics`: NVMe over Fabric support
  - `nvme_tcp`: NVMe over TCP support
  - `nvme_rdma`: NVMe (rDMA) support
  - `nvme_loop`: NVMe Loop Device support

  To load these on NixOS:

  ```nix
  # /etc/nixos/configuration.nix
  boot.kernelModules = [
    "nbd" "xfs" "btrfs" "nvmet" "nvme_fabrics" "nvmet_rdma" "nvme_tcp" "nvme_rdma" "nvme_loop"
  ];
  ```

  To load these on non-NixOS machines:

  ```bash
  modprobe nbd xfs btrfs nvmet nvmet_rdma nvme_fabrics nvme_tcp nvme_rdma nvme_loop
  ```

- For Asymmetric Namespace Access (ANA) support (early preview), the following kernel build configuration enabled:

  - `CONFIG_NVME_MULTIPATH`: enables support for multipath access to NVMe subsystems

  This is usually already enabled in distributions kernels, at least for RHEL/CentOS 8.2, Ubuntu 20.04 LTS, and SUSE
  Linux Enterprise 15.2.

  On some distributions such as RHEL 8, the feature must be enabled manually:

  ```sh
  # /etc/modprobe.d/nvme-multipath
  options nvme_core multipath=1
  ```

  followed by reloading the `nvme-core` module or rebooting.

  To build this on NixOS:

  ```nix
  # /etc/nixos/configuration.nix
  boot.kernelPackages = pkgs.linuxPackages;
  boot.kernelPatches = [ {
    name = "nvme-multipath";
    patch = null;
    extraConfig = ''
      NVME_MULTIPATH y
      '';
  } ];
  ```

  followed by:

  ```sh
  sudo nixos-rebuild boot
  ```

- An NVMe device. (Typically via PCI-E through a standard slot or [M.2][m-dot-2] port)
- A version of [`nix`][nix-install] configured as in the [build guide.][doc-build]

## Running binaries directly

Like in [the build guide's _Iterative Build_][doc-build-iterative-builds] section invoke
[`nix shell`][nix-shell]. This time, don't pass `--derivation`, this will cause `nix` to
evaluate the output, instead of the derivation.

```bash
nix shell -f . mayastor
io-engine-client --version
mayastor --version
```

Running `mayastor` should proceed without issue:

```bash
❯ mayastor
[2021-01-28T14:40:30.659032358-08:00  INFO mayastor:mayastor.rs:46] Starting Mayastor ..
[2021-01-28T14:40:30.659087313-08:00  INFO mayastor:mayastor.rs:47] kernel io_uring support: yes
[2021-01-28T14:40:30.659108924-08:00  INFO mayastor:mayastor.rs:51] free_pages: 4096 nr_pages: 4096
[2021-01-28T14:40:30.659132238-08:00  INFO mayastor::subsys::config:mod.rs:361] Applying Mayastor configuration settings
# ...
[2021-01-28T14:40:30.902100398-08:00  INFO mayastor:mayastor.rs:59] Mayastor started 🚀 ...
# ...
```

## Running Docker images directly

[**After building the images**][doc-build-building-docker-images], load them:

```bash
for FILE in artifacts/docker/*.tar; do
 echo "Loading ${FILE}..."
 docker load --quiet --input ${FILE}
done
```

**Notice the generated tags.** The [build guide][doc-build-building-docker-images] shows how to
retag these to `latest` if required. If you forget, check `docker images`.

**`io-engine-client`** and **`mayastor-csi`**:

```bash
docker run --interactive --rm mayadata/io-engine-client:${TAG}
docker run --interactive --rm mayadata/mayastor-csi:${TAG}
```

**`mayastor`** requires some special parameters:

```bash
docker run \
  --rm \
  --interactive \
  --privileged \
  --volume /dev:/dev:rw \
  --volume /dev/shm:/dev/shm:rw \
  --volume /dev/hugepages:/dev/hugepages:rw \
  --network host \
  mayadata/mayastor:${TAG}
```

Why these parameters?

- `--privileged` to allow controlling memory policies.

  > **TODO:** We can use [control groups][control-groups] for this!

- `-v /dev:/dev:rw` is needed to get access to any raw device you might want to consume as local
  storage and huge pages
- `-v /dev/shm:/dev/shm:rw` is needed as for a circular buffer that can trace any IO operations
  as they happen
- `--network host` to bypass virtual networks which might impact latency.

The **`moac`** container should not be directly launched.

## Running as a systemd service

> **TODO:** Mayastor currently depends on [Mozilla's Rust Overlay][mozilla-rust-overlay],
> so these instructions don't work, yet!

On NixOS:

```nix
# /etc/nixos/configuration.nix
nixpkgs.overlays = [
  (import "${(builtins.fetchGit {
    url = "https://github.com/openebs/Mayastor.git";
    ref = "master";
    rev = "a9fc77f2ae30e909244556fc797451931dab3dd5";
  }).outPath}/nix/mayastor-overlay.nix")
];

systemd.services.mayastor = {
  wantedBy = [ "multi-user.target" ];
  after = [ "network.target" ];
  description = "A cloud native declarative data plane.";
  serviceConfig = {
    Type = "forking";
    User = "nobody";
    ExecStart = "${pkgs.mayastor}/bin/mayastor";
    AmbientCapabilities = "CAP_SETPCAP CAP_SYS_ADMIN CAP_IPC_LOCK CAP_SYS_NICE";
  };
};
```

On a non-NixOS system, create the following, and point `ExecStart` at your [`mayastor`
bundle][doc-build-building-portable-nix-bundles]:

```systemd
# /etc/systemd/user/mayastor.service
[Unit]
After=network.target
Description=A cloud native declarative data plane.

[Service]
AmbientCapabilities=CAP_SETPCAP CAP_SYS_ADMIN CAP_IPC_LOCK CAP_SYS_NICE
ExecStart=/usr/bin/mayastor
Type=simple
User=nobody
```

## Running as a NixOS Service

**TODO:** This is not supported yet! This is an aspiration:

```nix
# /etc/nixos/configuration.nix
services.mayastor.enable = true;
services.mayastor.grpc-bind = "0.0.0.0:10124";
services.mayastor.config = /etc/mayastor/config.yml;
services.mayastor.huge-dir = /dev/hugepages/;
# Etc...
```

## Running in a VM

> **TODO:** We're still writing this! Sorry! Let us know if you want us to prioritize this!

## Running on a scratch Kubernetes cluster

First, get your k8s cluster working, our team likes to use the scripts in `terraform/` for this.
That will use [`lxd`][lxd] and [`libvirtd`][libvirtd]. See [`terraform/README`][terraform-readme]
for detailed instructions.

Otherwise, you can follow [the production guide][running-on-a-real-kubernetes-cluster] on your favourite.

Our testing showed some common Kubernetes development environments are not sufficient for
Mayastor development.

Here are the ones known to not work:

- [`kind`][kind]

## Running on a real Kubernetes cluster

We have a [production deployment manual & user manual][manual] prepared. Please refer to that for
production Mayastor deployment and operation instructions.

[running-on-a-real-kubernetes-cluster]: #running-on-a-real-kubernetes-cluster
[doc-build]: ./build.md
[doc-build-iterative-builds]: ./build.md#Iterative-Builds
[doc-build-building-docker-images]: ./build.md#Building-Docker-images
[doc-build-building-portable-nix-bundles]: ./build.md#Building-portable-Nix-bundles
[doc-test]: ./test.md
[io_uring-intro]: https://unixism.net/loti/what_is_io_uring.html
[hugepages-lwn-one]: https://lwn.net/Articles/374424/
[hugepages-lwn-two]: https://lwn.net/Articles/375096/
[hugepages-lwn-three]: https://lwn.net/Articles/376606/
[hugepages-lwn-four]: https://lwn.net/Articles/378641/
[hugepages-lwn-five]: https://lwn.net/Articles/379748/
[m-dot-2]: https://en.wikipedia.org/wiki/M.2
[nix-install]: https://nixos.org/download.html
[nix-shell]: https://nixos.org/manual/nix/unstable/command-ref/new-cli/nix3-shell.html
[control-groups]: https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v1/cgroups.html
[mozilla-rust-overlay]: https://github.com/mozilla/nixpkgs-mozilla/blob/master/rust-overlay.nix
[isa]: https://en.wikipedia.org/wiki/Instruction_set_architecture
[kind]: https://kind.sigs.k8s.io/
[manual]: https://mayastor.gitbook.io/
[lxd]: https://linuxcontainers.org/
[libvirtd]: https://libvirt.org/index.html
[terraform-readme]: ./terraform/readme.adoc
