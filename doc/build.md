# Building Mayastor

> You will not need to always build Mayastor. We will provide official x86_64
> & aarch64 binaries and images in future releases.

Mayastor is a multi-component [Rust][rust-lang] project that makes heavy use of
[Nix][nix-explore] for our development and build process.

If you're coming from a non-Rust (or non-Nix) background, **building Mayastor may be a bit
different than you're used to.** There is no `Makefile`, you won't need a build toolchain,
you won't need to worry about cross compiler toolchains, and all builds are reproducible.

## Table of Contents

- [Prerequisites](#Prerequisites)
- [Iterative Builds](#Iterative-Builds)
- [Artifacts](#Artifacts)

## Prerequisites

Mayastor **only** builds on modern Linuxes. We'd adore contributions to add support for
Windows, FreeBSD, OpenWRT, or other server platforms.

If you do not have a Linux system:

- **Windows:** We recommend using [WSL2][windows-wsl2] if you only need to
  build Mayastor. You'll need a [Hyper-V VM][windows-hyperv] if you want to use it.
- **Mac:** We recommend you use [Docker for Mac][docker-install]
  and follow the Docker process described. Please let us know if you find a way to
  run it!
- **FreeBSD:** We _think_ this might actually work, SPDK is compatible! But, we haven't
  tried it yet.
- **Others:** This is kind of a "Do-it-yourself" situation. Sorry, we can't be more help!

The only thing your system needs to build Mayastor is [**Nix**][nix-install].

Usually [Nix][nix-install] can be installed via (Do **not** use `sudo`!):

```bash
curl -L https://nixos.org/nix/install | sh
```

> **Can't install Nix?**
>
> That's totally fine. You can use [`docker`][docker-install] just fine for one-off or occasional PRs!
>
> This flow will get you a pre-fetched `nix` store:
>
> ```bash
> docker run --name mayastor-nix-prefetch -it -v $(pwd):/scratch:rw --privileged --workdir /scratch nixos/nix nix-shell --run "exit 0"
> docker commit mayastor-nix-prefetch mayastor/dev-env:latest
> docker rm mayastor-nix-prefetch
> docker run --rm -it -v $(pwd):/scratch:rw --workdir /scratch mayastor/dev-env:latest nix-shell
> ```
>
> To re-enter, just run the last command again.

- Some of our team uses [NixOS][nixos] which has `nix` baked in, but you don't need to.
- Some of our team uses [`direnv][direnv], but you don't need to.

For some tasks, we use features from `nixUnstable`. You can use `nixos-unstable`
**(or `nixpkgs-unstable` for `nix` users)** by [changing your channel][nix-channel].

First, setting the following:

```nix
{ pkgs, ... }: {
   nix.extraOptions = ''
      experimental-features = nix-command flakes
   '';
   nix.package = pkgs.nixUnstable;
}
```

Then, updating the channel:

```bash
$ sudo nix-channel --list
nixos https://nixos.org/channels/nixos-20.09
$ sudo nix-channel --remove nixos
$ sudo nix-channel --add https://nixos.org/channels/nixos-unstable nixos
$ sudo nixos-rebuild switch --update
```

> If you don't want, you can drop into a
> `nixUnstable` supporting shell with:
>
> ```bash
> nix-shell -I nixpkgs=channel:nixpkgs-unstable -p nixUnstable --command "nix --experimental-features 'nix-command flakes' develop -f . mayastor"
> ```
>
> Don't want to use `nixUnstable`? **That's ok!** Use `nix-shell` and `nix-build` as you normally would.

Check out the submodules:

```bash
git submodule update --init
```

**Want to run or hack on Mayastor?** _You need more configuration!_ See
[running][doc-run], then [testing][doc-test].

You can use a tool like [`direnv`][direnv] to automate `nix shell` entry.
If you are unable to use the Nix provided Rust for some reason, there are `norust` and
`nospdk` arguments to Nix shell. `nix-shell --arg norust true`

## Iterative Builds

Contributors often build Mayastor repeatedly during the development process.
Using [`nix develop`][nix-develop] to enter a more persistent development shell can help improve
iteration time:

```bash
nix develop -f . mayastor
```

Once entered, you can start any tooling (eg `code .`) to ensure the correct resources are available.
The project can then be interacted with like any other Rust project.

Building:

```bash
cargo build
cargo build --release
```

**Want to run or hack on Mayastor?** _You need more configuration!_ See
[running][doc-run], then [testing][doc-test].

To ensure you are aware of this, we greet you with a nice cow.

## Artifacts

There are a few ways to build Mayastor! If you're hacking on Mayastor, it's best to use
[`nix develop`][nix-develop] (above) then turn to traditional Rust tools. If you're looking for releases,
use [`nix build`][nix-build] or [`nix bundle`][nix-bundle] depending on your needs.

> **Why is the build process this way?**
>
> Mayastor creates [_reproducible builds_][reproducible-builds], it won't use any of your
> local system dependencies (other than `nix`). This is a component of the best practices of the
> [Core Infrastructure Initiative][cii-best-practices]. More on how Nix works can be found in the
> [Nix paper][nix-paper].

### Building non-portable Nix derivations

You can build release binaries of Mayastor with [`nix build`][nix-build]:

```bash
for PKG in mayastor; do
  echo "Building ${PKG} to artifacts/pkgs/${PKG}"; \
  nix build -f . -o artifacts/pkgs/${PKG} ${PKG};
done
```

Try them as if they were installed:

```rust
nix shell -f . mayastor
```

### Building portable Nix bundles

In order to make an artifact which can be distributed, we use [`nix bundle`][nix-bundle].

> **TODO:** We currently don't generate bundles some executables, such as
> `io-engine-client`. This is coming.

```bash
for BUNDLE in mayastor; do
  echo "Bundling ${BUNDLE} to artifacts/bundle/${BUNDLE}"; \
  nix bundle -f . -o artifacts/bundles/${BUNDLE} ${BUNDLE};
done
```

Test them:

```bash
for FILE in artifacts/bundles/*; do
 echo "Testing bundle ${FILE}..."
 ${FILE} --version
done
```

### Building Docker images

Build the Docker images with [`nix build`][nix-build]:

```bash
for IMAGE in \
  io-engine-client mayastor mayastor-csi io-engine-client kiiss-service \
  node-service volume-service pool-service rest-service node-operator; \
do
  echo "Building ${IMAGE} to artifacts/docker/${IMAGE}.tar"; \
  nix build -f . -o artifacts/docker/${IMAGE}.tar images.${IMAGE};
done
```

**Optionally,** the generated Docker images will **not** tag to the `latest`. You may wish to do that if
you want to run them locally:

```bash
for FILE in artifacts/docker/*.tar; do
 echo "Loading ${FILE}..."
 docker load --quiet --input ${FILE} \
   | awk '{ print $3 }' \
   | ( \
      read IMAGE; \
      LATEST=$(echo ${IMAGE} | awk '{split($0,a,":"); print a[1]}'):latest; \
      echo "Tagging ${IMAGE} to ${LATEST} (from ${FILE})."; \
      docker tag ${IMAGE} ${LATEST}; \
     );
done
```

Then, to test the images:

```bash
for FILE in artifacts/docker/*.tar; do
  echo "Loading ${FILE}..."
  docker load --quiet --input ${FILE} \
    | awk '{ print $3 }' \
    | ( \
        read IMAGE; \
        echo "Testing ${IMAGE} (from ${FILE})."; \
        docker run --rm --interactive ${IMAGE} --version; \
        docker rmi ${IMAGE} > /dev/null
      );
done
```

### Building KVM images

> **TODO:** We're still writing this! Sorry!

### Building Artifacts the Hard Way

> This isn't really the 'hard way', you'll still use `cargo`.

> **TODO:** We're still writing this! Sorry! For now, please refer to
> `spdk-rs` README on this matter.

[doc-run]: ./run.md
[doc-test]: ./test.md
[direnv]: https://direnv.net/
[nix-explore]: https://nixos.org/explore.html
[nix-install]: https://nixos.org/download.html
[nix-develop]: https://nixos.org/manual/nix/unstable/command-ref/new-cli/nix3-develop.html
[nix-paper]: https://edolstra.github.io/pubs/nixos-jfp-final.pdf
[nix-build]: https://nixos.org/manual/nix/unstable/command-ref/new-cli/nix3-build.html
[nix-bundle]: https://nixos.org/manual/nix/unstable/command-ref/new-cli/nix3-bundle.html
[nix-shell]: https://nixos.org/manual/nix/unstable/command-ref/new-cli/nix3-shell.html
[nix-channel]: https://nixos.wiki/wiki/Nix_channels
[nixos]: https://nixos.org/
[rust-lang]: https://www.rust-lang.org/
[windows-wsl2]: https://wiki.ubuntu.com/WSL#Ubuntu_on_WSL
[windows-hyperv]: https://wiki.ubuntu.com/Hyper-V
[docker-install]: https://docs.docker.com/get-docker/
[reproducible-builds]: https://reproducible-builds.org/
[cii-best-practices]: https://www.coreinfrastructure.org/programs/best-practices-program/
[direnv]: https://direnv.net/
