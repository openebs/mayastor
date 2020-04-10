# When we use contents []; what in effect will happen is that during the
# generation of the layers, /bin /include and /lib gets"flattend" into a
# single directory. (lack of a better term sorry). What this means is that
# if we have pkgs.A and pkgs.B it would yield, typically;
#
# /store/nix/<hash>-A/{bin,lib,include}
# /store/nix/<hash>-B/{bin,lib,include}
#
# After we have a "contents" all outputs get stored in /include and /lib
# this is fine, however, some of the team would like to have an actual nix
# environment, that is to say, the nix tools and nix-env -q remain working.
#
# In the case of mayastor for example this is exactly what we want. We have
# /bin/mayastor and a couple of /libs.
#
# To keep the existing derivations working, nix-style we must construct a
# user profile as you normally would have if you where running nix.
#
# A user profile is a symlinked together directory /nix/var/nix/profiles/<name>
# the construction of the profile is done by nix-env so we emulate that within
# docker layer cake, by importing the output derivations in to the nix-store
# after building the packages.
#

{ bashInteractive
, binutils
, cacert
, coreutils
, diffutils
, e2fsprogs
, findutils
, fio
, git
, gnugrep
, gnused
, gnumake
, closureInfo
, rustup
, numactl
, dockerTools
, liburing
, openssl
, python
, rdma-core
, utillinux
, libspdk
, libiscsi
, mkContainerEnv
, libaio
, binutils-unwrapped
, nodejs
, protobuf
, llvmPackages
, gzip
, less
, nix
, procps
, stdenv
, xfsprogs
, xz
, glibc
}:
let
  #
  # priorities determine what gets linked in. If, two or more packages have
  # the same binaries. Remember, a profile is symling A/bin and B/bin into a
  # profile. If both A and B have /bin/foo -- we have a collision.
  #

  bintools = binutils.overrideAttrs (o: rec { meta.priority = 9; });
  libclang =
    llvmPackages.libclang.overrideAttrs (o: rec { meta.priority = 4; });

  libc = glibc.overrideAttrs (o: rec { meta.priority = 4; });
  # useful things to be included within the container
  core = [
    bashInteractive
    bintools
    cacert
    coreutils
    findutils
    git
    gnugrep
    gnused
    gzip
    less
    libc
    nix
    procps
    stdenv.cc
    stdenv.cc.cc.lib
    xz
  ];

  # things we need for rust
  rust = [ rustup libclang protobuf ];
  # this we need for node
  node = [ nodejs python gnumake ];

  # generate a user profile for the image
  profile = mkContainerEnv {
    derivations = [
      fio
      diffutils
      e2fsprogs
      libaio
      libiscsi.lib
      libspdk
      liburing
      numactl
      openssl
      rdma-core
      utillinux
      utillinux.dev
      xfsprogs
    ] ++ core ++ rust ++ node;
  };

  image = dockerTools.buildImage {
    name = "mayadata/ms-buildenv";
    tag = "nix";

    extraCommands = ''
      # create the Nix DB
      export NIX_REMOTE=local?root=$PWD
      export USER=nobody

      # import the profile into the new DB
      ${nix}/bin/nix-store --load-db < ${
        closureInfo { rootPaths = [ profile ]; }
      }/registration

      # set the user profile
      ${profile}/bin/nix-env --profile nix/var/nix/profiles/default --set ${profile}

      # minimal "convenience" things as its common to use /bin/bash in containers
      # this is not strictly necessary though as the PATH will resolve it anyway

      mkdir -p bin usr/bin
      ln -s /nix/var/nix/profiles/default/bin/sh bin/sh
      ln -s /nix/var/nix/profiles/default/bin/env usr/bin/env
      ln -s /nix/var/nix/profiles/default/bin/bash bin/bash

      # setup shadow, bashrc
      # instead of cat EOF magic, simply copy over some files to /etc
      cp -r ${./root/etc} etc

      chmod +w etc etc/group etc/passwd etc/shadow
      # make sure /tmp exists which is used by cargo
      mkdir -m 0777 tmp
      # used for our mayastor RPC socket
      mkdir -p -m 0777 var/tmp

      # you must still call nix-channel update if you wish to install something
      # we don't do that by default because we we are already pretty fat. The
      # root user is assumed to be the default user here, so create a dir.

      mkdir root
      echo 'https://nixos.org/channels/nixpkgs-unstable' > root/.nix-channels
    '';

    config = {
      Cmd = [ "/nix/var/nix/profiles/default/bin/bash" ];
      Env = [
        # set some environment variables so that cargo can find them
        "PROTOC_INCLUDE=${protobuf}/include"
        "PROTOC=${protobuf}/bin/protoc"
        "LIBCLANG_PATH=${llvmPackages.libclang}/lib"
        "LOCAL_ACRHIVE=${glibc}/lib/locale/locale-archive"

        # C_INCLUDE_PATH is searched by compilers next to any includes it gets
        # passed during invocation

        "C_INCLUDE_PATH=${libspdk}/include/spdk:/nix/var/nix/profiles/default/include"

        # compilers (like rustc) need to know where the libraries are
        "LIBRARY_PATH=/nix/var/nix/profiles/default/lib"
        # same for runtime but only needed for non nix binaries who are not elf patched
        "LD_LIBRARY_PATH=/nix/var/nix/profiles/default/lib"
        # ensure that the generated profile is loaded when we enter the container
        "ENV=/nix/var/nix/profiles/default/etc/profile.d/nix.sh"

        "PAGER=less"
        "PATH=/nix/var/nix/profiles/default/bin"

        # in order for tools that download stuff to work we need certs
        "SSL_CERT_FILE=/nix/var/nix/profiles/default/etc/ssl/certs/ca-bundle.crt"
        "GIT_SSL_CAINFO=/nix/var/nix/profiles/default/etc/ssl/certs/ca-bundle.crt"
        "RUST_BACKTRACE=1"
      ];
    };
  };
in
image // {
  meta = image.meta // { description = "Mayastor development container"; };
}
