{ stdenv
, busybox
, clang
, dockerTools
, e2fsprogs
, fetchFromGitHub
, lib
, libaio
, libiscsi
, libspdk
, libudev
, liburing
, llvmPackages
, makeRustPlatform
, numactl
, openssl
, pkgs
, pkg-config
, protobuf
, rdma-core
, release ? true
, utillinux
, writeScriptBin
, xfsprogs
}:
let
  sources = import ../../sources.nix;
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable.rust;
    cargo = channel.stable.cargo;
  };
in
rec {

  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix:
            lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;

  release-src = fetchFromGitHub {
    owner = "openebs";
    repo = "mayastor";
    rev = "1e5dca302cff3e66220c3cf892a08c89733d535e";
    sha256 = "0w7apa3rnjbkz2ma1nca42w729zj33k2xvfy34c84x6q0f4s9zjz";
  };

  mayastor = rustPlatform.buildRustPackage rec {
    name = "mayastor";
    cargoSha256 = "06zcprq4hvfpy8ikvxiqy4wy936h5ymnnkyzp6bkpqqq691xcrgf";
    version = "0.1.1";
    src = if release then release-src else whitelistSource ../../../. [
      "Cargo.lock"
      "Cargo.toml"
      "cli"
      "csi"
      "devinfo"
      "jsonrpc"
      "mayastor"
      "nvmeadm"
      "rpc"
      "spdk-sys"
      "sysfs"
    ];

    LIBCLANG_PATH = "${llvmPackages.libclang}/lib";

    # these are required for building the proto files that tonic can't find otherwise.
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";
    C_INCLUDE_PATH = "${libspdk}/include/spdk";

    buildInputs = [
      clang
      e2fsprogs
      libaio
      libiscsi.lib
      libspdk
      liburing
      libudev.dev
      llvmPackages.libclang
      numactl
      openssl
      pkg-config
      protobuf
      utillinux
      xfsprogs
      utillinux.dev
    ];

    buildType = if release then "release" else "debug";
    verifyCargoDeps = false;

    doCheck = false;
    meta = { platforms = stdenv.lib.platforms.linux; };
  };

  env = stdenv.lib.makeBinPath [ busybox utillinux xfsprogs e2fsprogs ];

  mayastorIscsiadm = writeScriptBin "mayastor-iscsiadm" ''
    #!${stdenv.shell}
    chroot /host /usr/bin/env -i PATH="/sbin:/bin:/usr/bin" iscsiadm "$@"
  '';

  mayastorImage = dockerTools.buildLayeredImage {
    name = "mayadata/mayastor";
    tag = release-src.rev;
    created = "now";
    contents = [ busybox mayastor ];
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor" ];
    };
  };

  mayastorCSIImage = dockerTools.buildLayeredImage {
    name = "mayadata/mayastor-grpc";
    tag = release-src.rev;
    created = "now";
    contents = [ busybox mayastor mayastorIscsiadm ];
    config = {
      Entrypoint = [ "/bin/mayastor-agent" ];
      Env = [ "PATH=${env}" ];
    };
  };
}
