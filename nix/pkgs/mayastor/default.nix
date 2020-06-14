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
  channel = import ../../lib/rust.nix {
    inherit fetchFromGitHub;
    inherit pkgs;
  };
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
    rev = "0165058897ae3d8cdd3bcd3c56ef55eb3b1eb13f";
    sha256 = "09sgj7rbaqalf4ywnjaw6cimpn6p4ibzlbyjdsx5sjf1rlb5jv9a";
  };

  mayastor = rustPlatform.buildRustPackage rec {
    name = "mayastor";
    cargoSha256 = "12h4qy4afl82pwswmvv1jpixvw5b5g6s95x32fnsvbbyhzykiarn";
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
