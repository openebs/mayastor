{ stdenv
, e2fsprogs
, libaio
, libiscsi
, libspdk
, liburing
, llvmPackages
, numactl
, openssl
, pkg-config
, protobuf
, rdma-core
, clang
, utillinux
, xfsprogs
, makeRustPlatform
, fetchFromGitHub
, dockerTools
, writeScriptBin
, pkgs ? import <nixpkgs>
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
with pkgs; rec {

  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        pkgs.lib.any
          (allowedPrefix:
            pkgs.lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes) src;

  mayastor = rustPlatform.buildRustPackage rec {
    name = "mayastor";
    cargoSha256 = "0fj204dh4x5066hdb22mfv48phpsvha6yq7gz2rbal9b20m8qlkx";
    version = "unstable";
    src = whitelistSource ../../../. [
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
      # We need to copy git as we use git_version!() in rust, we can also
      # use use nix to pass the hash if we want to by we should, mosty
      # likely, go with a proper release version.
      ".git"
    ];

    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";

    # these are required for building the proto files that tonic can't find otherwise.
    PROTOC = "${pkgs.protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${pkgs.protobuf}/include";
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

    buildType = "debug";
    verifyCargoDeps = false;

    doCheck = false;
    meta = { platforms = stdenv.lib.platforms.linux; };
  };

  env = pkgs.stdenv.lib.makeBinPath [ pkgs.busybox pkgs.utillinux pkgs.xfsprogs pkgs.e2fsprogs ];

  mayastorIscsiadm = writeScriptBin "mayastor-iscsiadm" ''
    #!${pkgs.stdenv.shell}
    chroot /host /usr/bin/env -i PATH="/sbin:/bin:/usr/bin" iscsiadm "$@"
  '';

  mayastorImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayadata/mayastor";
    tag = "latest";
    created = "now";
    contents = [ pkgs.busybox mayastor ];
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor" ];
    };
  };

  mayastorCSIImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayadata/mayastor-grpc";
    tag = "latest";
    created = "now";
    contents = [ pkgs.busybox mayastor mayastorIscsiadm ];
    config = {
      Entrypoint = [ "/bin/mayastor-agent" ];
      Env = [ "PATH=${env}" ];
    };
  };
}
