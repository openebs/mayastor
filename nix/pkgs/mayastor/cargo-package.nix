{ stdenv, clang_11, dockerTools, e2fsprogs, lib, libaio, libspdk, libspdk-dev
, libudev, liburing, makeRustPlatform, numactl, openssl, pkg-config, protobuf
, sources, xfsprogs, utillinux, llvmPackages_11, targetPackages, buildPackages
, targetPlatform, version, cargoBuildFlags ? [ ] }:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable.rust;
    cargo = channel.stable.cargo;
  };
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource (path: type:
      lib.any
      (allowedPrefix: lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
      allowedPrefixes) src;
  src_list = [
    ".git"
    "Cargo.lock"
    "Cargo.toml"
    "cli"
    "composer"
    "csi"
    "devinfo"
    "jsonrpc"
    "mayastor"
    "mbus-api"
    "nvmeadm"
    "rpc"
    "spdk-sys"
    "sysfs"
  ];
  buildProps = rec {
    name = "mayastor";
    inherit version cargoBuildFlags;
    src = whitelistSource ../../../. src_list;
    LIBCLANG_PATH = "${llvmPackages_11.libclang.lib}/lib";
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    nativeBuildInputs = [ pkg-config protobuf llvmPackages_11.clang ];
    buildInputs = [
      llvmPackages_11.libclang
      protobuf
      libaio
      libudev
      liburing
      numactl
      openssl
      utillinux
    ];
    cargoLock = {
      lockFile = ../../../Cargo.lock;
      outputHashes = { };
    };
    doCheck = false;
    meta = { platforms = lib.platforms.linux; };
  };
in {
  release = rustPlatform.buildRustPackage (buildProps // {
    buildType = "release";
    buildInputs = buildProps.buildInputs ++ [ libspdk ];
    SPDK_PATH = "${libspdk}";
  });
  debug = rustPlatform.buildRustPackage (buildProps // {
    buildType = "debug";
    buildInputs = buildProps.buildInputs ++ [ libspdk-dev ];
    SPDK_PATH = "${libspdk-dev}";
  });
  # this is for an image that does not do a build of mayastor
  adhoc = stdenv.mkDerivation {
    name = "mayastor-adhoc";
    inherit version;
    src = [
      ../../../target/debug/mayastor
      ../../../target/debug/mayastor-csi
      ../../../target/debug/mayastor-client
      ../../../target/debug/jsonrpc
    ];

    buildInputs =
      [ libaio libspdk-dev liburing libudev openssl xfsprogs e2fsprogs ];

    unpackPhase = ''
      for srcFile in $src; do
         cp $srcFile $(stripHash $srcFile)
      done
    '';
    dontBuild = true;
    dontConfigure = true;
    installPhase = ''
      mkdir -p $out/bin
      install * $out/bin
    '';
  };
}
