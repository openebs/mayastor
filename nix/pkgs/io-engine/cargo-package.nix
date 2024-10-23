{ stdenv
, clang
, dockerTools
, e2fsprogs
, lib
, libaio
, libbsd
, libexecinfo
, libnvme
, libspdk
, libspdk-dev
, libpcap
, udev
, liburing
, libunwind
, makeRustPlatform
, numactl
, openssl
, pkg-config
, protobuf
, sources
, xfsprogs
, utillinux
, llvmPackages
, targetPackages
, buildPackages
, targetPlatform
, versions
, systemdMinimal
, rdma-core
, cargoBuildFlags ? [ ]
, rustFlags
}:
let
  version = versions.version;
  channel = import ../../../spdk-rs/nix/lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable;
    cargo = channel.stable;
  };
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix: lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
  src_list = [
    ".cargo"
    "Cargo.lock"
    "Cargo.toml"
    "cli"
    "composer"
    "jsonrpc"
    "libnvme-rs"
    "io-engine"
    "spdk-rs"
    "sysfs"
    "utils"
  ];
  buildProps = rec {
    name = "io-engine";
    inherit version cargoBuildFlags;
    src = whitelistSource ../../../. src_list;
    LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    GIT_VERSION_LONG = "${versions.long}";
    GIT_VERSION = "${versions.tag_or_long}";

    nativeBuildInputs = [
      pkg-config
      protobuf
      llvmPackages.bintools
      llvmPackages.clang
    ];
    buildInputs = [
      libaio
      libbsd
      libexecinfo
      libnvme
      libpcap
      libunwind
      liburing
      llvmPackages.libclang
      numactl
      openssl.dev
      protobuf
      systemdMinimal.dev
      utillinux.dev
      rdma-core
    ];
    cargoLock = {
      lockFile = ../../../Cargo.lock;
    };
    doCheck = false;
    meta = { platforms = lib.platforms.linux; };
    outputs = [ "out" ];
  };
in
{
  cargoDeps = rustPlatform.importCargoLock {
    lockFile = ../../../Cargo.lock;
  };
  release = rustPlatform.buildRustPackage (buildProps // {
    cargoBuildFlags = "--bin io-engine --bin io-engine-client --bin casperf";
    buildType = "release";
    buildInputs = buildProps.buildInputs ++ [ libspdk ];
    ${if rustFlags == "" then null else "RUSTFLAGS"} = builtins.split " " rustFlags;
    SPDK_ROOT_DIR = "${libspdk}";
  });
  debug = rustPlatform.buildRustPackage (buildProps // {
    cargoBuildFlags = "--workspace --bins --exclude io-engine-bench";
    buildType = "debug";
    buildInputs = buildProps.buildInputs ++ [ libspdk-dev ];
    ${if rustFlags == "" then null else "RUSTFLAGS"} = builtins.split " " rustFlags;
    SPDK_ROOT_DIR = "${libspdk-dev}";
  });
}
