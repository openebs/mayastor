{ stdenv
, clang_11
, dockerTools
, e2fsprogs
, lib
, libaio
, libbsd
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
, btrfs-progs
, utillinux
, llvmPackages_11
, targetPackages
, buildPackages
, targetPlatform
, versions
, cargoBuildFlags ? [ ]
}:
let
  version = versions.version;
  channel = import ../../lib/rust.nix { inherit sources; };
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
    "Cargo.lock"
    "Cargo.toml"
    "cli"
    "composer"
    "jsonrpc"
    "libnvme-rs"
    "io-engine"
    "rpc"
    "spdk-rs"
    "sysfs"
    "utils"
  ];
  buildProps = rec {
    name = "io-engine";
    inherit version cargoBuildFlags;
    src = whitelistSource ../../../. src_list;
    LIBCLANG_PATH = "${llvmPackages_11.libclang.lib}/lib";
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    GIT_VERSION_LONG = "${versions.long}";
    GIT_VERSION = "${versions.tag_or_long}";

    nativeBuildInputs = [ pkg-config protobuf llvmPackages_11.clang ];
    buildInputs = [
      llvmPackages_11.libclang
      protobuf
      libaio
      libbsd
      libnvme
      libpcap
      udev
      liburing
      numactl
      openssl
      utillinux
      libunwind
    ];
    cargoLock = {
      lockFile = ../../../Cargo.lock;
    };
    doCheck = false;
    meta = { platforms = lib.platforms.linux; };
    fixupPhase = ''
      local ms_lib_path
      local new_rpath
      echo "fixing rpaths in io-engine binaries to point to $out/lib"
      ms_lib_path=$(echo "$out/lib" | sed 's/\//\\\//g')
      new_rpath=$(patchelf --print-rpath "$out/bin/io-engine" | sed -r 's/\/build\/io-engine(\/[^:]*)+/'"$ms_lib_path"'/')
      patchelf \
          --set-interpreter "$(cat $NIX_CC/nix-support/dynamic-linker)" \
          --set-rpath "$new_rpath" \
          "$out/bin/io-engine"
    '';
  };
in
{
  release = rustPlatform.buildRustPackage (buildProps // {
    cargoBuildFlags = "--bin io-engine --bin io-engine-client";
    buildType = "release";
    buildInputs = buildProps.buildInputs ++ [ libspdk ];
    SPDK_PATH = "${libspdk}";
  });
  debug = rustPlatform.buildRustPackage (buildProps // {
    cargoBuildFlags = "--workspace --bins --exclude io-engine-bench";
    buildType = "debug";
    buildInputs = buildProps.buildInputs ++ [ libspdk-dev ];
    SPDK_PATH = "${libspdk-dev}";
  });
}
