{ stdenv
, clang
, dockerTools
, e2fsprogs
, git
, lib
, libaio
, libiscsi
, libspdk
, libspdk-dev
, libudev
, liburing
, llvmPackages
, makeRustPlatform
, numactl
, openssl
, pkg-config
, protobuf
, sources
, xfsprogs
, utillinux
, rustup
, docker-compose
}:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable.rust;
    cargo = channel.stable.cargo;
  };
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix:
            lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
  version_drv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${version_drv}";
  src_list = [
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
    "control-plane"
    "composer"
  ];
  buildProps = rec {
    name = "mayastor";
    #cargoSha256 = "0000000000000000000000000000000000000000000000000000";
    cargoSha256 = "1ynd6fmdr89f0g9vqsbz2rfl6ld23qv92lqcma5m4xcyhblbv5g0";
    inherit version;
    src = whitelistSource ../../../. src_list;
    LIBCLANG_PATH = "${llvmPackages.libclang}/lib";
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    # Before editing dependencies, consider:
    # https://nixos.org/manual/nixpkgs/stable/#ssec-cross-dependency-implementation
    # https://nixos.org/manual/nixpkgs/stable/#ssec-stdenv-dependencies
    basePackages = [
    ];
    nativeBuildInputs = [
      clang
      pkg-config
    ];
    buildInputs = [
      llvmPackages.libclang
      protobuf
      libaio
      libiscsi.lib
      libudev
      liburing
      numactl
      openssl
      utillinux
    ];
    verifyCargoDeps = false;
    doCheck = false;
    meta = { platforms = stdenv.lib.platforms.linux; };
  };
in
{
  inherit src_list;
  release = rustPlatform.buildRustPackage
    (buildProps // {
      buildType = "release";
      buildInputs = buildProps.buildInputs ++ [ libspdk ];
      SPDK_PATH = "${libspdk}";
    });
  debug = rustPlatform.buildRustPackage
    (buildProps // {
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

    buildInputs = [
      libaio
      libiscsi.lib
      libspdk-dev
      liburing
      libudev
      openssl
      xfsprogs
      e2fsprogs
    ];

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
