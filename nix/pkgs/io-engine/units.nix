{ stdenv
, clang_11
, dockerTools
, e2fsprogs
, lib
, libaio
, libspdk
, libspdk-dev
, libudev
, liburing
, makeRustPlatform
, numactl
, openssl
, pkg-config
, protobuf
, sources
, xfsprogs
, utillinux
, llvmPackages_11
, targetPackages
, buildPackages
, targetPlatform
, pkgs
}:
let
  version = (builtins.fromTOML (builtins.readFile ../../../io-engine/Cargo.toml)).package.version;
  project-builder = { cargoBuildFlags }: pkgs.callPackage ./cargo-package.nix { inherit version cargoBuildFlags; };
  components = { build }: {
    io-engine = (project-builder { cargoBuildFlags = [ "--bin io-engine" ]; }).${build};
    io-engine-cli = (project-builder { cargoBuildFlags = [ "--bin io-engine-cli" ]; }).${build};
    custom = { cargoBuildFlags }: (project-builder { cargoBuildFlags = [ cargoBuildFlags ]; }).${build};
  };
in
{
  release = components { build = "release"; };
  debug = components { build = "debug"; };
}
