{ stdenv
, clang_11
, dockerTools
, e2fsprogs
, lib
, libaio
, libiscsi
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
  version = (builtins.fromTOML (builtins.readFile ../../../mayastor/Cargo.toml)).package.version;
  project-builder = { cargoBuildFlags }: pkgs.callPackage ./cargo-package.nix { inherit version cargoBuildFlags; };
  components = { build }: {
    mayastor = (project-builder { cargoBuildFlags = [ "--bin mayastor" ]; }).${build};
    mayastor-cli = (project-builder { cargoBuildFlags = [ "--bin mayastor-cli" ]; }).${build};
    custom = { cargoBuildFlags }: (project-builder { cargoBuildFlags = [ cargoBuildFlags ]; }).${build};
  };
in
{
  release = components { build = "release"; };
  debug = components { build = "debug"; };
}
