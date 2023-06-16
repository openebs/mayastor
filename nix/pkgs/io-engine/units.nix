{ stdenv
, clang_11
, dockerTools
, e2fsprogs
, lib
, libaio
, libspdk
, libspdk-dev
, udev
, liburing
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
, pkgs
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git tag; };
  versions = {
    "version" = version;
    "long" = builtins.readFile "${versionDrv.long}";
    "tag_or_long" = builtins.readFile "${versionDrv.tag_or_long}";
  };
  project-builder = { cargoBuildFlags }: pkgs.callPackage ./cargo-package.nix { inherit versions cargoBuildFlags; };
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
