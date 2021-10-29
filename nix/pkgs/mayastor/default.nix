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
  version = (builtins.fromTOML (builtins.readFile ../../../mayastor/Cargo.toml)).package.version;
  project-builder = pkgs.callPackage ./cargo-package.nix { inherit version; };
in
{
  release = project-builder.release;
  debug = project-builder.debug;
  adhoc = project-builder.adhoc;
  cov = project-builder.cov;
}
