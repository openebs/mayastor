{ stdenv
, clang_11
, dockerTools
, e2fsprogs
, lib
, libaio
, libbsd
, libspdk
, libspdk-dev
, libpcap
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
, git
, tag
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git tag; };
  versions = {
    "version" = builtins.readFile "${versionDrv}";
    "long" = builtins.readFile "${versionDrv.long}";
    "tag_or_long" = builtins.readFile "${versionDrv.tag_or_long}";
  };
  project-builder = pkgs.callPackage ./cargo-package.nix { inherit versions; };
in
{
  release = project-builder.release;
  debug = project-builder.debug;
  adhoc = project-builder.adhoc;
}
