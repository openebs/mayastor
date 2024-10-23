{ stdenv
, lib
, pkgs
, git
, tag
, sourcer
, rustFlags
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git tag sourcer; };
  versions = {
    "version" = builtins.readFile "${versionDrv}";
    "long" = builtins.readFile "${versionDrv.long}";
    "tag_or_long" = builtins.readFile "${versionDrv.tag_or_long}";
  };
  project-builder = pkgs.callPackage ./cargo-package.nix { inherit versions rustFlags; };
in
{
  release = project-builder.release;
  debug = project-builder.debug;
  adhoc = project-builder.adhoc;
}
