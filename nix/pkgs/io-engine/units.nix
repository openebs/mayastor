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
    "version" = "${versionDrv}";
    "long" = builtins.readFile "${versionDrv.long}";
    "tag_or_long" = builtins.readFile "${versionDrv.tag_or_long}";
  };
  project-builder = { cargoBuildFlags ? [ ] }: pkgs.callPackage ./cargo-package.nix { inherit versions cargoBuildFlags rustFlags; };
  components = { build }: {
    io-engine = (project-builder { cargoBuildFlags = [ "--bin io-engine" ]; }).${build};
    io-engine-cli = (project-builder { cargoBuildFlags = [ "--bin io-engine-cli" ]; }).${build};
    casperf = (project-builder { cargoBuildFlags = [ "--bin casperf" ]; }).${build};
    custom = { cargoBuildFlags }: (project-builder { cargoBuildFlags = [ cargoBuildFlags ]; }).${build};
  };
in
{
  cargoDeps = (project-builder { }).cargoDeps;
  release = components { build = "release"; };
  debug = components { build = "debug"; };
}
