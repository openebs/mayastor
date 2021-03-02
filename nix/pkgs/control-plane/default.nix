{ stdenv
, git
, lib
, pkgs
}:
let
  project-builder = pkgs.callPackage ../control-plane/cargo-project.nix { };
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  agent = { name, src }: stdenv.mkDerivation {
    inherit src;
    name = "${name}-${version}";
    installPhase = ''
      mkdir -p $out/bin
      cp $src/bin/${name} $out/bin/${name}-agent
    '';
  };
  components = { src }: {
    core = agent { inherit src; name = "core"; };
    rest = agent { inherit src; name = "rest"; };
  };
in
{
  release = components { src = project-builder.release; };
  debug = components { src = project-builder.debug; };
}
