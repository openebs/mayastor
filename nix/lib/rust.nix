{ sources ? import ../sources.nix }:
let
  pkgs = import sources.nixpkgs { overlays = [ (import sources.nixpkgs-mozilla) ]; };
in
rec {
  nightly = pkgs.rustChannelOf {
    channel = "nightly";
  };

  stable = pkgs.rustChannelOf {
    channel = "stable";
  };
}
