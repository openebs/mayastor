{ sources ? import ../sources.nix }:
let
  pkgs = import sources.nixpkgs { overlays = [ (import sources.nixpkgs-mozilla) ]; };
in
rec {
  nightly = pkgs.rustChannelOf { channel = "nightly"; date = "2020-06-21"; };
  stable = pkgs.rustChannelOf { channel = "stable"; };
}
