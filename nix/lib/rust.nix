{ sources ? import ../sources.nix }:
let
  pkgs = import sources.nixpkgs { overlays = [ (import sources.nixpkgs-mozilla) ]; };
in
rec {
  nightly = pkgs.rustChannelOf { channel = "nightly"; date = "2021-04-19"; };
  stable = pkgs.rustChannelOf { channel = "stable"; };
}
