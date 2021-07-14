{ sources ? import ../sources.nix }:
let
  pkgs =
    import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
in
with pkgs; rec  {
  nightly = rust-bin.nightly."2021-06-22".default;
  stable = rust-bin.stable.latest.default;
}
