{ sources ? import ../sources.nix }:
let
  pkgs =
    import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
in
with pkgs; rec  {
  nightly = rust-bin.nightly."2023-01-08".default;
  stable = rust-bin.stable.latest.default;
}
