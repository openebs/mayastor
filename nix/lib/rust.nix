{ sources ? import ../sources.nix }:
let
  pkgs =
    import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
in
with pkgs; rec  {
  nightly = rust-bin.nightly."2023-08-25".default;
  stable = rust-bin.stable."1.72.0".default;
}
