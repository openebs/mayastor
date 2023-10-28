{ sources ? import ../sources.nix }:
let
  pkgs =
    import sources.nixpkgs { overlays = [ (import sources.rust-overlay) ]; };
  nightly_version = "2023-08-25";
  stable_version = "1.72.0";
in
with pkgs; rec  {
  nightly = rust-bin.nightly.${nightly_version}.default;
  stable = rust-bin.stable.${stable_version}.default;
  asan = rust-bin.nightly.${nightly_version}.default;
}
