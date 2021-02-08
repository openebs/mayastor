{
    crossSystem ? null
}:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/mayastor-overlay.nix)
    ];
    inherit crossSystem;
  };
in
pkgs
