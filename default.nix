let
  nixpkgs = (import ./nix/lib/nixPackages.nix) { };
  pkgs = import nixpkgs {
    system = "x86_64-linux";
    config = { };
    overlays = [ (import ./nix/mayastor-overlay.nix) ];
  };
in
pkgs
