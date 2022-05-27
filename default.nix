{ crossSystem ? null
}:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/io-engine-overlay.nix)
    ];
    inherit crossSystem;
  };
in
pkgs
