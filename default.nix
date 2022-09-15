{ crossSystem ? null
, img_tag ? ""
}:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/mayastor-overlay.nix { inherit img_tag; })
    ];
    inherit crossSystem;
  };
in
pkgs
