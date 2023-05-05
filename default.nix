{ crossSystem ? null
, img_tag ? ""
, tag ? ""
}:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/overlay.nix { inherit img_tag tag; })
    ];
    inherit crossSystem;
  };
in
pkgs
