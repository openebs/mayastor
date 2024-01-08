{ crossSystem ? null
, img_tag ? ""
, tag ? ""
, img_org ? ""
}:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/overlay.nix { inherit img_tag tag img_org; })
    ];
    inherit crossSystem;
  };
in
pkgs
