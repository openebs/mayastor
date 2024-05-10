{ crossSystem ? null
, img_tag ? ""
, tag ? ""
, img_org ? ""
, product_prefix ? ""
}:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/overlay.nix { inherit img_tag tag img_org product_prefix; })
    ];
    inherit crossSystem;
  };
in
pkgs
