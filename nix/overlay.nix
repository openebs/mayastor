{ img_tag ? "", tag ? "", img_org ? "", product_prefix ? "", rustFlags ? "" }:
let
  config = import ./config.nix;
  img_prefix = if product_prefix == "" then config.product_prefix else product_prefix;
in
self: super: rec {
  sourcer = super.callPackage ./lib/sourcer.nix { };
  images = super.callPackage ./pkgs/images { inherit img_tag img_org img_prefix; };
  io-engine = (super.callPackage ./pkgs/io-engine { inherit tag sourcer rustFlags; }).release;
  io-engine-adhoc = (super.callPackage ./pkgs/io-engine { inherit tag rustFlags; }).adhoc;
  io-engine-dev = (super.callPackage ./pkgs/io-engine { inherit tag rustFlags; }).debug;
  mkContainerEnv = super.callPackage ./lib/mkContainerEnv.nix { };
  ms-buildenv = super.callPackage ./pkgs/ms-buildenv { };
  nvmet-cli = super.callPackage ./pkgs/nvmet-cli { };
  units = (super.callPackage ./pkgs/io-engine/units.nix { inherit tag sourcer; });
}
  // (import ../spdk-rs/nix/overlay.nix { } self super)
