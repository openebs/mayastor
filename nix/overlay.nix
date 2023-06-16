{ img_tag ? "", tag ? "" }:
self: super: {
  btrfs-progs = super.callPackage ./pkgs/btrfs-progs { };
  fio = super.callPackage ./pkgs/fio { };
  images = super.callPackage ./pkgs/images { inherit img_tag; };
  libnvme = super.callPackage ./pkgs/libnvme { };
  libspdk = (super.callPackage ./pkgs/libspdk { with-fio = false; }).release;
  libspdk-fio = (super.callPackage ./pkgs/libspdk { with-fio = true; multi-outputs = true; }).release;
  libspdk-dev = (super.callPackage ./pkgs/libspdk { with-fio = true; }).debug;
  io-engine = (super.callPackage ./pkgs/io-engine { inherit tag; }).release;
  io-engine-adhoc = (super.callPackage ./pkgs/io-engine { inherit tag; }).adhoc;
  io-engine-dev = (super.callPackage ./pkgs/io-engine { inherit tag; }).debug;
  mkContainerEnv = super.callPackage ./lib/mkContainerEnv.nix { };
  ms-buildenv = super.callPackage ./pkgs/ms-buildenv { };
  nvme-cli = super.callPackage ./pkgs/nvme-cli { };
  nvmet-cli = super.callPackage ./pkgs/nvmet-cli { };
  units = (super.callPackage ./pkgs/io-engine/units.nix { });
}
