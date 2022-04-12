self: super: {

  fio = super.callPackage ./pkgs/fio { };
  images = super.callPackage ./pkgs/images { };
  libnvme = super.callPackage ./pkgs/libnvme { };
  libspdk = (super.callPackage ./pkgs/libspdk { }).release;
  libspdk-dev = (super.callPackage ./pkgs/libspdk { }).debug;
  io-engine = (super.callPackage ./pkgs/io-engine { }).release;
  io-engine-adhoc = (super.callPackage ./pkgs/io-engine { }).adhoc;
  io-engine-dev = (super.callPackage ./pkgs/io-engine { }).debug;
  mkContainerEnv = super.callPackage ./lib/mkContainerEnv.nix { };
  ms-buildenv = super.callPackage ./pkgs/ms-buildenv { };
  nvmet-cli = super.callPackage ./pkgs/nvmet-cli { };
  units = (super.callPackage ./pkgs/io-engine/units.nix { });
}
