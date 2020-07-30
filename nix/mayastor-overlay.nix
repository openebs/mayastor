self: super: {
  libiscsi = super.callPackage ./pkgs/libiscsi { };
  liburing = super.callPackage ./pkgs/liburing { };
  nvmet-cli = super.callPackage ./pkgs/nvmet-cli { };
  libspdk = super.callPackage ./pkgs/libspdk { };
  mayastor = (super.callPackage ./pkgs/mayastor { }).release;
  mayastor-dev = (super.callPackage ./pkgs/mayastor { }).debug;
  mayastor-adhoc = (super.callPackage ./pkgs/mayastor { }).adhoc;
  moac = (import ./../csi/moac { pkgs = super; }).package;
  images = super.callPackage ./pkgs/images { };

  ms-buildenv = super.callPackage ./pkgs/ms-buildenv { };
  mkContainerEnv = super.callPackage ./lib/mkContainerEnv.nix { };
}
