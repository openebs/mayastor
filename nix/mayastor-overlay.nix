self: super: {
  libiscsi = super.callPackage ./pkgs/libiscsi { };
  nvme-cli = super.callPackage ./pkgs/nvme-cli { };
  libspdk = super.callPackage ./pkgs/libspdk { };
  node-moac = ( import ./../csi/moac { pkgs=super; } ).package;
}
