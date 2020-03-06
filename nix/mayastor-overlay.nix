self: super: {
  libiscsi = super.callPackage ./pkgs/libiscsi {};
  liburing = super.callPackage ./pkgs/liburing {};
  nvme-cli = super.callPackage ./pkgs/nvme-cli {};
  nvmet-cli = super.callPackage ./pkgs/nvmet-cli {};
  libspdk = super.callPackage ./pkgs/libspdk {};
  mayastor = (super.callPackage ./pkgs/mayastor {}).mayastor;
  mayastorImage = (super.callPackage ./pkgs/mayastor {}).mayastorImage;
  mayastorCSIImage = (super.callPackage ./pkgs/mayastor {}).mayastorCSIImage;
  node-moac = (import ./../csi/moac { pkgs = super; }).package;
  node-moacImage = (import ./../csi/moac { pkgs = super; }).buildImage;
}
