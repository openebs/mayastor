self: super: {
  libiscsi = super.callPackage ./pkgs/libiscsi {};
  nvme-cli = super.callPackage ./pkgs/nvme-cli {};
  libspdk = super.callPackage ./pkgs/libspdk {};
  mayastor = super.callPackage ./pkgs/mayastor {};
  k9s = super.callPackage ./pkgs/k9s {};
  stern = super.callPackage ./pkgs/stern {};

  node-moac = (import ./../csi/moac { pkgs = super; }).package;
}
