self: super: {
  lcov = super.lcov.overrideAttrs (oldAttrs: rec {
    patches = [
      (super.fetchpatch {
        url =
          "https://github.com/linux-test-project/lcov/commit/ebfeb3e179e450c69c3532f98cd5ea1fbf6ccba7.patch";
        sha256 = "0dalkqbjb6a4vp1lcsxd39dpn5fzdf7ihsjbiviq285s15nxdj1j";
      })
      (super.fetchpatch {
        url =
          "https://github.com/linux-test-project/lcov/commit/75fbae1cfc5027f818a0bb865bf6f96fab3202da.patch";
        sha256 = "0v1hn0511dxqbf50ppwasc6vmg0m6rns7ydbdy2rdbn0j7gxw30x";
      })
    ];

  });

  terraform-provider-lxd = super.callPackage ./pkgs/terraform-provider-lxd {};
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
