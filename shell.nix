{ channel ? "nightly" }:
let
  nixpkgs = (import ./nix/lib/nixPackages.nix) { };
  pkgs = import nixpkgs {
    config = { };
    overlays = [ (import ./nix/mayastor-overlay.nix) ];
  };
in
with pkgs;
let
  rustChannel = import ./nix/lib/rust.nix {
    inherit fetchFromGitHub;
    inherit pkgs;
  };
  libspdk = pkgs.libspdk.override { enableDebug = true; };
in
mkShell {
  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];

  buildInputs = [
    figlet
    fio
    gdb
    gptfdisk
    libiscsi.bin
    nats-server
    nodejs-12_x
    nvme-cli
    pre-commit
    python3
    rustChannel.${channel}.rust
  ] ++ mayastor.buildInputs;

  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;

  # to avoid clobbering the top-level include dir
  # with SPDK private header files, we need have put
  # the headers elsewhere. (files are always stored in
  # /bin, /include etc)

  # XXX: we can also not set this and change the paths
  # in wrapper.h? this only effects our bindings

  C_INCLUDE_PATH = "${libspdk}/include/spdk";

  shellHook = ''
    pre-commit install
    figlet ${channel}
  '';
}
