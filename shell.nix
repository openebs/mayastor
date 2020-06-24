{ nospdk ? false }:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/mayastor-overlay.nix)
    ];
  };
in
with pkgs;
let
  moth = "You have requested environment without SPDK, you should provide it!";
in
mkShell {

  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];
  buildInputs = [
    cowsay
    fio
    gdb
    gptfdisk
    libiscsi.bin
    nats-server
    nodejs-12_x
    nvme-cli
    pre-commit
    python3
  ] ++ pkgs.lib.optionals (!nospdk) mayastor.buildInputs
  ++ pkgs.lib.optionals (nospdk) [
    clang
    cunit
    libudev.dev
    libunwind
    llvmPackages.libclang
    pkg-config
  ] ++ pkgs.lib.optionals (nospdk) libspdk.buildInputs;
  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;

  C_INCLUDE_PATH = if nospdk then "" else "${libspdk}/include/spdk";
  shellHook = ''
    ${pkgs.lib.optionalString (nospdk) "cowsay ${moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk)
      ''export RUSTFLAGS="-C link-args=-Wl,-rpath,$(pwd)/spdk-sys/spdk"''}
    pre-commit install
  '';
}
