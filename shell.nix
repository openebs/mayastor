{ nospdk ? false }:
let
  nixpkgs = (import ./nix/lib/nixPackages.nix) { };
  pkgs = import nixpkgs {
    config = { };
    overlays = [ (import ./nix/mayastor-overlay.nix) ];
  };
in with pkgs;
let
  libspdk = pkgs.libspdk.override { enableDebug = true; };
  moth =
    "You have requested environment without SPDK, you should provide it!";
in mkShell {

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
    nodePackages.semistandard
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
    ] ++ libspdk.buildInputs;

  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;

  shellHook = ''
    ${pkgs.lib.optionalString (nospdk) "cowsay ${moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk)
    ''export RUSTFLAGS="-C link-args=-Wl,-rpath,$(pwd)/spdk-sys/spdk"''}
    pre-commit install
  '';
}
