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
  channel = import ./nix/lib/rust.nix { inherit sources; };
in
mkShell {

  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];
  buildInputs = [
    #    nats-server
    bash
    channel.stable.rust
    clang
    cowsay
    e2fsprogs
    fio
    gdb
    gptfdisk
    libaio
    libiscsi
    libiscsi.bin
    libudev
    llvmPackages.libclang
    nodejs-12_x
    nvme-cli
    openssl
    pkg-config
    pre-commit
    python3
    utillinux
    xfsprogs
  ] ++ pkgs.lib.optionals (nospdk) libspdk.buildInputs
  ++ pkgs.lib.optional (!nospdk) libspdk;
  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;

  SPDK_PATH = if nospdk then null else "${libspdk}";
  shellHook = ''
    ${pkgs.lib.optionalString (nospdk) "cowsay ${moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk)
      ''export RUSTFLAGS="-C link-args=-Wl,-rpath,$(pwd)/spdk-sys/spdk"''}
    pre-commit install
  '';
}
