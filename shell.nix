{ channel ? "nightly"
, nospdk ? false
}:
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
  moth = "You have an environment with no SPDK avaiable, you should provide it!";
in
mkShell {
  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];

  buildInputs = [
    cowsay
    figlet
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
    #    rustChannel.${channel}.rust
  ] ++ pkgs.lib.optionals (!nospdk) mayastor.buildInputs
  ++ pkgs.lib.optionals (nospdk) libspdk.buildInputs;

  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;

  shellHook = ''
    ${pkgs.lib.optionalString (nospdk) "cowsay ${moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    pre-commit install
    figlet ${channel}
  '';
}
