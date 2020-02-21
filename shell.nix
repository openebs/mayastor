{ channel ? "nightly"
, pkgs ? import <nixpkgs> {
    # import the mayastor-overlay
    overlays = [ (import ./nix/mayastor-overlay.nix) ];
  }
}:
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
    libspdk
    nodePackages.jshint
    nodePackages.prettier
    nodejs-10_x
    nvme-cli
    pre-commit
    python3
    rustChannel.${channel}.rust
    xfsprogs
  ] ++ mayastor.buildInputs;

  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;

  shellHook = ''
    figlet ${channel}
  '';
}
