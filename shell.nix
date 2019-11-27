{ channel ? "nightly"
, pkgs ? import <nixpkgs> {

    # import the mayastor-overlay
    overlays = [ (import ./nix/mayastor-overlay.nix) ];
  }
,
}:
with pkgs;

let
  rustChannel = import ./nix/lib/rust.nix {
    inherit fetchFromGitHub;
    inherit pkgs;
  };

  libspdk = enableDebugging pkgs.libspdk;
in
mkShell {
  buildInputs = [
    figlet
    gdb
    gptfdisk
    libiscsi.bin
    nodePackages.jshint
    nodePackages.prettier
    nodejs-10_x
    nvme-cli
    pre-commit
    python3
    rustChannel.${channel}.clippy-preview
    rustChannel.${channel}.rls-preview
    rustChannel.${channel}.rust
    rustChannel.${channel}.rust-src
    rustChannel.${channel}.rustc
    rustChannel.${channel}.rustfmt-preview
    xfsprogs
  ] ++ mayastor.buildInputs;

  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;

  shellHook = ''
    figlet ${channel}
  '';
}
