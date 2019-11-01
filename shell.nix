{ pkgs ? import <nixpkgs> {
    # ensure that we import the mayastor-overlay
    overlays = [ (import ./nix/mayastor-overlay.nix) ];
  }
}:
with pkgs;

let
  rustChannel = import ./nix/lib/rust.nix {
    inherit fetchFromGitHub;
    inherit pkgs;
  };
in
mkShell {
  inputsFrom = [ mayastor ];
  buildInputs = [
    gdb
    gptfdisk
    libiscsi.bin
    nodePackages.jshint
    nodePackages.prettier
    nodejs-10_x
    nvme-cli
    pre-commit
    python3
    rustChannel.cargo
    rustChannel.clippy-preview
    rustChannel.rls-preview
    rustChannel.rustfmt-preview
    xfsprogs
    # TODO: Install cargo make
  ];

  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;
}
