{ pkgs ? import <nixpkgs> {
  # ensure that we import the mayastor-overlay
  overlays = [ (import ./nix/mayastor-overlay.nix) ];
} }:
with pkgs;

let
  rustChannel = import ./nix/lib/rust.nix {
    inherit fetchFromGitHub;
    inherit pkgs;
  };
in
  mkShell {
    inputsFrom = [ mayastor.mayastor ];
    buildInputs = [
      gptfdisk
      libiscsi.bin
      nodejs-10_x
      nvme-cli
      pre-commit
      python3
      gdb
      xfsprogs
      nodePackages.prettier
      nodePackages.jshint
      rustChannel.cargo
      rustChannel.clippy-preview
      rustChannel.rustfmt-preview
      rustChannel.rls-preview
      # TODO: Install cargo make
    ];

    LIBCLANG_PATH = mayastor.mayastor.LIBCLANG_PATH;
    PROTOC = mayastor.mayastor.PROTOC;
    PROTOC_INCLUDE = mayastor.mayastor.PROTOC_INCLUDE;
  }
