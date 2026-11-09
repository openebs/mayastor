# The pinned rust toolchain only, eg: to update the Cargo.lock
{ rust ? "stable" }:
let
  sources = import ../sources.nix;
  pkgs = import sources.nixpkgs { };
  channels = import ../../spdk-rs/nix/lib/rust.nix { inherit sources; };
in
pkgs.mkShellNoCC {
  name = "rust-shell";
  buildInputs = [ channels.${rust} ];
}
