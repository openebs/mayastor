{ fetchFromGitHub, pkgs ? import <nixpkgs> }:
let
  mozilla = fetchFromGitHub {
    owner = "mozilla";
    repo = "nixpkgs-mozilla";
    rev = "e912ed483e980dfb4666ae0ed17845c4220e5e7c";
    sha256 = "08fvzb8w80bkkabc1iyhzd15f4sm7ra10jn32kfch5klgl0gj3j3";
  };

  overlay = import (builtins.toPath "${mozilla}/package-set.nix") { inherit pkgs; };
in
rec {
  nightly = overlay.rustChannelOf {
    channel = "nightly";
  };

  stable = overlay.rustChannelOf {
    channel = "stable";
  };
}
