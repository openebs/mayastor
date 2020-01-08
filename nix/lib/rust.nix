{ fetchFromGitHub, pkgs ? import <nixpkgs> }:
let
  mozilla = fetchFromGitHub {
    owner = "mozilla";
    repo = "nixpkgs-mozilla";
    rev = "ac8e9d7bbda8fb5e45cae20c5b7e44c52da3ac0c";
    sha256 = "1irlkqc0jdkxdfznq7r52ycnf0kcvvrz416qc7346xhmilrx2gy6";
  };

  overlay = import (builtins.toPath "${mozilla}/package-set.nix") { inherit pkgs; };
in
rec {
  nightly = overlay.rustChannelOf {
    date = "2019-12-19";
    channel = "nightly";
  };

  stable = overlay.rustChannelOf {
    channel = "stable";
  };
}
