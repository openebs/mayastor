# As soon as async becomes stable; we dont need to import the mozilla overlay
# anymore. This will greatly simplify the expression.
{ stdenv
, libaio
, libiscsi
, libspdk
, llvmPackages
, numactl
, openssl
, pkg-config
, protobuf
, rdma-core
, clang
, utillinux
, makeRustPlatform
, fetchFromGitHub
, dockerTools
, pkgs ? import <nixpkgs>
, buildType ? "release"
}:

let
  channel = import ../../lib/rust.nix {
    inherit fetchFromGitHub;
    inherit pkgs;
  };
  rustPlatform = makeRustPlatform {
    rustc = channel.rust;
    cargo = channel.cargo;
  };
in
rec {
  # An alternative approach is to build separate outputs for the workspaces:
  #
  # sidecar = rustPlatform.buildRustPackage rec {
  #   name = "mayastor-sidecar";
  #   ....
  #
  #   buildPhase = ''
  #    cargo build ${stdenv.lib.optionalString (buildType == "release") "--release"} \
  #    --target ${stdenv.hostPlatform.config} -p csi
  #    '';
  #   };
  #
  #   The downside of this is that we compile twice but maybe that is not the case
  #   if the src are fetched from github instead of the working as it is right now.

  mayastor = rustPlatform.buildRustPackage rec {
    name = "mayastor";
    cargoSha256 = "150w3paf53104vqr45z3nw2kyb08zi90ccxwf39k3rp6gsid06gr";
    version = "unstable";
    src = ../../../.;

    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";

    # these are required for building the proto files that tonic can't find otherwise.
    PROTOC = "${pkgs.protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${pkgs.protobuf}/include";

    buildInputs = [
      pkgs.clang
      libaio
      libiscsi.lib
      libspdk
      llvmPackages.libclang
      numactl
      openssl
      pkg-config
      protobuf
      rdma-core
      utillinux.dev
    ];

    doCheck = false;
    meta = { platforms = stdenv.lib.platforms.linux; };
  };

  mayastorImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayastor";
    tag = "latest";
    created = "now";
    contents = [ mayastor ];
  };

  mayastorCSIImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayastor-csi";
    tag = "latest";
    created = "now";
    contents = [ mayastor ];
  };
}
