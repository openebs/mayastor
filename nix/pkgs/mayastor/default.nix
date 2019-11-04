# As soon as async becomes stable; we dont need to import the mozilla overlay
# anymore. This will greatly simplify the expression.
{ stdenv
, e2fsprogs
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
, xfsprogs
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
    cargoSha256 = "1b5d7ji3dvk127ghycda4gy3c8pnkavw6wv32gwq7ixb45ah119v";
    version = "unstable";
    src = ../../../.;

    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";

    # these are required for building the proto files that tonic can't find otherwise.
    PROTOC = "${pkgs.protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${pkgs.protobuf}/include";

    buildInputs = [
      clang
      e2fsprogs
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
      xfsprogs
    ];

    doCheck = false;
    meta = { platforms = stdenv.lib.platforms.linux; };
  };

  mayastorImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayadata/mayastor";
    tag = "latest";
    created = "now";
    contents = [ pkgs.bash pkgs.coreutils mayastor ];
    config = {
      Entrypoint = [ "/bin/mayastor" ];
    };
  };

  mayastorCSIImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayadata/mayastor-grpc";
    tag = "latest";
    created = "now";
    contents = [ pkgs.bash pkgs.coreutils mayastor ];
    config = {
      Entrypoint = [ "/bin/mayastor-agent" ];
      ExposedPorts = { "10124/tcp" = {}; };
    };
  };
}
