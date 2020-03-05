# As soon as async becomes stable; we dont need to import the mozilla overlay
# anymore. This will greatly simplify the expression.
{ stdenv
, e2fsprogs
, libaio
, libiscsi
, libspdk
, liburing
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
, writeScriptBin
, pkgs ? import <nixpkgs>
}:

let
  channel = import ../../lib/rust.nix {
    inherit fetchFromGitHub;
    inherit pkgs;
  };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable.rust;
    cargo = channel.stable.cargo;
  };
in
rec {

  mayastor = rustPlatform.buildRustPackage rec {
    name = "mayastor";
    cargoSha256 = "06wwywn4fpvaaivrmbzc70b87x1vbv692xz1yn64wvpwlngh0dvx";
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
      liburing
      llvmPackages.libclang
      numactl
      openssl
      pkg-config
      protobuf
      rdma-core
      utillinux
      xfsprogs
    ];

    doCheck = false;
    meta = { platforms = stdenv.lib.platforms.linux; };
  };

  # mkfs.* and mount commands need to be in the PATH for mayastor-agent to work.
  # For NIX these run-time dependencies are hidden so it ignores them.
  # The solution is to create shell script wrapper setting the PATH and calling
  # mayastor-agent, which makes it apparent for the NIX.
  mayastorAgent = writeScriptBin "mayastor-agent" ''
    #!${pkgs.stdenv.shell}
    PATH=${pkgs.e2fsprogs}/bin:${pkgs.utillinux}/bin:${pkgs.xfsprogs}/bin:$PATH ${mayastor}/bin/mayastor-agent "$@"
  '';

  mayastorImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayadata/mayastor";
    tag = "latest";
    created = "now";
    contents = [ pkgs.busybox mayastor ];
    config = {
      Entrypoint = [ "/bin/mayastor" ];
    };
  };

  mayastorCSIImage = pkgs.dockerTools.buildLayeredImage {
    name = "mayadata/mayastor-grpc";
    tag = "latest";
    created = "now";
    contents = [ pkgs.busybox mayastorAgent ];
    config = {
      Entrypoint = [ "/bin/mayastor-agent" ];
      ExposedPorts = { "10124/tcp" = {}; };
    };
  };
}
