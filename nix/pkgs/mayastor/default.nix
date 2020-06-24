{ stdenv
, busybox
, clang
, dockerTools
, e2fsprogs
, fetchFromGitHub
, lib
, libaio
, libiscsi
, libspdk
, libudev
, liburing
, llvmPackages
, makeRustPlatform
, numactl
, openssl
, pkgs
, pkg-config
, protobuf
, rdma-core
, utillinux
, writeScriptBin
, xfsprogs
, sources
}:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable.rust;
    cargo = channel.stable.cargo;
  };
in
rustPlatform.buildRustPackage rec {
  name = "mayastor";
  cargoSha256 = "06zcprq4hvfpy8ikvxiqy4wy936h5ymnnkyzp6bkpqqq691xcrgf";
  version = sources.mayastor.branch;
  src = sources.mayastor;

  LIBCLANG_PATH = "${llvmPackages.libclang}/lib";

  # these are required for building the proto files that tonic can't find otherwise.
  PROTOC = "${protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${protobuf}/include";
  C_INCLUDE_PATH = "${libspdk}/include/spdk";

  nativeBuildInputs = [
    clang
    llvmPackages.libclang
    pkg-config
    protobuf
    utillinux.dev
  ];

  buildInputs = [
    libaio
    libiscsi.lib
    libspdk
    libudev
    liburing
    numactl
    openssl
    utillinux
  ];

  buildType = "release";
  verifyCargoDeps = false;

  doCheck = false;
  meta = { platforms = stdenv.lib.platforms.linux; };
}
