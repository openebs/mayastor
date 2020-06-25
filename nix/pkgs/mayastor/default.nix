{ stdenv
, clang
, dockerTools
, libaio
, libiscsi
, libspdk
, libudev
, liburing
, llvmPackages
, makeRustPlatform
, numactl
, openssl
, pkg-config
, protobuf
, release ? true
, sources
, utillinux
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

  buildType = if release then "release" else "debug";
  verifyCargoDeps = false;

  doCheck = false;
  meta = { platforms = stdenv.lib.platforms.linux; };
}
