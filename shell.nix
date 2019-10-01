{ pkgs ? import <nixpkgs> {
  # see ensure that that we import the mayastor-overlay
  overlays = [ (import ./nix/mayastor-overlay.nix) ];
} }:
with pkgs;

stdenv.mkDerivation rec {
  name = "MayaStor";

  buildInputs = [
    bash
    binutils
    gcc
    git
    gptfdisk
    libaio
    libiscsi
    libuuid
    llvmPackages.libclang
    nasm
    nodejs-10_x
    numactl
    nvme-cli
    openiscsi
    openssl
    pkgconfig
    protobuf
    python3
    rdma-core
    gdb
    utillinux
    xfsprogs
  ];

  propagatedBuildInputs = [ clang ];
  hardeningDisable = [ "all" ];

  LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";
  PROTOC = "${pkgs.protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${pkgs.protobuf}/include";
  shellHook = ''
    echo
    echo
    echo "Please note: using the hosts RUST environment, when running pure"
    echo "install a rust environment with async support."
    echo
    echo
    export RUSTFLAGS="-C link-args=-Wl,-rpath=$(pwd)/spdk-sys/build"
  '';

}
