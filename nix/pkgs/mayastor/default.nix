# As soon as async becomes stable; we dont need to import the mozilla overlay
# anymore. This will greatly simplyfy the expression.
#
# runtime dependencies are determined by elf magic on the build artifacts
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
, pkgs ? import <nixpkgs>
}:
let
  mozilla = fetchFromGitHub {
    owner = "mozilla";
    repo = "nixpkgs-mozilla";
    rev = "ac8e9d7bbda8fb5e45cae20c5b7e44c52da3ac0c";
    sha256 = "1irlkqc0jdkxdfznq7r52ycnf0kcvvrz416qc7346xhmilrx2gy6";
  };

  overlay = import "${mozilla}/package-set.nix" { inherit pkgs; };
  channel = overlay.rustChannelOf {
    date = "2019-10-14";
    channel = "nightly";
  };

  nightly = makeRustPlatform {
    rustc = channel.rust;
    cargo = channel.cargo;
  };

in
nightly.buildRustPackage rec {
  pname = "mayastor";
  cargoSha256 = "04pbdjd7vbdfraw2n8pn5jfv7kl8sd99wic8yj8my3w4rj55nvhn";
  version = "unstable";
  src = ../../../.;

  # crates that run bindgen (blkid) require these to be set
  propagatedBuildInputs = [ clang ];
  LIBCLANG_PATH = "${pkgs.llvmPackages.libclang}/lib";

  # these are requirerd for building the proto files that tonic can't find otherwise.
  PROTOC = "${pkgs.protobuf}/bin/protoc";
  PROTOC_INCLUDE = "${pkgs.protobuf}/include";

  buildInputs = [
    libaio
    libiscsi.lib
    libspdk
    llvmPackages.libclang
    numactl
    openssl
    pkg-config
    protobuf
    rdma-core
    utillinux
  ];

  doCheck = false;
  meta = { platforms = stdenv.lib.platforms.linux; };
}
