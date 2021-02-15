{ stdenv
, clang
, git
, lib
, llvmPackages
, makeRustPlatform
, openssl
, pkg-config
, protobuf
, sources
, pkgs
}:
let
  channel = import ../../lib/rust.nix { inherit sources; };
  rustPlatform = makeRustPlatform {
    rustc = channel.stable.rust;
    cargo = channel.stable.cargo;
  };
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix:
            lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
  version_drv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${version_drv}";
  buildProps = rec {
    name = "control-plane";
    #cargoSha256 = "0000000000000000000000000000000000000000000000000000";
    cargoSha256 = "1iqmrl8qm8nw1hg219kdyxd1zk9c58p1avymjis3snxnlagafx37";
    inherit version;
    src = whitelistSource ../../../. (pkgs.callPackage ../mayastor { }).src_list;
    cargoBuildFlags = [ "-p mbus_api" "-p agents" "-p rest" "-p operators" ];

    LIBCLANG_PATH = "${llvmPackages.libclang}/lib";
    PROTOC = "${protobuf}/bin/protoc";
    PROTOC_INCLUDE = "${protobuf}/include";

    nativeBuildInputs = [
      clang
      pkg-config
    ];
    buildInputs = [
      llvmPackages.libclang
      openssl
    ];
    verifyCargoDeps = false;
    doCheck = false;
    meta = { platforms = stdenv.lib.platforms.linux; };
  };
in
{
  release = rustPlatform.buildRustPackage
    (buildProps // {
      buildType = "release";
      buildInputs = buildProps.buildInputs;
    });
  debug = rustPlatform.buildRustPackage
    (buildProps // {
      buildType = "debug";
      buildInputs = buildProps.buildInputs;
    });
}
