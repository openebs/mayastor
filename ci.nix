{ nospdk ? false, norust ? false }:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays =
      [ (_: _: { inherit sources; }) (import ./nix/mayastor-overlay.nix) ];
  };
in
with pkgs;
let
  nospdk_moth =
    "You have requested environment without SPDK, you should provide it!";
  norust_moth =
    "You have requested environment without RUST, you should provide it!";
  channel = import ./nix/lib/rust.nix { inherit sources; };
  # python environment for test/python
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools asyncssh black ]);
in
mkShell {
  name = "mayastor-dev-shell";
  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];
  buildInputs = [
    clang_11
    cowsay
    docker
    docker-compose
    e2fsprogs
    etcd
    fio
    gdb
    git
    kubernetes-helm
    libaio
    libbsd
    libnvme
    libpcap
    libudev
    liburing
    llvmPackages_11.libclang
    meson
    ninja
    nodejs-16_x
    nvme-cli
    numactl
    openssl
    pkg-config
    pre-commit
    procps
    pytest_inputs
    python3
    utillinux
    xfsprogs
  ] ++ (if (nospdk) then [ libspdk-dev.buildInputs ] else [ libspdk-dev ])
  ++ pkgs.lib.optional (!norust) channel.stable
  ++ pkgs.lib.optional (!norust) channel.nightly;

  RUST_NIGHTLY_PATH = channel.nightly;
  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;
  SPDK_PATH = if nospdk then null else "${libspdk-dev}";

  shellHook = ''
    ${pkgs.lib.optionalString (nospdk) "cowsay ${nospdk_moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk) "echo"}
    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo 'Hint: use rustup tool.'"}
    ${pkgs.lib.optionalString (norust) "echo"}

    # SRCDIR is needed by docker-compose files as it requires absolute paths
    export SRCDIR=`pwd`
    pre-commit install
    pre-commit install --hook commit-msg
  '';
}
