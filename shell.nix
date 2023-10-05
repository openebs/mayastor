{ nospdk ? false, spdk_rel ? false }:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays =
      [ (_: _: { inherit sources; }) (import ./nix/overlay.nix { }) ];
  };
in
with pkgs;
let
  nospdk_moth =
    "You have requested environment without SPDK, you should provide it!";
  norustc_msg = "no rustc, use rustup tool to install it";
  channel = import ./nix/lib/rust.nix { inherit sources; };
  # python environment for test/python
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools asyncssh black ]);
  spdk = if (!spdk_rel) then libspdk-dev else libspdk;
in
mkShell {
  name = "io-engine-dev-shell";
  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];
  buildInputs = [
    clang_11
    cowsay
    etcd
    fio
    libaio
    libbsd
    libnvme
    libpcap
    udev
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
    gnuplot
    xfsprogs
    libunwind
    autoconf
    automake
    yasm
  ] ++ (if (nospdk) then [ spdk.buildInputs ] else [ spdk ]);

  LIBCLANG_PATH = io-engine.LIBCLANG_PATH;
  PROTOC = io-engine.PROTOC;
  PROTOC_INCLUDE = io-engine.PROTOC_INCLUDE;
  SPDK_PATH = if nospdk then null else "${spdk}";
  FIO_SPDK = if nospdk then null else "${spdk}/fio/spdk_nvme";
  ETCD_BIN = "${etcd}/bin/etcd";
  ETCDCTL_API = "3";

  shellHook = ''
    ${pkgs.lib.optionalString (!nospdk) "echo 'SPDK version    :' $(echo $SPDK_PATH | sed 's/.*libspdk-//g')"}
    ${pkgs.lib.optionalString (!nospdk) "echo 'SPDK path       :' $SPDK_PATH"}
    ${pkgs.lib.optionalString (!nospdk) "echo 'SPDK FIO plugin :' $FIO_SPDK"}
    echo 'Rust version    :' $(rustc --version 2> /dev/null || echo '${norustc_msg}')
    echo 'Rust path       :' $(which rustc 2> /dev/null || echo '${norustc_msg}')
    ${pkgs.lib.optionalString (nospdk) "cowsay ${nospdk_moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk) "echo"}

    echo

    # SRCDIR is needed by docker-compose files as it requires absolute paths
    export SRCDIR=`pwd`
    pre-commit install
    pre-commit install --hook commit-msg
  '';
}
