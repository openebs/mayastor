{ nospdk ? false, norust ? false, spdk_rel ? false, asan ? false }:
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
  norust_moth =
    "You have requested environment without RUST, you should provide it!";
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
    autoconf
    automake
    clang
    cowsay
    docker
    docker-compose
    e2fsprogs
    etcd
    fio
    gdb
    git
    gnuplot
    kubernetes-helm
    libaio
    libbsd
    libnvme
    libpcap
    libunwind
    liburing
    llvmPackages.bintools
    llvmPackages.libclang
    meson
    ninja
    nodejs-16_x
    numactl
    nvme-cli
    openssl
    pkg-config
    pre-commit
    procps
    pytest_inputs
    python3
    udev
    utillinux
    xfsprogs
    yasm
  ] ++ (if (nospdk) then [ spdk.buildInputs ] else [ spdk ])
  ++ pkgs.lib.optional (!norust && asan) channel.asan
  ++ pkgs.lib.optional (!norust && !asan) channel.stable
  ++ pkgs.lib.optional (!norust) channel.nightly;

  RUST_NIGHTLY_PATH = channel.nightly;
  LIBCLANG_PATH = io-engine.LIBCLANG_PATH;
  PROTOC = io-engine.PROTOC;
  PROTOC_INCLUDE = io-engine.PROTOC_INCLUDE;
  SPDK_PATH = if nospdk then null else "${spdk}";
  FIO_SPDK = if nospdk then null else "${spdk}/fio/spdk_nvme";
  ETCD_BIN = "${etcd}/bin/etcd";

  IO_ENGINE_DIR = if asan then "target/x86_64-unknown-linux-gnu/debug" else "target/debug";

  # ASAN-related Cargo settings.
  ASAN_ENABLE = if asan then "1" else null;
  ASAN_OPTIONS = if asan then "detect_leaks=0" else null;
  RUSTFLAGS = if asan then "-Zsanitizer=address" else null;
  CARGO_BUILD_RUSTFLAGS = if asan then "-Zbuild-std" else null;
  CARGO_BUILD_TARGET = if asan then "x86_64-unknown-linux-gnu" else null;
  CARGO_PROFILE_DEV_PANIC = if asan then "unwind" else null;
  RUST_BACKTRACE = if asan then "full" else null;

  shellHook = ''
    ${pkgs.lib.optionalString (asan) "export LLVM_SYMBOLIZER_DIR=$(dirname $(realpath $(which llvm-symbolizer)))"}

    ${pkgs.lib.optionalString (asan) "echo 'AddressSanitizer is enabled, forcing nightly rustc.'"}
    ${pkgs.lib.optionalString (asan) "echo '  ASAN_ENABLE             :' $\{ASAN_ENABLE\}"}
    ${pkgs.lib.optionalString (asan) "echo '  ASAN_OPTIONS            :' $\{ASAN_OPTIONS\}"}
    ${pkgs.lib.optionalString (asan) "echo '  RUSTFLAGS               :' $\{RUSTFLAGS\}"}
    ${pkgs.lib.optionalString (asan) "echo '  CARGO_BUILD_RUSTFLAGS   :' $\{CARGO_BUILD_RUSTFLAGS\}"}
    ${pkgs.lib.optionalString (asan) "echo '  CARGO_BUILD_TARGET      :' $\{CARGO_BUILD_TARGET\}"}
    ${pkgs.lib.optionalString (asan) "echo '  CARGO_PROFILE_DEV_PANIC :' $\{CARGO_PROFILE_DEV_PANIC\}"}
    ${pkgs.lib.optionalString (asan) "echo '  RUST_BACKTRACE          :' $\{RUST_BACKTRACE\}"}
    ${pkgs.lib.optionalString (asan) "echo '  LLVM_SYMBOLIZER_DIR     :' $\{LLVM_SYMBOLIZER_DIR\}"}
    ${pkgs.lib.optionalString (asan) "echo"}

    ${pkgs.lib.optionalString (!nospdk) "echo 'SPDK version    :' $(echo $SPDK_PATH | sed 's/.*libspdk-//g')"}
    ${pkgs.lib.optionalString (!nospdk) "echo 'SPDK path       :' $SPDK_PATH"}
    ${pkgs.lib.optionalString (!nospdk) "echo 'SPDK FIO plugin :' $FIO_SPDK"}
    ${pkgs.lib.optionalString (!norust) "echo 'Rust version    :' $(rustc --version 2> /dev/null || echo '${norustc_msg}')"}
    ${pkgs.lib.optionalString (!norust) "echo 'Rust path       :' $(which rustc 2> /dev/null || echo '${norustc_msg}')"}
    echo 'I/O engine dir  :' $IO_ENGINE_DIR
    ${pkgs.lib.optionalString (nospdk) "cowsay ${nospdk_moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk) "echo"}
    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo 'Hint: use rustup tool.'"}
    ${pkgs.lib.optionalString (norust) "echo"}

    echo

    # SRCDIR is needed by docker-compose files as it requires absolute paths
    export SRCDIR=`pwd`
    pre-commit install
    pre-commit install --hook commit-msg
  '';
}
