{ nospdk ? false
, norust ? false
}:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/mayastor-overlay.nix)
    ];
  };
in
with pkgs;
let
  nospdk_moth = "You have requested environment without SPDK, you should provide it!";
  norust_moth = "You have requested environment without rust, you should provide it!";
  channel = import ./nix/lib/rust.nix { inherit sources; };
in
mkShell {

  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];
  buildInputs = [
    clang
    cowsay
    docker
    docker-compose
    e2fsprogs
    envsubst # for e2e tests
    etcd
    fio
    gdb
    git
    go
    gptfdisk
    kind
    kubectl
    kubernetes-helm
    libaio
    libiscsi
    libudev
    liburing
    llvmPackages.libclang
    meson
    nats-server
    ninja
    nodejs-16_x
    numactl
    nvme-cli
    nvmet-cli
    openssl
    pkg-config
    pre-commit
    procps
    python3
    utillinux
    xfsprogs
  ]
  ++ (if (nospdk) then [ libspdk-dev.buildInputs ] else [ libspdk-dev ])
  ++ pkgs.lib.optional (!norust) channel.nightly.rust;

  LIBCLANG_PATH = mayastor.LIBCLANG_PATH;
  PROTOC = mayastor.PROTOC;
  PROTOC_INCLUDE = mayastor.PROTOC_INCLUDE;
  SPDK_PATH = if nospdk then null else "${libspdk-dev}";

  shellHook = ''
    ${pkgs.lib.optionalString (nospdk) "cowsay ${nospdk_moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk)
      ''export RUSTFLAGS="-C link-args=-Wl,-rpath,$(pwd)/spdk-sys/spdk"''}
    ${pkgs.lib.optionalString (nospdk) "echo"}
    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo 'Hint: use rustup tool.'"}
    ${pkgs.lib.optionalString (norust) "echo"}
    pre-commit install
    pre-commit install --hook commit-msg
  '';
}
