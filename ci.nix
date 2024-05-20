{ rust ? "stable"
, spdk ? "develop"
, spdk-path ? null
} @ args:
let
  sources = import ./nix/sources.nix;

  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ./nix/overlay.nix { })
    ];
  };

  # python environment for test/python
  pytest_inputs = with pkgs; python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools asyncssh black ]);

  shellAttrs = import ./spdk-rs/nix/shell (args // {
    inherit sources;
    inherit pkgs;

    cfg = {
      buildInputs = with pkgs; [
        docker
        docker-compose
        e2fsprogs
        etcd
        gdb
        git
        gnuplot
        kubernetes-helm
        nodejs-16_x
        numactl
        pytest_inputs
        udev
        xfsprogs
      ];

      shellEnv = with pkgs; {
        PROTOC = io-engine.PROTOC;
        PROTOC_INCLUDE = io-engine.PROTOC_INCLUDE;
        ETCD_BIN = "${etcd}/bin/etcd";
        LVM_BINS = "${lvm2.bin}/bin";
      };

      shellHook = ''
        # SRCDIR is needed by docker-compose files as it requires absolute paths
        export SRCDIR=`pwd`

        export PATH="$PATH:$(pwd)/scripts/nix-sudo"

        export IO_ENGINE_DIR="$RUST_TARGET_DEBUG"
      '';

      shellInfoHook = ''
        echo
        echo "PROTOC          : $PROTOC"
        echo "PROTOC_INCLUDE  : $PROTOC_INCLUDE"
        echo "ETCD_BIN        : $ETCD_BIN"
        echo "LVM path        : $LVM_BINS"
        echo "I/O engine dir  : $IO_ENGINE_DIR"
      '';
    };
  });
in
  pkgs.mkShell shellAttrs // {
    name = "io-engine-dev-shell";
  }
