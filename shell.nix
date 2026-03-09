{ rust ? (let v = builtins.getEnv "NIX_RUST"; in if v == "" then "stable" else v)
, spdk ? (let v = builtins.getEnv "NIX_SPDK"; in if v == "" then "develop" else v)
, spdk-path ? (let v = builtins.getEnv "NIX_SPDK_PATH"; in if v == "" then null else v)
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

  nix-file = "\\$" + "{workspaceFolder}/shell.nix";

  shellAttrs = import ./spdk-rs/nix/shell {
    inherit rust;
    inherit spdk;
    inherit spdk-path;
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
        nodejs-slim
        numactl
        pytest_inputs
        udev
        libnvme
        nvme-cli
        xfsprogs
        nixpkgs-fmt
        ublksrv
      ];

      shellEnv = with pkgs; {
        PROTOC = io-engine.PROTOC;
        PROTOC_INCLUDE = io-engine.PROTOC_INCLUDE;
        ETCD_BIN = "${etcd}/bin/etcd";
        LVM_BINS = "${lvm2.bin}/bin";
        NVME = "${nvme-cli}/bin/nvme";

        # Dummy values in case environment does not have /etc/nvme
        NVME_HOSTID = "03f79caf-dc58-475a-a111-bf0b75214a51";
        NVME_HOSTNQN = "nqn.2014-08.org.nvmexpress:uuid:03f79caf-dc58-475a-a111-bf0b75214a51";

        # Env vars to allow for better integration with code editors which use a nix environment selector
        NIX_RUST = rust;
        NIX_SPDK = spdk;
        NIX_SPDK_PATH = toString spdk-path;
      };

      shellHook = ''
        # SRCDIR is needed by docker-compose files as it requires absolute paths
        export SRCDIR=`pwd`

        export PATH="$PATH:$(pwd)/scripts/nix-sudo"

        export IO_ENGINE_DIR="$RUST_TARGET_DEBUG"

        # Prevent Rust tooling to fallback to potentially incompatible host clang compiler
        export CLANG_PATH="$NIX_CC_FOR_TARGET/bin/clang"

        cat > "$SRCDIR/.vscode/settings.json" <<EOF
        {
            "nixEnvSelector.args": "--argstr rust ${rust} --argstr spdk ${spdk} --argstr spdk-path $(realpath ${toString spdk-path})",
            "nixEnvSelector.nixFile": "${toString nix-file}"
        }
        EOF
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
  };
in
pkgs.mkShell shellAttrs // {
  name = "io-engine-dev-shell";
}
