#!/usr/bin/env bash

SCRIPTDIR="$(realpath "$(dirname "$0")")"

cleanup_handler() {
  ERROR=$?
  "$SCRIPTDIR"/clean-cargo-tests.sh || true
  trap '' EXIT
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}

echo "running cargo-test..."
echo "rustc version:"
rustc --version

cleanup_handler
trap cleanup_handler INT QUIT TERM HUP EXIT

export PATH=$PATH:${HOME}/.cargo/bin
set -euo pipefail

# Warn if rdma-rxe and nvme-rdme kernel modules are not
# available. Absence of rdma-rxe can be ignored on hardware
# RDMA setups.
if ! lsmod | grep -q rdma_rxe; then
  echo "Warning: rdma_rxe kernel module is not loaded. Please load it for rdma tests to work."
fi

if ! lsmod | grep -q nvme_rdma; then
  echo "Warning: nvme_rdma kernel module is not loaded. Please load it for rdma tests to work."
fi

if ! "$SCRIPTDIR"/nvme-conf.sh --check; then
  echo "Warning: nvme configuration may not be valid, this can cause some test issues"
  exit 1
fi

( cd jsonrpc && cargo test )
# test dependencies
cargo build --bins --features=io-engine-testing
( cd io-engine && cargo test --features=io-engine-testing -- --test-threads=1 )
