#!/usr/bin/env bash

SCRIPTDIR=$(dirname "$0")

cleanup_handler() {
  $SCRIPTDIR/clean-cargo-tests.sh || true
}

trap cleanup_handler ERR INT QUIT TERM HUP EXIT

echo "running cargo-test..."
echo "rustc version:"
rustc --version

cleanup_handler

set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
( cd jsonrpc && cargo test )
# test dependencies
cargo build --bins --features=io-engine-testing
( cd io-engine && cargo test --features=io-engine-testing -- --test-threads=1 )
