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
set -euxo pipefail

( cd jsonrpc && cargo test )
# test dependencies
cargo build --bins --features=io-engine-testing
( cd io-engine && cargo test --features=io-engine-testing -- --test-threads=1 )
