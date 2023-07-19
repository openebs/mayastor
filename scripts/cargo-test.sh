#!/usr/bin/env bash

SCRIPTDIR="$(realpath "$(dirname "$0")")"

cleanup_handler() {
  ERROR=$?
  "$SCRIPTDIR"/clean-cargo-tests.sh || true
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}

echo "running cargo-test..."
echo "rustc version:"
rustc --version

leanup_handler
trap cleanup_handler INT QUIT TERM HUP EXIT

set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
( cd jsonrpc && cargo test )
# test dependencies
cargo build --bins
( cd mayastor && cargo test -- --test-threads=1 )
( cd nvmeadm && cargo test )
