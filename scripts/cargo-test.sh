#!/usr/bin/env sh
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
( cd jsonrpc && cargo test )
# test dependencies
cargo build --bins
for test in composer mayastor services rest; do
    ( cd ${test} && cargo test -- --test-threads=1 )
done
( cd nvmeadm && cargo test )
