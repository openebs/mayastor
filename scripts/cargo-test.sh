#!/usr/bin/env sh
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
cargo build -p cli
( cd jsonrpc && cargo test )
( cd mayastor && cargo test -- --test-threads=1 )
( cd nvmeadm && cargo test )
