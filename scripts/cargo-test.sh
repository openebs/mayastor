#!/usr/bin/env sh
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
( cd jsonrpc && cargo test )
( cd mayastor && cargo test -- --test-threads=1 )
( cd nvmeadm && cargo test )
