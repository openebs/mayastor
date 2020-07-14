#!/usr/bin/env sh
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
cargo build --all
./scripts/cargo-test.sh
./scripts/node-test.sh
