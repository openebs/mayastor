#!/usr/bin/env bash

SCRIPTDIR=$(dirname "$0")

cleanup_handler() {
  for c in $(docker ps -a --filter "label=io.mayastor.test.name" --format '{{.ID}}') ; do
    docker kill "$c" || true
    docker rm "$c" || true
  done

  for n in $(docker network ls --filter "label=io.mayastor.test.name" --format '{{.ID}}') ; do
    docker network rm "$n" || true
  done
}

trap cleanup_handler ERR INT QUIT TERM HUP

echo "running cargo-test..."
echo "rustc version:"
rustc --version

set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
( cd jsonrpc && cargo test )
# test dependencies
cargo build --bins
( cd io-engine && cargo test -- --test-threads=1 )
