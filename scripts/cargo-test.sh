#!/usr/bin/env bash

cleanup_handler() {
  for c in $(docker ps -a --format '{{.ID}}') ; do
    docker kill "$c" || true
    docker rm "$c" || true
  done
}

trap cleanup_handler ERR INT QUIT TERM HUP

set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
( cd jsonrpc && cargo test )
( cd mayastor && cargo test -- --test-threads=1 )
( cd nvmeadm && cargo test )
