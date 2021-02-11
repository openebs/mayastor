#!/usr/bin/env bash

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

set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
# test dependencies
cargo build --bins
for test in composer agents rest ctrlp-tests; do
    cargo test -p ${test} -- --test-threads=1
done
