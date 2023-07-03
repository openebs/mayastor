#!/usr/bin/env bash

SCRIPTDIR=$(dirname "$0")
trap cleanup_handler ERR INT QUIT TERM HUP EXIT

cleanup_handler() {
  $SCRIPTDIR/clean-cargo-tests.sh || true
}

set -euxo pipefail

export PATH="$PATH:${HOME}/.cargo/bin"
export npm_config_jobs=$(nproc)

cleanup_handler

cargo build --all
cd "$(dirname "$0")/../test/grpc"
npm install --legacy-peer-deps

sudo pkill io-engine || true

for ts in cli replica nexus rebuild; do
  ./node_modules/mocha/bin/mocha test_${ts}.js \
      --reporter ./multi_reporter.js \
      --reporter-options reporters="xunit spec",output=../../${ts}-xunit-report.xml
done
