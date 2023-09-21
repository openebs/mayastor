#!/usr/bin/env bash

SCRIPTDIR="$(realpath "$(dirname "$0")")"

cleanup_handler() {
  ERROR=$?
  "$SCRIPTDIR"/clean-cargo-tests.sh || true
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}

cleanup_handler
trap cleanup_handler INT QUIT TERM HUP EXIT

set -euxo pipefail

export PATH="$PATH:${HOME}/.cargo/bin"
export npm_config_jobs=$(nproc)

cargo build --all
cd "$(dirname "$0")/../test/grpc"
npm install --legacy-peer-deps

sudo pkill io-engine || true

for ts in cli replica nexus rebuild; do
  ./node_modules/mocha/bin/_mocha test_${ts}.js \
      --reporter ./multi_reporter.js \
      --reporter-options reporters="xunit spec",output=../../${ts}-xunit-report.xml
done
