#!/usr/bin/env bash

set -euxo pipefail

export PATH="$PATH:${HOME}/.cargo/bin"
export npm_config_jobs=$(nproc)

$(dirname "$0")/clean-cargo-tests.sh

cargo build --all
cd "$(dirname "$0")/../test/grpc"
npm install --legacy-peer-deps

sudo pkill io-engine || true

for ts in cli replica nexus rebuild; do
  ./node_modules/mocha/bin/mocha test_${ts}.js \
      --reporter ./multi_reporter.js \
      --reporter-options reporters="xunit spec",output=../../${ts}-xunit-report.xml
done
