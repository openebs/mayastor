#!/usr/bin/env sh

set -euxo pipefail

export PATH=$PATH:${HOME}/.cargo/bin

cargo build --all
cd test/grpc
npm install

for ts in cli replica nexus csi rebuild snapshot nats; do
  ./node_modules/mocha/bin/mocha test_${ts}.js \
      --reporter ./multi_reporter.js \
      --reporter-options reporters="xunit spec",output=../../${ts}-xunit-report.xml
done