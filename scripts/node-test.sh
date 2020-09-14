#!/usr/bin/env sh

set -euxo pipefail

export PATH=$PATH:${HOME}/.cargo/bin

cargo build --all
cd mayastor-test
npm install
./node_modules/mocha/bin/mocha test_cli.js
./node_modules/mocha/bin/mocha test_replica.js
./node_modules/mocha/bin/mocha test_nexus.js
./node_modules/mocha/bin/mocha test_csi.js
./node_modules/mocha/bin/mocha test_rebuild.js
./node_modules/mocha/bin/mocha test_snapshot.js
./node_modules/mocha/bin/mocha test_nats.js
