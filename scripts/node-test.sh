#!/usr/bin/env sh
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
cargo build --all
( cd mayastor-test && npm install && ./node_modules/mocha/bin/mocha test_cli.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_replica.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_nexus.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_rebuild.js )
