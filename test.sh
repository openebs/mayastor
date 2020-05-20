#!/usr/bin/env bash
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
cargo build --all
( cd jsonrpc && cargo test )
( cd mayastor && cargo test -- --test-threads=1 )
( cd mayastor-test && npm install && ./node_modules/mocha/bin/mocha test_cli.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_replica.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_csi_iscsi.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_csi_nbd.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_csi.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_nexus.js )
( cd mayastor-test && ./node_modules/mocha/bin/mocha test_rebuild.js )
( cd nvmeadm && cargo test )
