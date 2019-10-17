#!/usr/bin/env bash
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
cargo build --all

pushd mayastor && cargo test && popd
pushd mayastor-test && npm install && ./node_modules/mocha/bin/mocha test_cli.js && popd
pushd mayastor-test && ./node_modules/mocha/bin/mocha test_grpc.js && popd
pushd mayastor-test && ./node_modules/mocha/bin/mocha test_csi.js && popd
pushd mayastor-test && ./node_modules/mocha/bin/mocha test_nexus_grpc.js && popd
pushd csi/moac && npm install && ./node_modules/mocha/bin/mocha && popd

