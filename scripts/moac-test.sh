#!/usr/bin/env sh

set -euxo pipefail

cd csi/moac
npm install
npm run prepare
npm run compile
# TODO: Add XML output for jenkins
./node_modules/mocha/bin/mocha test/index.js
