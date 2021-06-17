#!/usr/bin/env bash

set -euxo pipefail

cd "$(dirname "$0")/../csi/moac"
export npm_config_jobs=$(nproc)
npm install
npm run prepare
npm run compile

./node_modules/mocha/bin/mocha test/index.ts \
    --reporter test/multi_reporter.js \
    --reporter-options reporters="xunit spec",output=../../moac-xunit-report.xml
