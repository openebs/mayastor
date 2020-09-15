#!/usr/bin/env sh

set -euxo pipefail

cd csi/moac
npm install
npm run prepare
npm run compile

./node_modules/mocha/bin/mocha test/index.js \
    --reporter test/multi_reporter.js \
    --reporter-options reporters="xunit spec",output=../../moac-xunit-report.xml
