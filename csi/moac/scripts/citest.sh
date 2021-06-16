#!/usr/bin/env bash

set -euxo pipefail

DIR="$(realpath "$(dirname "$0")")"
export npm_config_jobs=$(nproc)

( cd "$DIR/.." && npm install && npm run compile )

( cd "$DIR/.." && ./node_modules/.bin/mocha test/index.ts \
    --reporter test/multi_reporter.js \
    --reporter-options reporters="xunit spec",output=../../moac-xunit-report.xml )
