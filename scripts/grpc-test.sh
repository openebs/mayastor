#!/usr/bin/env bash

SCRIPTDIR="$(realpath "$(dirname "$0")")"

cleanup_handler() {
  ERROR=$?
  "$SCRIPTDIR"/clean-cargo-tests.sh || true
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}

cleanup_handler
trap cleanup_handler INT QUIT TERM HUP EXIT

set -euxo pipefail

export PATH="$PATH:${HOME}/.cargo/bin"
export npm_config_jobs=$(nproc)

# The gRPC tests manage their own TLS: the suites spawn the io-engine with a
# freshly generated server certificate and the clients verify it. Clear any
# ambient TLS settings so they can't conflict with that per-test configuration.
unset GRPC_TLS GRPC_AUTO_TLS GRPC_TLS_CERT_FILE GRPC_TLS_KEY_FILE GRPC_TLS_CA_FILE

cargo build --bins --features=io-engine-testing
cd "$(dirname "$0")/../test/grpc"
npm install --legacy-peer-deps

sudo pkill io-engine || true

if ! "$SCRIPTDIR"/nvme-conf.sh --check; then
  echo "Warning: nvme configuration may not be valid, this can cause some test issues"
  exit 1
fi

for ts in cli replica nexus rebuild; do
  ./node_modules/mocha/bin/_mocha test_${ts}.js \
      --reporter ./multi_reporter.js \
      --reporter-options reporters="xunit spec",output=../../${ts}-xunit-report.xml
done
