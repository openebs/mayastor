#! /usr/bin/env bash

set -euxo pipefail
cd "$(dirname ${BASH_SOURCE[0]})"

# Example of how to bringup the test cluster and build the images in parallel,
# then run the tests.

pushd setup
  ./bringup-cluster.sh &
popd
../../scripts/release.sh --skip-publish &

for job in $(jobs -p); do
  wait $job
done

# Now that everything up and built, push the images...
../../scripts/release.sh --skip-build --alias-tag "ci" --registry "172.18.8.101:30291"

# ... and install mayastor.
pushd install
  go test
popd

