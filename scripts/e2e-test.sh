#!/usr/bin/env sh

set -e

SCRIPTDIR=$(dirname "$(realpath "$0")")
REGISTRY=$1
TESTS="install"

# TODO: Add proper argument parser
if [ -z "$REGISTRY" ]; then
  echo "Missing parameter registry"
  exit 1
fi

test_failed=
export e2e_docker_registry="$REGISTRY"
export e2e_pool_device=/dev/nvme1n1

for dir in $TESTS; do
  cd "$SCRIPTDIR/../mayastor-test/e2e/$dir"
  if ! go test; then
    test_failed=1
    break
  fi
done

# must always run uninstall test in order to clean up the cluster
cd "$SCRIPTDIR/../mayastor-test/e2e/uninstall"
go test

if [ -n "$test_failed" ]; then
  exit 1
fi

echo "All tests have passed"
exit 0