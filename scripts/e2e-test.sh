#!/usr/bin/env bash

set -eux

SCRIPTDIR=$(dirname "$(realpath "$0")")
TESTS=${e2e_tests:-"install basic_volume_io csi uninstall"}
DEVICE=
REGISTRY=
TAG=
TESTDIR=$(realpath "$SCRIPTDIR/../test/e2e")

help() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --device <path>           Device path to use for storage pools.
  --registry <host[:port]>  Registry to pull the mayastor images from.
  --tag <name>              Docker image tag of mayastor images (default "ci")

Examples:
  $0 --registry 127.0.0.1:5000 --tag a80ce0c
EOF
}

# Parse arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d|--device)
      shift
      DEVICE=$1
      shift
      ;;
    -r|--registry)
      shift
      REGISTRY=$1
      shift
      ;;
    -t|--tag)
      shift
      TAG=$1
      shift
      ;;
    -h|--help)
      help
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      help
      exit 1
      ;;
  esac
done

if [ -z "$DEVICE" ]; then
  echo "Device for storage pools must be specified"
  help
  exit 1
fi
export e2e_pool_device=$DEVICE
if [ -n "$TAG" ]; then
  export e2e_image_tag="$TAG"
fi
if [ -n "$REGISTRY" ]; then
  export e2e_docker_registry="$REGISTRY"
fi

test_failed=

# Run go test in directory specified as $1 (relative path)
function runGoTest {
    echo "Run go test in $PWD/\"$1\""
    if [ -z "$1" ] || [ ! -d "$1" ]; then
        return 1
    fi

    cd "$1"
    if ! go test -v . -ginkgo.v -ginkgo.progress -timeout 0; then
        return 1
    fi

    return 0
}

for dir in $TESTS; do
  cd "$TESTDIR"
  case "$dir" in
      uninstall)
          # defer till after all tests have been run.
          ;;
      csi)
        # TODO move the csi-e2e tests to a subdirectory under e2e
        # issues with using the same go.mod file prevents this at the moment.
        cd "../csi-e2e/"
        if ! ./runtest.sh ; then
            test_failed=1
            break
        fi
        ;;
      *)
        if ! runGoTest "$dir" ; then
            test_failed=1
            break
        fi
        ;;
  esac
done

for dir in $TESTS; do
  cd "$TESTDIR"
  case "$dir" in
      uninstall)
        echo "Uninstalling mayastor....."
        if ! runGoTest "$dir" ; then
            test_failed=1
        fi
        ;;
      *)
        ;;
   esac
done

if [ -n "$test_failed" ]; then
    echo "At least one test has FAILED!"
  exit 1
fi

echo "All tests have PASSED!"
exit 0
