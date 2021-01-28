#!/usr/bin/env bash

set -eu

SCRIPTDIR=$(dirname "$(realpath "$0")")
# new tests should be added before the replica_pod_remove test
#TESTS="install basic_volume_io csi replica rebuild node_disconnect/replica_pod_remove uninstall"
TESTS="install basic_volume_io csi uninstall"
DEVICE=
REGISTRY=
TAG=
TESTDIR=$(realpath "$SCRIPTDIR/../test/e2e")
REPORTSDIR=$(realpath "$SCRIPTDIR/..")
GENERATE_LOGS=0
ON_FAIL="continue"

help() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --device <path>           Device path to use for storage pools.
  --registry <host[:port]>  Registry to pull the mayastor images from.
  --tag <name>              Docker image tag of mayastor images (default "ci")
  --tests <list of tests>   Lists of tests to run, delimited by spaces (default: "$TESTS")
        Note: the last 2 tests should be (if they are to be run)
             node_disconnect/replica_pod_remove uninstall
  --reportsdir <path>       Path to use for junit xml test reports (default: repo root)
  --logs                    Generate logs and cluster state dump at the end of successful test run,
                            prior to uninstall.
  --onfail <stop|continue>  On fail, stop immediately or continue default($ON_FAIL)
                            Behaviour for "continue" only differs if uninstall is in the list of tests (the default).
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
      ;;
    -r|--registry)
      shift
      REGISTRY=$1
      ;;
    -t|--tag)
      shift
      TAG=$1
      ;;
    -T|--tests)
      shift
      TESTS="$1"
      ;;
    -R|--reportsdir)
      shift
      REPORTSDIR="$1"
      ;;
    -h|--help)
      help
      exit 0
      ;;
    -l|--logs)
      GENERATE_LOGS=1
      ;;
    --onfail)
        shift
        case $1 in
            continue)
                ON_FAIL=$1
                ;;
            stop)
                ON_FAIL=$1
                ;;
            *)
                help
                exit 2
        esac
        ;;
    *)
      echo "Unknown option: $1"
      help
      exit 1
      ;;
  esac
  shift
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

export e2e_reports_dir="$REPORTSDIR"
if [ ! -d "$e2e_reports_dir" ] ; then
    echo "Reports directory $e2e_reports_dir does not exist"
    exit 1
fi

test_failed=0

# Run go test in directory specified as $1 (relative path)
function runGoTest {
    cd "$TESTDIR"
    echo "Running go test in $PWD/\"$1\""
    if [ -z "$1" ] || [ ! -d "$1" ]; then
        echo "Unable to locate test directory  $PWD/\"$1\""
        return 1
    fi

    cd "$1"
    if ! go test -v . -ginkgo.v -ginkgo.progress -timeout 0; then
        GENERATE_LOGS=1
        return 1
    fi

    return 0
}

# Check if $2 is in $1
contains() {
    [[ $1 =~ (^|[[:space:]])$2($|[[:space:]]) ]] && return 0  || return 1
}

echo "Environment:"
echo "    e2e_pool_device=$e2e_pool_device"
echo "    e2e_image_tag=$e2e_image_tag"
echo "    e2e_docker_registry=$e2e_docker_registry"
echo "    e2e_reports_dir=$e2e_reports_dir"

echo "list of tests: $TESTS"
for dir in $TESTS; do
  # defer uninstall till after other tests have been run.
  if [ "$dir" != "uninstall" ] ;  then
      if ! runGoTest "$dir" ; then
          test_failed=1
          break
      fi

      if ! ("$SCRIPTDIR"/e2e_check_pod_restarts.sh) ; then
          test_failed=1
          GENERATE_LOGS=1
          break
      fi

  fi
done

if [ "$GENERATE_LOGS" -ne 0 ]; then
    if ! "$SCRIPTDIR"/e2e-cluster-dump.sh ; then
        # ignore failures in the dump script
        :
    fi
fi

if [ "$test_failed" -ne 0 ] && [ "$ON_FAIL" == "stop" ]; then
    exit 3
fi

# Always run uninstall test if specified
if contains "$TESTS" "uninstall" ; then
    if ! runGoTest "uninstall" ; then
        test_failed=1
        if ! "$SCRIPTDIR"/e2e-cluster-dump.sh --clusteronly ; then
            # ignore failures in the dump script
            :
        fi
    fi
fi

if [ "$test_failed" -ne 0 ]; then
    echo "At least one test has FAILED!"
    exit 1
fi

echo "All tests have PASSED!"
exit 0
