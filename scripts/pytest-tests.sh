#! /usr/bin/env bash

set -eu -o pipefail

SCRIPTDIR="$(realpath "$(dirname "$0")")"
ROOTDIR=$(readlink -f "$SCRIPTDIR/..")

function cleanup_handler()
{
  ERROR=$?
  trap - INT QUIT TERM HUP
  clean_all
  if [ $ERROR != 0 ]; then exit $ERROR; fi
}
function trap_setup()
{
  trap cleanup_handler INT QUIT TERM HUP
}

function clean_all()
{
  echo "Cleaning up all docker-compose clusters..."
  for file in $(find . -name docker-compose.yml); do
    (
      cd "$(dirname "$file")"
      echo "$(pwd)"
      docker-compose down 2>/dev/null || true
    )
  done
  echo "Done"
}

function run_tests()
{
  while read name extra
  do
    if [[ "$name" = '#'* ]]
    then
      continue
    fi
    if [ -z "$name" ]
    then
      continue
    fi
    if [ -d "$name" ]
    then
    (
      set -x
      report=$(echo "${name}-xunit-report.xml" | tr  '/' '-')
      python -m pytest --tc-file='test_config.ini' --docker-compose="$name" "$name" --junit-xml="$ROOTDIR/$report" $TEST_ARGS
    )
    elif [ -f "$name" ] || [ -f "${name%::*}" ]
    then
    (
      set -x
      base=$(dirname "$name")
      ( cd "$base"; docker-compose down 2>/dev/null || true )
      report=$(echo "$base/${name%.py}-xunit-report.xml" | tr  '/' '-')
      python -m pytest --tc-file='test_config.ini' --docker-compose="$base" "$name" --junit-xml="$ROOTDIR/$report" $TEST_ARGS
    )
    fi
  done
}

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

pushd "$SRCDIR/test/python" >/dev/null && source ./venv/bin/activate && popd >/dev/null

TEST_LIST=
TEST_ARGS=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --clean-all)
      clean_all
      ;;
    --clean-all-exit)
      clean_all
      exit 0
      ;;
    *)
      set +e
      real_1="$(realpath $1 2>/dev/null)"
      set -e
      param="$1"
      if [ -d "$real_1" ] || [ -f "$real_1" ] || [ -f "${real_1%::*}" ]; then
        param="$real_1"
      else
        TEST_ARGS="${TEST_ARGS:-}$1"
      fi
      TEST_LIST="$TEST_LIST \n$param"
      ;;
  esac
  shift
done

cd "$SRCDIR/test/python"

# Ensure we cleanup when terminated
trap_setup

if [ -n "$TEST_LIST" ]; then
  echo -e "$TEST_LIST" | run_tests
  exit 0
fi

run_tests << 'END'

tests/cli_controller
tests/replica
tests/publish
tests/rebuild

#tests/csi

tests/ana_client
tests/cli_controller
tests/replica_uuid
#tests/rpc

tests/nexus_multipath
tests/nexus_fault
tests/nexus

v1/pool
v1/replica
v1/nexus

cross-grpc-version/pool
cross-grpc-version/nexus
cross-grpc-version/rebuild
cross-grpc-version/replica

END
