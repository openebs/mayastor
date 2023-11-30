#! /usr/bin/env bash

set -eu -o pipefail

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
      python -m pytest --tc-file='test_config.ini' --docker-compose="$name" "$name"
    )
    elif [ -f "$name" ] || [ -f "${name%::*}" ]
    then
    (
      set -x
      base=$(dirname "$name")
      ( cd "$base"; docker-compose down 2>/dev/null || true )
      python -m pytest --tc-file='test_config.ini' --docker-compose="$base" "$name"
    )
    fi
  done
}

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

cd "$SRCDIR/test/python" && source ./venv/bin/activate

TEST_LIST=
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
      TEST_LIST="$TEST_LIST \n$1"
      ;;
  esac
  shift
done

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
