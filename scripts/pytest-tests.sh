#! /usr/bin/env bash

set -eu -o pipefail

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
    fi
  done
}

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

cd "$SRCDIR/test/python" && source ./venv/bin/activate

run_tests << 'END'

tests/replica
tests/publish
tests/rebuild

tests/csi

tests/ana_client
tests/cli_controller
tests/replica_uuid

# tests/rpc

tests/nexus_multipath
tests/nexus

END
