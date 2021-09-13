#! /usr/bin/env bash

set -eu -o pipefail

function run_tests()
{
  while read name extra
  do
    case "$name" in
      test_*)
        if [ -f "$name.py" ]
        then
        (
          set -x
          pytest --tc-file=test_config.ini "$name.py"
        )
        fi
      ;;
    esac
  done
}

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

cd "$SRCDIR/test/python" && source ./venv/bin/activate

run_tests << 'END'

test_bdd_pool
test_bdd_replica
test_bdd_nexus
test_nexus_publish
test_bdd_rebuild
test_bdd_csi
test_csi

test_ana_client
test_cli_controller
test_replica_uuid
# test_rpc

test_bdd_nexus_multipath
test_nexus_multipath

test_multi_nexus
test_nexus
test_null_nexus

END
