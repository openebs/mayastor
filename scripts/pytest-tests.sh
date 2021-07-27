#! /usr/bin/env bash

set -euxo pipefail

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

tests='test_bdd_pool test_bdd_replica test_bdd_nexus test_nexus_publish test_bdd_rebuild test_bdd_csi test_csi'

cd "$SRCDIR/test/python" && source ./venv/bin/activate

for ts in $tests
do
  pytest --tc-file=test_config.ini "$ts.py"
done
