#! /usr/bin/env bash

set -euxo pipefail

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

tests='test_bdd_pool'
tests+=' test_bdd_replica'
tests+=' test_bdd_nexus'
tests+=' test_nexus_publish'
tests+=' test_bdd_rebuild'
tests+=' test_bdd_csi'
tests+=' test_csi'
tests+=' test_nexus_multipath'
tests+=' test_bdd_nexus_multipath'

cd "$SRCDIR/test/python" && source ./venv/bin/activate

for ts in $tests
do
  pytest --tc-file=test_config.ini "$ts.py"
done
