#!/usr/bin/env bash

set -euo pipefail

SCRIPTDIR=$(dirname "$0")


# Decouple JS files according to the sub-project that they belong to.
MOAC_FILES=
MS_TEST_FILES=

for path in "$@"; do
	rel_path=`echo $path | sed 's,csi/moac/,,'`
	if [ "$rel_path" != "$path" ]; then
		MOAC_FILES="$MOAC_FILES $rel_path"
	else
		rel_path=`echo $path | sed 's,test/grpc/,,'`
		if [ "$rel_path" != "$path" ]; then
			MS_TEST_FILES="$MS_TEST_FILES $rel_path"
		fi
	fi
done

# Run semistandard check for each subproject
if [ -n "$MOAC_FILES" ]; then
	( cd $SCRIPTDIR/../csi/moac && npx semistandard --fix $MOAC_FILES )
fi

if [ -n "$MS_TEST_FILES" ]; then
	( cd $SCRIPTDIR/../test/grpc && npx semistandard --fix $MS_TEST_FILES )
fi
