#!/usr/bin/env bash

set -euo pipefail

SCRIPTDIR=$(dirname "$0")


# Filter only the files that belong to grpc tests
MS_TEST_FILES=

for path in "$@"; do
	rel_path=`echo $path | sed 's,test/grpc/,,'`
	if [ "$rel_path" != "$path" ]; then
		MS_TEST_FILES="$MS_TEST_FILES $rel_path"
	fi
done

# Run semistandard check
if [ -n "$MS_TEST_FILES" ]; then
	( cd $SCRIPTDIR/../test/grpc && npx semistandard --fix $MS_TEST_FILES )
fi
