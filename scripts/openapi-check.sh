#!/usr/bin/env bash

set -e

SCRIPTDIR=$(dirname "$0")
SRC="$SCRIPTDIR/../control-plane/rest"
SPECS="$SCRIPTDIR/../control-plane/rest/openapi-specs"

# Regenerate the spec only if the rest src changed
check_rest_src="no"

case "$1" in
    --changes)
        check_rest_src="yes"
        ;;
esac

if [[ $check_rest_src = "yes" ]]; then
    git diff --cached --exit-code $SRC 1>/dev/null && exit 0
fi

cargo run --bin rest -- -d --no-auth -o $SPECS

# If the spec was modified then fail the check
git diff --exit-code $SPECS
