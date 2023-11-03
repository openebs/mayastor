#!/usr/bin/env bash

set -e

SCRIPTDIR=$(dirname "$0")
ROOTDIR="$SCRIPTDIR"/../
DEPLOYDIR="$ROOTDIR"/deploy

CORES=2
PROFILE=release
TAG=v1.0.9

"$SCRIPTDIR"/generate-deploy-yamls.sh -c "$CORES" -t "$TAG" "$PROFILE"

git diff --exit-code "$DEPLOYDIR" 1>/dev/null && exit 0
