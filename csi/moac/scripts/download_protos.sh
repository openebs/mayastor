#!/usr/bin/env sh

set -e

DIR="$(realpath "$(dirname "$0")")"
COMMIT=develop

if [ -n "$1" ]; then
  COMMIT="$1"
fi
if [ -d "$DIR/../proto" ]; then
  echo "Not doing anything - proto directory already exists"
  exit 0;
fi
mkdir -p "$DIR/../proto"

echo "Downloading RPC proto files from Mayastor@$COMMIT ..."
curl -o "$DIR/../proto/mayastor.proto" "https://raw.githubusercontent.com/openebs/Mayastor/$COMMIT/rpc/proto/mayastor.proto"
curl -o "$DIR/../proto/csi.proto" "https://raw.githubusercontent.com/openebs/Mayastor/$COMMIT/csi/proto/csi.proto"
echo "Done"
