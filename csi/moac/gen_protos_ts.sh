#!/bin/sh

SCRIPTDIR="$(realpath "$(dirname "$0")")"

# Directory to write generated code to (.js and .d.ts files)
OUT_DIR="$SCRIPTDIR/proto/generated"
if [ ! -d "$OUT_DIR" ]; then
    mkdir -p "$OUT_DIR"
fi

"$SCRIPTDIR"/node_modules/.bin/pbjs --keep-case -t static-module "$SCRIPTDIR"/proto/persistence.proto -o "$OUT_DIR"/persistence.js
"$SCRIPTDIR"/node_modules/.bin/pbts -o "$OUT_DIR"/persistence.d.ts "$OUT_DIR"/persistence.js
