SCRIPT_DIR="$(dirname "$0")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/..")

# Kill's processes running off the workspace cargo binary location
ps aux | grep "$ROOT_DIR/target" | grep -v sudo | head -n -1 | awk '{ print $2 }' | xargs -I% kill -9 %

exit 0