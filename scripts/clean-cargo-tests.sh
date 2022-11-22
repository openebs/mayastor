SCRIPT_DIR="$(dirname "$0")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/..")

# Kill's processes running off the workspace cargo binary location
ps aux | grep "$ROOT_DIR/target" | grep -v -e sudo -e grep | awk '{ print $2 }' | xargs -I% sudo kill -9 %

exit 0
