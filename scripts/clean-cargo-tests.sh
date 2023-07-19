SCRIPT_DIR="$(dirname "$0")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/..")

sudo nvme disconnect-all

for c in $(docker ps -a --filter "label=io.mayastor.test.name" --format '{{.ID}}') ; do
  docker kill "$c"
  docker rm "$c"
done

for n in $(docker network ls --filter "label=io.mayastor.test.name" --format '{{.ID}}') ; do
  docker network rm "$n" || ( sudo systemctl restart docker && docker network rm "$n" )
done

# Kill's processes running off the workspace cargo binary location
ps aux | grep "$ROOT_DIR/target" | grep -v -e sudo -e grep | awk '{ print $2 }' | xargs -I% sudo kill -9 %

exit 0

