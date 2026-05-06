#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/..")

if ! command -v nix-sudo >/dev/null; then
  echo "Please run this from the nix-shell environment" >&2
  exit 1
fi

nix-sudo nvme disconnect-all

# Clean up ublk devices
back_dir="/tmp/io-engine-tests/"
nix-sudo ublk list -v | jq -r --arg dir "$back_dir" '
  select(.target.backing_file? // "" | startswith($dir))
  | .dev_info.dev_id
' | while read -r id; do
    echo "Deleting ublk device $id"
    nix-sudo ublk del -n "$id" --async
done

for device in $(losetup -l -J | jq -r --arg tmp_dir $back_dir '.loopdevices[]|select(."back-file" | startswith($tmp_dir)) | .name'); do
  echo "Found stale loop device: $device"

  vgs=$(nix-sudo vgs --noheadings -o vg_name --select "pv_name=$device")
  for vg in $vgs; do
      for lvpath in $(nix-sudo lvs --noheadings --select="vg_name=$vg" -o lv_path "$vg"); do
          nix-sudo dmsetup resume "$lvpath" || echo "Could not resume: $lvpath"
      done
  done

  nix-sudo $(which vgremove) -y --select="pv_name=$device" || :
  nix-sudo $(which pvremove) -y "$device" || :
  sudo losetup -d "$device" || :

  for file in $(losetup -l -J | jq -r --arg tmp_dir $back_dir --arg dev $device '.loopdevices[]|select((."back-file" | startswith($tmp_dir)) and .name == $dev) | ."back-file"'); do
    [ "$file" == "(deleted)" ] && continue;
    echo "Left stale file: $file"
  done
done

# Delete the directory too
nix-sudo rmdir --ignore-fail-on-non-empty "/tmp/io-engine-tests" 2>/dev/null

# If there was a soft rdma device created and left undeleted by nvmf rdma test,
# delete that now. Not removing rdma-rxe kernel module.
nix-sudo rdma link delete io-engine-rxe0 2>/dev/null

for c in $(docker ps -a --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
  docker kill "$c"
  docker rm "$c"
done

for n in $(docker network ls --filter "label=io.composer.test.name" --format '{{.ID}}') ; do
  docker network rm "$n" || ( sudo systemctl restart docker && docker network rm "$n" )
done

# Kill's processes running off the workspace cargo binary location
ps aux | grep "$ROOT_DIR/target" | grep -v -e sudo -e grep | awk '{ print $2 }' | xargs -I% sudo kill -9 %

sudo rm -rf /var/run/dpdk/*

exit 0
