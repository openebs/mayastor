#!/bin/bash
# Validate real NVMe/TCP over ffsock: custom SPDK nvmf_tgt (single core, net_tap,
# ffsock sock impl) as target; the host kernel nvme-tcp driver as initiator.
# Namespace is a 64MB malloc bdev; we connect, then write+read-verify over it.
# Entirely inside this box's netns; never touches a real NIC.
set -u

SPDK=/root/mayastore/spdk
TGT=$SPDK/build/bin/nvmf_tgt
RPC="python3 $SPDK/scripts/rpc.py"
CONF=/root/sandbox/mayastor/experiments/ff-spdk-phase2/f-stack.conf

FF_IP=10.99.0.2
KERN_IP=10.99.0.1
PORT=4420
NQN=nqn.2016-06.io.spdk:cnode1
LOG=/tmp/nvmftcp_tgt.log

export FFSOCK_CONF="$CONF"     # ffsock lazy-init reads this for F-Stack config

echo "== [nvmftcp] launching nvmf_tgt (-m 0x8, net_tap, no-pci)"
"$TGT" -m 0x8 --no-pci --env-context="--vdev=net_tap0,iface=fftap0 --in-memory" \
	> "$LOG" 2>&1 &
TGT_PID=$!

cleanup() {
	echo "== [nvmftcp] cleanup"
	nvme disconnect -n "$NQN" >/dev/null 2>&1
	kill "$TGT_PID" 2>/dev/null; wait "$TGT_PID" 2>/dev/null
}
trap cleanup EXIT

# Wait for the RPC socket.
for i in $(seq 1 30); do
	$RPC rpc_get_methods >/dev/null 2>&1 && break
	kill -0 "$TGT_PID" 2>/dev/null || { echo "!! tgt died early"; cat "$LOG"; exit 1; }
	sleep 1
done

echo "== [nvmftcp] configuring target (TCP transport, malloc bdev, subsystem, ns, ffsock listener)"
set -e
$RPC nvmf_create_transport -t TCP
$RPC bdev_malloc_create -b Malloc0 64 512
$RPC nvmf_create_subsystem "$NQN" -a -s SPDK00000000000001 -d SPDK_Controller1
$RPC nvmf_subsystem_add_ns "$NQN" Malloc0
# -S ffsock: this listener is served by the F-Stack-backed sock impl. This RPC
# triggers ffsock's lazy F-Stack bring-up on the reactor (fftap0 appears now).
$RPC nvmf_subsystem_add_listener "$NQN" -t tcp -a "$FF_IP" -s "$PORT" -f ipv4 -S ffsock
set +e

echo "== [nvmftcp] waiting for fftap0 (F-Stack net_tap port)"
for i in $(seq 1 20); do ip link show fftap0 >/dev/null 2>&1 && break; sleep 1; done
if ! ip link show fftap0 >/dev/null 2>&1; then
	echo "!! fftap0 never appeared; tgt log:"; tail -40 "$LOG"; exit 1
fi
# Distinct kernel-side MAC (net_tap MAC-collision fix), address on F-Stack subnet.
ip link set fftap0 down 2>/dev/null
ip link set fftap0 address 02:00:00:00:00:01 2>/dev/null
ip addr add "$KERN_IP/24" dev fftap0 2>/dev/null
ip link set fftap0 up
echo "== [nvmftcp] fftap0 up ($KERN_IP); target listening on $FF_IP:$PORT via ffsock"
sleep 2

echo "== [nvmftcp] initiator: nvme connect over TCP"
BEFORE=$(ls /dev/nvme*n1 2>/dev/null)
nvme connect -t tcp -a "$FF_IP" -s "$PORT" -n "$NQN" 2>&1
sleep 2
AFTER=$(ls /dev/nvme*n1 2>/dev/null)
DEV=$(comm -13 <(echo "$BEFORE" | tr ' ' '\n' | sort) <(echo "$AFTER" | tr ' ' '\n' | sort) | head -1)
if [ -z "$DEV" ]; then
	echo "!! no new nvme namespace device appeared; tgt log:"; tail -40 "$LOG"; exit 1
fi
echo "== [nvmftcp] connected; new namespace = $DEV"
nvme list 2>/dev/null | grep -E "$DEV|SPDK" || true

echo "== [nvmftcp] I/O: write random data, read back, compare (4KiB x 256 = 1MiB, O_DIRECT)"
dd if=/dev/urandom of=/tmp/nvme_w.bin bs=4k count=256 status=none
dd if=/tmp/nvme_w.bin of="$DEV" bs=4k count=256 oflag=direct status=none
sync
dd if="$DEV" of=/tmp/nvme_r.bin bs=4k count=256 iflag=direct status=none
if cmp -s /tmp/nvme_w.bin /tmp/nvme_r.bin; then
	echo "== [nvmftcp] RESULT: PASS — NVMe/TCP read/write over ffsock verified (data matches)"
	RC=0
else
	echo "== [nvmftcp] RESULT: FAIL — data mismatch on read-back"; RC=1
fi

echo "== [nvmftcp] target-side stats:"; $RPC nvmf_get_stats 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); pg=d['poll_groups'][0]; print('   poll_group', pg['name'], 'admin_qpairs', pg.get('admin_qpairs'), 'io_qpairs', pg.get('io_qpairs'))" 2>/dev/null || true
exit "$RC"
