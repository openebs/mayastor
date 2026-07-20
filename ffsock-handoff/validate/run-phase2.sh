#!/bin/bash
# Run the Phase 2 harness (SPDK owns EAL+loop; F-Stack embedded via ffsock) over
# a net_tap vdev and drive an echo test end-to-end. Never touches a real NIC.
set -u
PORT="${PORT:-4420}"
FF_IP="10.99.0.2"
KERN_IP="10.99.0.1"
BIN="${BIN:-./phase2}"
CONF="${CONF:-./f-stack.conf}"

echo "== [phase2] launching: ${BIN} --conf ${CONF} -- ${PORT}"
"${BIN}" --conf "${CONF}" -- "${PORT}" > /tmp/phase2.log 2>&1 &
PID=$!
cleanup() { kill "${PID}" 2>/dev/null; wait "${PID}" 2>/dev/null; }
trap cleanup EXIT

echo "== [phase2] waiting for fftap0 (DPDK net_tap under SPDK's EAL)..."
for i in $(seq 1 40); do
	ip link show fftap0 >/dev/null 2>&1 && break
	if ! kill -0 "${PID}" 2>/dev/null; then
		echo "!! [phase2] process died before fftap0 appeared. Log:"; cat /tmp/phase2.log; exit 1
	fi
	sleep 1
done
if ! ip link show fftap0 >/dev/null 2>&1; then
	echo "!! [phase2] FAIL: fftap0 never appeared. Log:"; cat /tmp/phase2.log; exit 1
fi

# net_tap gives the kernel fftap0 the SAME MAC as F-Stack's DPDK port -> kernel
# rejects F-Stack's ARP reply ("No route to host"). Give the kernel side a
# distinct MAC first (same fix as the spike).
ip link set fftap0 down 2>/dev/null
ip link set fftap0 address 02:00:00:00:00:01 2>/dev/null
ip addr add "${KERN_IP}/24" dev fftap0 2>/dev/null
ip link set fftap0 up
echo "== [phase2] fftap0 up (${KERN_IP}); F-Stack owns ${FF_IP}"
sleep 2

echo "== [phase2] TEST: echo to F-Stack ${FF_IP}:${PORT} via the SPDK sock layer"
PAYLOAD="hello-ffsock-phase2"
RESP="$(printf '%s\n' "${PAYLOAD}" | timeout 8 nc -w 3 ${FF_IP} ${PORT} 2>/dev/null)"
echo "== [phase2] sent   : ${PAYLOAD}"
echo "== [phase2] echoed : ${RESP:-<nothing>}"

if [ "${RESP}" = "${PAYLOAD}" ]; then
	echo "== [phase2] RESULT: PASS — round-trip via ffsock while SPDK owns EAL+reactor and pumps F-Stack (ff_run_once)"
	RC=0
else
	echo "== [phase2] RESULT: FAIL — no/incorrect echo. Log tail:"; tail -30 /tmp/phase2.log
	RC=1
fi
sleep 1
exit "${RC}"
