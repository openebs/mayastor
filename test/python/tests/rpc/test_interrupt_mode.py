"""
Smoke tests for reactor interrupt mode (PR #1966).

Only meaningful when the compose services are started with
`ENABLE_INTERRUPT_MODE=true`, so every test here skips otherwise.
They cover three regressions that the existing functional tests
would not surface:

1. mode-assertion smoke   — the env var was actually honoured and
   the reactor(s) transitioned to interrupt state
2. idle CPU sanity        — the reactor sleeps in `fd_group_wait`
   instead of busy-polling when no work is pending
3. wakeup-from-sleep      — `send_future` correctly kicks the
   reactor out of `fd_group_wait` via the wakeup eventfd
"""

import os
import subprocess
import time

import pytest

from common.mayastor import container_mod, mayastor_mod

INTERRUPT_ENABLED = os.environ.get("ENABLE_INTERRUPT_MODE", "").lower() == "true"

interrupt_only = pytest.mark.skipif(
    not INTERRUPT_ENABLED,
    reason="interrupt mode not enabled (ENABLE_INTERRUPT_MODE != true)",
)


@interrupt_only
def test_reactor_state_is_interrupt(container_mod):
    """The container log should show SPDK global interrupt mode enabled
    and at least one reactor core transitioning into interrupt state.
    Catches silent regressions where the env var stops being honoured."""
    logs = str(container_mod.get("ms1").logs())
    assert "SPDK interrupt mode enabled globally" in logs, (
        "interrupt mode env var was not honoured — the global enable line is "
        "missing from io-engine logs"
    )
    assert (
        "entered interrupt mode" in logs
    ), "no reactor transitioned to interrupt state — check enter_interrupt_mode"


@interrupt_only
def test_idle_cpu_is_low(container_mod):
    """With no I/O driven, an interrupt-mode reactor should sleep in
    fd_group_wait and consume near-zero CPU. A pure busy-poll reactor
    reports ~100% per core. Use a wide margin (<50% total) to tolerate
    noise from the SPDK heartbeat poller and reactor bookkeeping."""
    ms1 = container_mod.get("ms1")

    # Let things settle: gRPC server up, pollers registered, reactor
    # should have transitioned and be blocked in fd_group_wait.
    time.sleep(5)

    # docker stats --no-stream reports a single CPU% snapshot. Ask for
    # --no-trunc so the container name matches exactly; parse the %.
    out = subprocess.check_output(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}",
            ms1.name,
        ],
        text=True,
    ).strip()
    cpu_pct = float(out.rstrip("%"))

    # 50% is a generous ceiling — interrupt mode typically lands in the
    # single digits when idle. Poll mode would be ~100% per reactor core,
    # i.e. well over 100% for the default 2-core container.
    assert cpu_pct < 50.0, (
        f"interrupt-mode container idle CPU is {cpu_pct}% — expected <50%. "
        "Reactor is likely not sleeping in fd_group_wait."
    )


@interrupt_only
def test_wakeup_from_sleep(mayastor_mod):
    """A gRPC call against a sleeping interrupt-mode reactor must return
    promptly: the request is delivered as a future, send_future signals
    the reactor's wakeup eventfd, and fd_group_wait returns so the
    future can execute. If the wakeup fd path is broken this test
    hangs or exceeds the threshold."""
    ms1 = mayastor_mod.get("ms1")

    # Give the reactor a moment to enter fd_group_wait after startup.
    time.sleep(2)

    start = time.monotonic()
    ms1.mayastor_info()  # simple gRPC round-trip, executes on init_thread
    elapsed_ms = (time.monotonic() - start) * 1000

    # 500 ms is several orders of magnitude more than a healthy wakeup
    # (typically <5 ms end-to-end). Failure here means the wakeup fd
    # wasn't registered / drained correctly and the reactor is either
    # not sleeping or not being woken.
    assert elapsed_ms < 500.0, (
        f"gRPC call took {elapsed_ms:.1f} ms against a sleeping reactor — "
        "wakeup eventfd path may be broken"
    )
