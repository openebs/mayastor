# Interrupt Mode

## Overview

Interrupt mode is an opt-in operating mode for Mayastor's SPDK reactors that replaces
busy-polling with epoll-based event waiting. When idle, each slave reactor blocks in
`spdk_fd_group_wait()` instead of spinning on `spdk_thread_poll()`, eliminating nearly
all CPU consumption in the absence of I/O activity.

Enable it with:

```text
--enable-interrupt-mode          (CLI flag)
ENABLE_INTERRUPT_MODE=true       (environment variable)
```

> **Status:** Experimental. Validated only with NVMe-oF TCP targets.

---

## Background: Mayastor reactors vs. SPDK reactors

SPDK ships its own reactor framework in `lib/event/reactor.c`. That framework owns both
the polling loop *and* the interrupt-mode implementation; applications built on top of it
get interrupt support for free.

Mayastor does not use `lib/event/reactor.c`. It implements its own `Reactor` type
(`io-engine/src/core/reactor.rs`) because it needs to:

* interleave Rust async futures with SPDK thread polling,
* run the management (master) core under Tokio rather than on a plain DPDK loop, and
* route cross-core futures without going through SPDK's message FFI.

This means Mayastor must implement interrupt mode itself, using the lower-level
`spdk_fd_group` and `spdk_thread_set_interrupt_mode` APIs that SPDK exposes for exactly
this purpose, but wiring them up independently of SPDK's own reactor.

---

## Components

### `spdk_interrupt_mode_enable()`

Called once during `Reactors::init()`, **before** `spdk_thread_lib_init_ext()`. It arms
SPDK's internal bookkeeping so that individual threads can later be flipped to interrupt
mode. This call is unconditionally fatal on failure: silently falling back to poll mode
would leave the system in a mixed state and has historically caused hard-to-diagnose
deadlocks.

### Per-reactor `FdGroup`

Each `Reactor` owns a `spdk_fd_group` (wrapped as `spdk_rs::FdGroup`). It acts as the
single wait point for the reactor's OS thread. All SPDK thread fd_groups are *nested*
into it so that any event on any thread wakes the one blocking `fd_group_wait()` call.

### Wakeup `eventfd`

A non-blocking `eventfd` is registered in the reactor's `FdGroup` with type
`SPDK_FD_TYPE_EVENTFD`. The EVENTFD type makes `fd_group_wait()` drain the counter
automatically; without this, a single `send_future()` write would leave the fd permanently
readable and spin the reactor.

Two code paths write to this eventfd:

* `Reactor::send_future()` — wakes the target reactor when a Rust future is cross-posted to it.
* `Reactors::schedule()` — wakes a slave reactor that is sleeping with an empty thread list
  when a new SPDK thread is scheduled onto it.

---

## Lifecycle

### Initialisation (startup, `Reactors::init`)

```rust
spdk_interrupt_mode_enable()          ← must precede spdk_thread_lib_init_ext
spdk_thread_lib_init_ext(...)
for each core:
    FdGroup::create()
    eventfd(EFD_NONBLOCK | EFD_CLOEXEC)
    fgrp.add_with_fd_type(efd, SPDK_FD_TYPE_EVENTFD)
```

### Entering interrupt mode (`Reactor::enter_interrupt_mode`)

Called once per reactor, after its SPDK threads have been created:

**Phase 1 — nest every thread's fd_group:**
Each SPDK thread has its own `spdk_fd_group`; get it with
`spdk_thread_get_interrupt_fd_group()` and call `spdk_fd_group_nest()` to link it under
the reactor's fd_group. If *any* nest fails, all already-nested groups are unnested and
the reactor stays in poll mode. A partially-nested reactor would silently drop events on
the un-nested threads.

**Phase 2 — flip each thread to interrupt mode:**
`spdk_thread_set_interrupt_mode(true)` must be called while the thread is current
(`spdk_set_thread`). This transitions the reactor's `flags` to `ReactorState::Interrupt`.

### Slave reactor poll loop (`poll_reactor` in `ReactorState::Interrupt`)

Slave reactors run `poll_reactor()` on a dedicated OS thread (pinned by DPDK):

```rust
enter_interrupt_mode()
loop:
    fd_group_wait(-1)        ← blocks until any I/O or wakeup event
    init_thread.set_current()
    receive_futures()        ← drain cross-core Rust future channel
    run_futures()
    add_incoming()           ← integrate newly scheduled SPDK threads
```

`fd_group_wait()` dispatches events directly to SPDK's interrupt callbacks (the poller
functions) for each nested thread. There is no separate per-thread `spdk_thread_poll()`
call: the interrupt path inside `spdk_thread_poll` is triggered *by* the nested fd_group
events during `fd_group_wait`.

The init thread is restored as `current` before running Rust futures because gRPC
handlers and other management code require `spdk_thread_is_app_thread()` to return true.

### Master core

In the production binary, the master core calls `poll_reactor()` directly after launching
the Tokio runtime on a separate thread. This means the master behaves identically to
slave cores in interrupt mode: it enters `ReactorState::Interrupt`, calls
`enter_interrupt_mode()`, and then blocks in `fd_group_wait(-1)` until events arrive.

### Dynamic thread scheduling (`add_incoming`)

When `Reactors::schedule()` places a new SPDK thread onto a reactor that is already in
`ReactorState::Interrupt`, `add_incoming()` nests the new thread's fd_group into the
reactor's fd_group. If nesting fails, the reactor falls back to `leave_interrupt_mode()`
for the whole core rather than leaving it partially nested.

### Shutdown (`exit_interrupt_mode` / `leave_interrupt_mode`)

On `ReactorState::Shutdown`, the reactor calls `exit_interrupt_mode()`:

```rust
for thread in threads():
    spdk_thread_set_interrupt_mode(false)   ← flip back to poll mode
    fgrp.unnest(thread_fgrp)
state → ReactorState::Running
```

---

## Key invariants

| Invariant | Consequence of violation |
| --------- | ------------------------ |
| `spdk_interrupt_mode_enable()` precedes `spdk_thread_lib_init_ext()` | Threads cannot be flipped to interrupt mode; `set_interrupt_mode` no-ops or asserts |
| All nests succeed before transitioning to `Interrupt` | Threads whose fd_groups are not nested never fire; silent missed events |
| No mixed-mode reactors (some interrupt, some poll) | Cross-core deadlocks; hard to diagnose (historical incident, PR #1966) |
| Wakeup eventfd registered as `SPDK_FD_TYPE_EVENTFD` | Without auto-drain, a single write spins the reactor at 100% |
| `block_on()` must not be called from interrupt mode | Re-enters `spdk_thread_poll` on an already-interrupt thread; undefined behaviour |

---

## Configuration reference

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `--enable-interrupt-mode` / `ENABLE_INTERRUPT_MODE` | `false` | Enable epoll-based waiting on all reactors |
| `NVME_IOQ_POLL_PERIOD` | `0` µs | NVMe initiator I/O queue poll period. `0` means poll as fast as possible; set a non-zero value to trade latency for CPU when interrupt mode is enabled |
| `NVME_ADMINQ_POLL_PERIOD` | `1000` µs | NVMe initiator admin queue poll period |

No per-core or per-thread granularity is exposed for interrupt mode. The flag applies
uniformly to every reactor to avoid mixed-mode configurations.
