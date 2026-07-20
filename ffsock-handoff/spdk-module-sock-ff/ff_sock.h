/*
 * ff_sock.h — public entrypoints for the F-Stack-backed spdk_sock module.
 *
 * The sock-impl itself registers automatically (SPDK_NET_IMPL_REGISTER at the
 * bottom of ff_sock.c). These two calls are the Phase 2 embed API: an external
 * SPDK application (the phase2 harness today, the Mayastor io-engine next)
 * brings F-Stack up on SPDK's already-initialized EAL and starts pumping it.
 */
#ifndef SPDK_MODULE_SOCK_FF_H
#define SPDK_MODULE_SOCK_FF_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Bring F-Stack up on SPDK's shared DPDK EAL and register a poller on the
 * CURRENT spdk_thread that drives one F-Stack packet-processing pass per tick.
 *
 * Preconditions:
 *   - SPDK's EAL is already initialized (spdk_env_init has run).
 *   - Called on the reactor thread/lcore that will own ffsock listeners and
 *     connections (F-Stack per-lcore state is thread-local to this thread).
 *
 * argv is the F-Stack argument vector (argv[0]=progname, then F-Stack config
 * args such as --conf=<path> --proc-type=primary); EAL args are ignored because
 * the EAL is adopted, not re-initialized.
 *
 * Returns 0 on success, negative errno on failure. Idempotent: a second call
 * while already started is a no-op returning 0.
 */
int ffsock_fstack_start(int argc, char *const argv[]);

/* Stop pumping F-Stack (unregister the poller). Call on the same thread. */
void ffsock_fstack_stop(void);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_MODULE_SOCK_FF_H */
