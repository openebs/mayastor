/*
 * phase2.c — F-Stack embedded under SPDK's EAL + reactor (Phase 2 proof).
 *
 * This is the INVERSE of the spike (experiments/ff-spdk-spike/spike.c):
 *
 *   spike:  F-Stack owns rte_eal_init AND the master loop (ff_run); SPDK runs
 *           env-less and is pumped by an ff_run() callback. Proved the mechanics
 *           but is NOT how a real io-engine looks.
 *
 *   phase2: SPDK owns the ONE rte_eal_init (spdk_env_init) AND the loop
 *           (spdk_thread_poll). F-Stack adopts SPDK's EAL (ff_init_adopt_eal)
 *           and is driven ONE pass at a time by an SPDK poller
 *           (ffsock_pump_poll -> ff_run_once), registered by ffsock_fstack_start.
 *           This is exactly how Mayastor's io-engine will embed it: mayastor
 *           owns EAL + reactors, ffsock rides along.
 *
 * PROVES: single shared EAL (no double rte_eal_init) + reactor-driven F-Stack +
 * a full TCP round-trip carried over ffsock -> F-Stack FreeBSD TCP, all under
 * SPDK's env/thread/sock stack.
 *
 * Run (root, hugepages up; net_tap PMD now built into SPDK's DPDK):
 *   ./phase2 --conf ./f-stack.conf -- <listen_port>
 * Then, from the host netns on F-Stack's subnet:
 *   printf 'hi\n' | nc 10.99.0.2 <listen_port>   # echoes back through ffsock
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/sock.h"
#include "spdk/thread.h"
#include "spdk/log.h"

#include "ff_sock.h"	/* ffsock_fstack_start / ffsock_fstack_stop */

#define ECHO_BUF_SIZE 8192

static struct spdk_thread	*g_thread;
static struct spdk_sock		*g_listen_sock;
static struct spdk_sock_group	*g_group;
static int			g_listen_port = 4420;
static volatile int		g_running = 1;

/* F-Stack arg vector (argv[0] + --conf ... ), everything before "--". */
static int			g_ff_argc;
static char			**g_ff_argv;

static void
sigint_handler(int signo)
{
	(void)signo;
	g_running = 0;
}

/* Sock group callback: echo whatever a connection sends back to it. */
static void
echo_cb(void *arg, struct spdk_sock_group *group, struct spdk_sock *sock)
{
	char buf[ECHO_BUF_SIZE];
	struct iovec iov;
	ssize_t n;

	(void)arg;
	n = spdk_sock_recv(sock, buf, sizeof(buf));
	if (n > 0) {
		iov.iov_base = buf;
		iov.iov_len = n;
		if (spdk_sock_writev(sock, &iov, 1) < 0) {
			SPDK_ERRLOG("writev failed: %d, closing\n", errno);
			goto close;
		}
		SPDK_NOTICELOG("echoed %zd bytes through ffsock\n", n);
		return;
	}
	if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
		return;
	}
close:
	spdk_sock_group_remove_sock(group, sock);
	spdk_sock_close(&sock);
	SPDK_NOTICELOG("connection closed\n");
}

/* SPDK poller: accept pending ffsock connections into the group. */
static int
accept_poller(void *ctx)
{
	struct spdk_sock *client;
	int accepted = 0;

	(void)ctx;
	while ((client = spdk_sock_accept(g_listen_sock)) != NULL) {
		if (spdk_sock_group_add_sock(g_group, client, echo_cb, NULL) < 0) {
			SPDK_ERRLOG("group_add_sock failed\n");
			spdk_sock_close(&client);
			continue;
		}
		SPDK_NOTICELOG("accepted a connection via ffsock\n");
		accepted++;
	}
	return accepted ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

/* SPDK poller: drive the sock group's epoll (ff_epoll_wait under the hood). */
static int
group_poller(void *ctx)
{
	(void)ctx;
	return spdk_sock_group_poll(g_group) > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

/*
 * Runs once on g_thread to wire everything up, IN ORDER:
 *   1. ffsock_fstack_start: adopt SPDK's EAL, bring F-Stack + its NIC port up on
 *      THIS thread, and register the F-Stack pump poller here.
 *   2. listen on ffsock + create the sock group + accept/group pollers.
 * All of it lives on the one reactor thread — the pump poller and the sock I/O
 * pollers interleave under spdk_thread_poll, which is the whole point.
 */
static void
setup_msg(void *ctx)
{
	(void)ctx;

	if (ffsock_fstack_start(g_ff_argc, g_ff_argv) != 0) {
		SPDK_ERRLOG("ffsock_fstack_start failed\n");
		g_running = 0;
		return;
	}

	g_listen_sock = spdk_sock_listen(NULL, g_listen_port, "ffsock");
	if (!g_listen_sock) {
		SPDK_ERRLOG("spdk_sock_listen(ffsock) failed on port %d\n", g_listen_port);
		g_running = 0;
		return;
	}
	g_group = spdk_sock_group_create(NULL);
	if (!g_group) {
		SPDK_ERRLOG("spdk_sock_group_create failed\n");
		g_running = 0;
		return;
	}
	spdk_poller_register(accept_poller, NULL, 1000 /* us */);
	spdk_poller_register(group_poller, NULL, 0 /* every tick */);

	SPDK_NOTICELOG("ffsock echo server up on port %d (impl=ffsock, SPDK owns EAL+loop)\n",
		       g_listen_port);
}

int
main(int argc, char **argv)
{
	struct spdk_env_opts opts;
	int rc;
	int i;

	/*
	 * Split argv at "--": tokens before it are the F-Stack arg vector
	 * (argv[0] + --conf <path> ...); the token after "--" is the listen port.
	 * EAL args are NOT taken from here — SPDK owns the EAL (opts below).
	 */
	g_ff_argc = argc;
	g_ff_argv = argv;
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			g_ff_argc = i;
			if (i + 1 < argc) {
				g_listen_port = atoi(argv[i + 1]);
			}
			break;
		}
	}

	signal(SIGINT, sigint_handler);
	signal(SIGTERM, sigint_handler);

	/*
	 * (1) THE single rte_eal_init, owned by SPDK. net_tap vdev + no-pci go
	 * through env_context (extra EAL args). core_mask 0x8 == F-Stack's
	 * lcore_mask=8 in f-stack.conf, so F-Stack's one lcore == this reactor's
	 * EAL main lcore. --in-memory keeps hugepage state private (no collision
	 * with any other DPDK process).
	 */
	spdk_env_opts_init(&opts);
	opts.name = "ffphase2";
	opts.core_mask = "0x8";
	opts.mem_size = 1024;
	opts.no_pci = true;
	opts.env_context = (void *)"--vdev=net_tap0,iface=fftap0 --in-memory";

	rc = spdk_env_init(&opts);
	if (rc < 0) {
		fprintf(stderr, "spdk_env_init failed: %d\n", rc);
		return 1;
	}

	/* (2) SPDK threading, on the EAL main lcore. */
	rc = spdk_thread_lib_init(NULL, 0);
	if (rc != 0) {
		fprintf(stderr, "spdk_thread_lib_init failed: %d\n", rc);
		return 1;
	}
	g_thread = spdk_thread_create("ffphase2", NULL);
	if (!g_thread) {
		fprintf(stderr, "spdk_thread_create failed\n");
		return 1;
	}
	spdk_set_thread(g_thread);

	spdk_thread_send_msg(g_thread, setup_msg, NULL);

	SPDK_NOTICELOG("phase2: SPDK owns EAL + loop; pumping F-Stack via a poller\n");

	/* (3) THE loop, owned by SPDK. Drives the F-Stack pump poller + sock I/O. */
	while (g_running) {
		spdk_thread_poll(g_thread, 0, 0);
	}

	SPDK_NOTICELOG("phase2: shutting down\n");
	ffsock_fstack_stop();
	return 0;
}
