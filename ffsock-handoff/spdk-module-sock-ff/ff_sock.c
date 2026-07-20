/*
 * ff_sock.c — F-Stack-backed spdk_sock implementation (FEASIBILITY SPIKE).
 *
 * Registers an "ffsock" spdk_net_impl that routes SPDK's socket abstraction
 * through F-Stack's userspace FreeBSD TCP stack (ff_socket / ff_recv /
 * ff_writev / ff_epoll). This is the same integration point SPDK's removed VPP
 * sock impl used, and the point NVMe/TCP (lib/nvmf/tcp.c, lib/nvme/nvme_tcp.c)
 * would eventually call through.
 *
 * SCOPE: Phase 2. Beyond the sock-impl members, this now brings F-Stack up on
 * SPDK's shared EAL (ffsock_fstack_start -> ff_init_adopt_eal) and pumps its
 * packet loop from an SPDK reactor poller (ffsock_pump_poll -> ff_run_once) —
 * so ffsock actually carries TCP once started. We implement the minimal core
 * member set that is stable across SPDK 25.05 (io-engine pin) and 26.09. We use
 * DESIGNATED INITIALIZERS below so any member absent in your SPDK version is
 * simply left NULL — do NOT name members that don't exist in the tree you build
 * against (e.g. init/connect_async/readv_async/get_interface_name/get_numa_id
 * are 26.x-only; get_opts/set_opts left NULL is fine here).
 *
 * DELIBERATELY OMITTED (not needed to prove the mechanics):
 *   - zero-copy send, recv pipe, TLS/SSL, placement_id/NAPI steering
 *   - writev_async / readv_async (async request queue)
 *   - impl opts get/set
 */

#include "spdk/stdinc.h"
#include "spdk/sock.h"
#include "spdk/log.h"
#include "spdk/string.h"	/* SPDK_STRINGIFY, used by SPDK_NET_IMPL_REGISTER_DEFAULT */
#include "spdk/thread.h"	/* Phase 2: spdk_poller for the F-Stack pump */
#include "spdk_internal/sock_module.h"

/* F-Stack public API (build with -I$(FF_PATH)/lib) */
#include "ff_api.h"
#include "ff_epoll.h"

#include "ff_sock.h"	/* ffsock_fstack_start / _stop (Phase 2 embed API) */

struct ff_sock {
	struct spdk_sock	base;
	int			fd;	/* F-Stack fd, NOT a kernel fd */
};

struct ff_group_impl {
	struct spdk_sock_group_impl	base;
	int				epfd;	/* F-Stack epoll fd */
};

#define __ff_sock(s)  ((struct ff_sock *)(s))
#define __ff_group(g) ((struct ff_group_impl *)(g))

/*
 * Lazy bring-up: the first ffsock socket/group operation on a reactor triggers
 * F-Stack init + the pump poller if the embedder hasn't already called
 * ffsock_fstack_start() explicitly. This lets a consumer that only knows the
 * sock layer (the Mayastor io-engine, the stock nvmf_tgt) use ffsock just by
 * selecting the impl — F-Stack bootstraps itself on the calling reactor thread.
 * Config path comes from $FFSOCK_CONF (default /etc/f-stack.conf).
 *
 * g_ffsock_pump doubles as the "F-Stack is up" flag: NULL until the pump poller
 * is registered, non-NULL afterward.
 */
static struct spdk_poller *g_ffsock_pump;
static void ffsock_ensure_started(void);

/* F-Stack sockets use struct linux_sockaddr (layout-compatible with sockaddr). */
static int
ff_set_nonblock(int fd)
{
	int flags = ff_fcntl(fd, F_GETFL, 0);
	if (flags < 0) {
		return -errno;
	}
	if (ff_fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		return -errno;
	}
	return 0;
}

static struct spdk_sock *
ffsock_listen(const char *ip, int port, struct spdk_sock_opts *opts)
{
	struct ff_sock *sock;
	struct sockaddr_in sa = {0};
	int fd, one = 1;

	ffsock_ensure_started();

	fd = ff_socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		SPDK_ERRLOG("ff_socket failed: %d\n", errno);
		return NULL;
	}
	ff_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	sa.sin_family = AF_INET;
	sa.sin_port = htons((uint16_t)port);
	sa.sin_addr.s_addr = (ip && *ip) ? inet_addr(ip) : htonl(INADDR_ANY);

	if (ff_bind(fd, (struct linux_sockaddr *)&sa, sizeof(sa)) < 0) {
		SPDK_ERRLOG("ff_bind(%s:%d) failed: %d\n", ip, port, errno);
		ff_close(fd);
		return NULL;
	}
	if (ff_listen(fd, opts ? 511 : 511) < 0) {
		SPDK_ERRLOG("ff_listen failed: %d\n", errno);
		ff_close(fd);
		return NULL;
	}
	if (ff_set_nonblock(fd) < 0) {
		ff_close(fd);
		return NULL;
	}

	sock = calloc(1, sizeof(*sock));
	if (!sock) {
		ff_close(fd);
		return NULL;
	}
	sock->fd = fd;
	return &sock->base;
}

static struct spdk_sock *
ffsock_connect(const char *ip, int port, struct spdk_sock_opts *opts)
{
	struct ff_sock *sock;
	struct sockaddr_in sa = {0};
	int fd;

	ffsock_ensure_started();

	fd = ff_socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return NULL;
	}
	sa.sin_family = AF_INET;
	sa.sin_port = htons((uint16_t)port);
	sa.sin_addr.s_addr = inet_addr(ip);

	/* Blocking connect for the spike; a real impl would go nonblocking +
	 * connect_async and complete via the poll group. */
	if (ff_connect(fd, (struct linux_sockaddr *)&sa, sizeof(sa)) < 0) {
		SPDK_ERRLOG("ff_connect(%s:%d) failed: %d\n", ip, port, errno);
		ff_close(fd);
		return NULL;
	}
	if (ff_set_nonblock(fd) < 0) {
		ff_close(fd);
		return NULL;
	}
	sock = calloc(1, sizeof(*sock));
	if (!sock) {
		ff_close(fd);
		return NULL;
	}
	sock->fd = fd;
	return &sock->base;
}

static struct spdk_sock *
ffsock_accept(struct spdk_sock *_sock)
{
	struct ff_sock *lsock = __ff_sock(_sock);
	struct ff_sock *nsock;
	struct sockaddr_in sa = {0};
	socklen_t salen = sizeof(sa);
	int fd;

	fd = ff_accept(lsock->fd, (struct linux_sockaddr *)&sa, &salen);
	if (fd < 0) {
		/* EAGAIN/EWOULDBLOCK is the normal "nothing pending" case; the
		 * accept poller in spike.c retries next tick. */
		errno = errno ? errno : EAGAIN;
		return NULL;
	}
	if (ff_set_nonblock(fd) < 0) {
		ff_close(fd);
		return NULL;
	}
	nsock = calloc(1, sizeof(*nsock));
	if (!nsock) {
		ff_close(fd);
		return NULL;
	}
	nsock->fd = fd;
	return &nsock->base;
}

static int
ffsock_close(struct spdk_sock *_sock)
{
	struct ff_sock *sock = __ff_sock(_sock);
	int rc = ff_close(sock->fd);
	free(sock);
	return rc < 0 ? -errno : 0;
}

static ssize_t
ffsock_recv(struct spdk_sock *_sock, void *buf, size_t len)
{
	return ff_recv(__ff_sock(_sock)->fd, buf, len, 0);
}

static ssize_t
ffsock_readv(struct spdk_sock *_sock, struct iovec *iov, int iovcnt)
{
	return ff_readv(__ff_sock(_sock)->fd, iov, iovcnt);
}

static ssize_t
ffsock_writev(struct spdk_sock *_sock, struct iovec *iov, int iovcnt)
{
	return ff_writev(__ff_sock(_sock)->fd, iov, iovcnt);
}

static int
ffsock_getaddr(struct spdk_sock *_sock, char *saddr, int slen, uint16_t *sport,
	       char *caddr, int clen, uint16_t *cport)
{
	struct ff_sock *sock = __ff_sock(_sock);
	struct sockaddr_in sa;
	socklen_t salen = sizeof(sa);

	if (saddr && slen > 0) {
		saddr[0] = '\0';
		if (ff_getsockname(sock->fd, (struct linux_sockaddr *)&sa, &salen) == 0) {
			inet_ntop(AF_INET, &sa.sin_addr, saddr, slen);
			if (sport) {
				*sport = ntohs(sa.sin_port);
			}
		}
	}
	if (caddr && clen > 0) {
		caddr[0] = '\0';
		salen = sizeof(sa);
		if (ff_getpeername(sock->fd, (struct linux_sockaddr *)&sa, &salen) == 0) {
			inet_ntop(AF_INET, &sa.sin_addr, caddr, clen);
			if (cport) {
				*cport = ntohs(sa.sin_port);
			}
		}
	}
	return 0;
}

static bool ffsock_is_ipv6(struct spdk_sock *s) { (void)s; return false; }
static bool ffsock_is_ipv4(struct spdk_sock *s) { (void)s; return true; }
static bool ffsock_is_connected(struct spdk_sock *s) { (void)s; return true; }

static int
ffsock_set_recvlowat(struct spdk_sock *_sock, int nbytes)
{
	int rc = ff_setsockopt(__ff_sock(_sock)->fd, SOL_SOCKET, SO_RCVLOWAT,
			       &nbytes, sizeof(nbytes));
	return rc < 0 ? -errno : 0;
}

static int
ffsock_set_recvbuf(struct spdk_sock *_sock, int sz)
{
	int rc = ff_setsockopt(__ff_sock(_sock)->fd, SOL_SOCKET, SO_RCVBUF,
			       &sz, sizeof(sz));
	return rc < 0 ? -errno : 0;
}

static int
ffsock_set_sendbuf(struct spdk_sock *_sock, int sz)
{
	int rc = ff_setsockopt(__ff_sock(_sock)->fd, SOL_SOCKET, SO_SNDBUF,
			       &sz, sizeof(sz));
	return rc < 0 ? -errno : 0;
}

/*
 * Drain queued write requests via ff_writev, completing each request as its
 * bytes are fully sent. This mirrors posix's _sock_flush() request-completion
 * accounting, minus zero-copy (ffsock sends synchronously). The nvmf/TCP data
 * path relies on this: it queues PDUs with writev_async and drains via flush.
 */
static int
ffsock_flush(struct spdk_sock *_sock)
{
	struct ff_sock *sock = __ff_sock(_sock);
	struct iovec iovs[IOV_BATCH_SIZE];
	int iovcnt, i, flags = 0, retval;
	struct spdk_sock_request *req;
	ssize_t rc, sent;
	unsigned int offset;
	size_t len;

	/* Can't flush from within a completion callback (recursion). */
	if (_sock->cb_cnt > 0) {
		errno = EAGAIN;
		return -1;
	}

	iovcnt = spdk_sock_prep_reqs(_sock, iovs, 0, NULL, &flags);
	if (iovcnt == 0) {
		return 0;
	}

	rc = ff_writev(sock->fd, iovs, iovcnt);
	if (rc <= 0) {
		if (rc == 0 || errno == EAGAIN || errno == EWOULDBLOCK) {
			errno = EAGAIN;
		}
		return -1;
	}
	sent = rc;

	/* Consume the requests that were actually written. */
	req = TAILQ_FIRST(&_sock->queued_reqs);
	while (req) {
		offset = req->internal.offset;

		for (i = 0; i < req->iovcnt; i++) {
			/* Skip the bytes already sent in a previous flush. */
			if (offset >= SPDK_SOCK_REQUEST_IOV(req, i)->iov_len) {
				offset -= SPDK_SOCK_REQUEST_IOV(req, i)->iov_len;
				continue;
			}
			len = SPDK_SOCK_REQUEST_IOV(req, i)->iov_len - offset;
			if (len > (size_t)rc) {
				/* Partially sent — remember progress, stop here. */
				req->internal.offset += rc;
				return sent;
			}
			offset = 0;
			req->internal.offset += len;
			rc -= len;
		}

		/* Whole request sent; ff_writev is synchronous so complete it now. */
		spdk_sock_request_pend(_sock, req);
		retval = spdk_sock_request_put(_sock, req, 0);
		if (retval) {
			break;
		}
		if (rc == 0) {
			break;
		}
		req = TAILQ_FIRST(&_sock->queued_reqs);
	}

	return sent;
}

static void
ffsock_writev_async(struct spdk_sock *_sock, struct spdk_sock_request *req)
{
	int rc;

	spdk_sock_request_queue(_sock, req);

	/* Flush eagerly; ffsock has no async engine, so completion is synchronous.
	 * A queue that's still short is left for the next flush (nvmf drives one
	 * from its poll group), matching posix's batching behavior. */
	if (_sock->queued_iovcnt >= IOV_BATCH_SIZE) {
		rc = ffsock_flush(_sock);
		if (rc < 0 && errno != EAGAIN) {
			spdk_sock_abort_requests(_sock);
		}
	}
}

static struct spdk_sock_group_impl *
ffsock_group_impl_get_optimal(struct spdk_sock *_sock, struct spdk_sock_group_impl *hint)
{
	(void)_sock;
	(void)hint;
	/* ffsock runs on a single F-Stack lcore with no placement/NUMA steering,
	 * so it expresses "no preference" (like posix with placement_id == -1).
	 * SPDK calls this unconditionally, so it must not be NULL. */
	return NULL;
}

/* ---- poll group: F-Stack epoll (ff_epoll.h uses standard sys/epoll.h layout) ---- */

static struct spdk_sock_group_impl *
ffsock_group_impl_create(void)
{
	struct ff_group_impl *group;
	int epfd;

	/*
	 * The generic sock layer creates a group_impl for EVERY registered impl
	 * (see spdk_sock_group_create), and does so eagerly — e.g. the nvmf TCP
	 * transport creates its listen sock group at transport-create time. Make
	 * sure F-Stack is up before we touch ff_epoll, and if it cannot be brought
	 * up, contribute NO ffsock group (return NULL) rather than dereferencing an
	 * uninitialized F-Stack and crashing the unrelated (e.g. posix) group.
	 */
	ffsock_ensure_started();
	if (!g_ffsock_pump) {
		return NULL;
	}

	epfd = ff_epoll_create(1024);
	if (epfd < 0) {
		return NULL;
	}
	group = calloc(1, sizeof(*group));
	if (!group) {
		ff_close(epfd);
		return NULL;
	}
	group->epfd = epfd;
	return &group->base;
}

static int
ffsock_group_impl_add_sock(struct spdk_sock_group_impl *_group, struct spdk_sock *_sock)
{
	struct ff_group_impl *group = __ff_group(_group);
	struct ff_sock *sock = __ff_sock(_sock);
	struct epoll_event ev = {0};

	ev.events = EPOLLIN | EPOLLERR;
	ev.data.ptr = sock;	/* SPDK expects the spdk_sock* back from poll */
	if (ff_epoll_ctl(group->epfd, EPOLL_CTL_ADD, sock->fd, &ev) < 0) {
		return -errno;
	}
	return 0;
}

static int
ffsock_group_impl_remove_sock(struct spdk_sock_group_impl *_group, struct spdk_sock *_sock)
{
	struct ff_group_impl *group = __ff_group(_group);
	struct ff_sock *sock = __ff_sock(_sock);

	if (ff_epoll_ctl(group->epfd, EPOLL_CTL_DEL, sock->fd, NULL) < 0) {
		return -errno;
	}
	return 0;
}

static int
ffsock_group_impl_poll(struct spdk_sock_group_impl *_group, int max_events,
		       struct spdk_sock **socks)
{
	struct ff_group_impl *group = __ff_group(_group);
	struct epoll_event events[MAX_EVENTS_PER_POLL];
	struct spdk_sock *sock, *tmp;
	int n, i, count = 0;

	/*
	 * Drain any queued writes first, exactly like posix's group poll. ffsock's
	 * writev_async only sends eagerly once a batch has accumulated; small
	 * responses (e.g. the NVMe/TCP fabrics Connect / Property Get capsule
	 * replies) would otherwise sit queued and stall the handshake. Flushing
	 * every poll cycle guarantees they go out promptly.
	 */
	TAILQ_FOREACH_SAFE(sock, &_group->socks, link, tmp) {
		if (!TAILQ_EMPTY(&sock->queued_reqs)) {
			ffsock_flush(sock);
		}
	}

	if (max_events > MAX_EVENTS_PER_POLL) {
		max_events = MAX_EVENTS_PER_POLL;
	}
	/* timeout 0: non-blocking. The reactor pump poller provides the cadence. */
	n = ff_epoll_wait(group->epfd, events, max_events, 0);
	if (n <= 0) {
		return n < 0 ? -errno : 0;
	}
	for (i = 0; i < n; i++) {
		socks[count++] = (struct spdk_sock *)events[i].data.ptr;
	}
	return count;
}

static int
ffsock_group_impl_close(struct spdk_sock_group_impl *_group)
{
	struct ff_group_impl *group = __ff_group(_group);
	ff_close(group->epfd);
	free(group);
	return 0;
}

/* ---- Phase 2: bring F-Stack up on SPDK's EAL + pump it from a reactor poller ---- */
/* g_ffsock_pump is declared near the top (it doubles as the "F-Stack up" flag). */

/*
 * Reactor poller: drive ONE F-Stack packet-processing pass per tick. This is
 * the control-loop inversion that makes ffsock real — instead of F-Stack's
 * ff_run() owning the thread, an SPDK poller owns cadence and calls into
 * F-Stack. Socket readiness is handled separately on this same thread by the
 * sock-group poller (ffsock_group_impl_poll -> ff_epoll_wait), so no user loop
 * callback is passed here.
 */
static int
ffsock_pump_poll(void *arg)
{
	(void)arg;
	return ff_run_once(NULL, NULL) ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int
ffsock_fstack_start(int argc, char *const argv[])
{
	int rc;

	if (g_ffsock_pump) {
		SPDK_WARNLOG("ffsock: F-Stack already started\n");
		return 0;
	}

	/*
	 * Adopt SPDK's already-initialized EAL (single shared rte_eal_init). Must
	 * run on the reactor thread/lcore that will own ffsock listeners and drive
	 * the pump poller — F-Stack per-lcore state is thread-local.
	 */
	rc = ff_init_adopt_eal(argc, argv);
	if (rc < 0) {
		SPDK_ERRLOG("ffsock: ff_init_adopt_eal failed: %d\n", rc);
		return rc;
	}

	g_ffsock_pump = spdk_poller_register(ffsock_pump_poll, NULL, 0);
	if (!g_ffsock_pump) {
		SPDK_ERRLOG("ffsock: failed to register F-Stack pump poller\n");
		return -ENOMEM;
	}

	SPDK_NOTICELOG("ffsock: F-Stack up on shared EAL; pump poller running\n");
	return 0;
}

void
ffsock_fstack_stop(void)
{
	if (g_ffsock_pump) {
		spdk_poller_unregister(&g_ffsock_pump);
	}
}

static void
ffsock_ensure_started(void)
{
	const char *conf;
	char *argv[3];

	if (g_ffsock_pump) {
		return;		/* already started (explicitly or by a prior lazy call) */
	}

	/*
	 * SAFETY: stay dormant unless F-Stack is explicitly enabled via FFSOCK_CONF.
	 * The sock layer calls ffsock_group_impl_create for EVERY registered impl
	 * (every sock group), so an io-engine that merely LINKS ffsock but isn't
	 * configured for it must NOT try to bring up F-Stack here — F-Stack's
	 * ff_load_config / ff_dpdk_init call exit()/rte_exit() on a missing config
	 * or missing NIC port, which would take the whole process (and the node)
	 * down. With FFSOCK_CONF unset we leave g_ffsock_pump NULL; group_impl_create
	 * then returns NULL and posix serves as usual. ffsock is opt-in.
	 */
	conf = getenv("FFSOCK_CONF");
	if (!conf) {
		return;
	}

	/* argv[0] is the (ignored) progname; --conf points F-Stack at its config.
	 * EAL args are absent on purpose — the EAL is adopted, not created here. */
	argv[0] = (char *)"ffsock";
	argv[1] = (char *)"--conf";
	argv[2] = (char *)conf;

	if (ffsock_fstack_start(3, argv) != 0) {
		SPDK_ERRLOG("ffsock: lazy F-Stack bring-up failed (conf=%s)\n", conf);
	}
}

static struct spdk_net_impl g_ffsock_net_impl = {
	.name			= "ffsock",

	.getaddr		= ffsock_getaddr,
	.connect		= ffsock_connect,
	.listen			= ffsock_listen,
	.accept			= ffsock_accept,
	.close			= ffsock_close,
	.recv			= ffsock_recv,
	.readv			= ffsock_readv,
	.writev			= ffsock_writev,
	.writev_async		= ffsock_writev_async,
	.flush			= ffsock_flush,

	.set_recvlowat		= ffsock_set_recvlowat,
	.set_recvbuf		= ffsock_set_recvbuf,
	.set_sendbuf		= ffsock_set_sendbuf,

	.is_ipv6		= ffsock_is_ipv6,
	.is_ipv4		= ffsock_is_ipv4,
	.is_connected		= ffsock_is_connected,

	.group_impl_get_optimal	= ffsock_group_impl_get_optimal,
	.group_impl_create	= ffsock_group_impl_create,
	.group_impl_add_sock	= ffsock_group_impl_add_sock,
	.group_impl_remove_sock	= ffsock_group_impl_remove_sock,
	.group_impl_poll	= ffsock_group_impl_poll,
	.group_impl_close	= ffsock_group_impl_close,
};

/* Register as a NON-default impl: ffsock only carries traffic once F-Stack is
 * initialized (Phase 2), so it must be opt-in (posix stays the default). Select
 * it explicitly via spdk_sock_impl_set_default_impl("ffsock") or per-listener
 * impl_name once F-Stack init + reactor drive is wired. */
SPDK_NET_IMPL_REGISTER(ffsock, &g_ffsock_net_impl);
