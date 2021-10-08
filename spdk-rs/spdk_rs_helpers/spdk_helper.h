
#ifndef MAYASTOR_SPDK_HELPER_H
#define MAYASTOR_SPDK_HELPER_H

#include <spdk/thread.h>

/**
 * Note: This function is a wrapper for an SPDK function which
 * is static-inline and is therefore unreachable for Rust.
 *
 * Get the context buffer associated with an I/O channel.
 *
 * \param ch I/O channel.
 *
 * \return a pointer to the context buffer.
 */
void *spdk_rs_io_channel_get_ctx(struct spdk_io_channel *ch);

#endif // MAYASTOR_SPDK_HELPER_H
