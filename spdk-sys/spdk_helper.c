
#include "spdk_helper.h"
#include <spdk/thread.h>

void *spdk_io_channel_get_ctx_hpl(struct spdk_io_channel *ch)
{
	return spdk_io_channel_get_ctx(ch);
}
