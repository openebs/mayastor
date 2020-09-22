#include "nvme_helper.h"

#include <spdk/nvme_spec.h>

struct spdk_nvme_status *
get_nvme_status(struct spdk_nvme_cpl *cpl) {
	return &cpl->status;
}

uint16_t *
get_nvme_status_raw(struct spdk_nvme_cpl *cpl) {
	return &cpl->status_raw;
}
