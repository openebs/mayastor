#include <stddef.h>
#include <stdint.h>

#include <spdk/bdev.h>
#include <spdk/nvme.h>

struct spdk_nvme_cmd;
struct spdk_nvme_cpl;
struct spdk_nvme_status;

uint32_t nvme_cmd_cdw10_get_val(const struct spdk_nvme_cmd *cmd);
uint32_t nvme_cmd_cdw11_get_val(const struct spdk_nvme_cmd *cmd);
uint32_t *nvme_cmd_cdw10_get(struct spdk_nvme_cmd *cmd);
uint32_t *nvme_cmd_cdw11_get(struct spdk_nvme_cmd *cmd);

struct spdk_nvme_status *nvme_status_get(struct spdk_nvme_cpl *cpl);
uint16_t *nvme_status_raw_get(struct spdk_nvme_cpl *cpl);

int
spdk_bdev_nvme_admin_passthru_ro(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			      const struct spdk_nvme_cmd *cmd, void *buf, size_t nbytes,
			      spdk_bdev_io_completion_cb cb, void *cb_arg);

void
nvme_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr);
