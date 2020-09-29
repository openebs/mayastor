#include <stddef.h>
#include <stdint.h>

#include <spdk/bdev.h>

struct spdk_nvme_cmd;
struct spdk_nvme_cpl;
struct spdk_nvme_status;

struct spdk_nvme_status *get_nvme_status(struct spdk_nvme_cpl *cpl);
uint16_t *get_nvme_status_raw(struct spdk_nvme_cpl *cpl);

int
spdk_bdev_nvme_admin_passthru_ro(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			      const struct spdk_nvme_cmd *cmd, void *buf, size_t nbytes,
			      spdk_bdev_io_completion_cb cb, void *cb_arg);
