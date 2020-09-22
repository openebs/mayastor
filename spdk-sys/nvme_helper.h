#include <stdint.h>

struct spdk_nvme_cpl;
struct spdk_nvme_status;

struct spdk_nvme_status *get_nvme_status(struct spdk_nvme_cpl *cpl);
uint16_t *get_nvme_status_raw(struct spdk_nvme_cpl *cpl);
