# validate that when recreating lvols on the same pool,
# does not retain previous filesystem data

from common.mayastor import mayastors, target_vm
import pytest
from common.nvme import nvme_remote_connect, nvme_remote_disconnect
from common.command import run_cmd_async_at
import json

from common.constants import nvme_nqn_prefix


@pytest.fixture
def create_pool(mayastors):
    ms = mayastors.get("ms0")
    ms.pool_create("tpool", "aio:///dev/sda3")
    yield
    ms.pool_destroy("tpool")


def create_volumes(mayastors):
    ms = mayastors.get("ms0")
    for i in range(0, 15):
        ms.replica_create("tpool", f"replica-{i}", 4 * 1024 * 1024)


def delete_volumes(mayastors):
    ms = mayastors.get("ms0")
    for i in range(0, 15):
        ms.replica_destroy(f"replica-{i}")


# this will fail the second time around as mkfs will fail if it finds
# a preexisting filesystem
async def mkfs_on_target(target_vm, mayastors):
    host_ip = mayastors.get("ms0").ip_address()
    remote_devices = []

    for i in range(0, 15):
        dev = await nvme_remote_connect(
            target_vm, f"nvmf://{host_ip}:8420/{nvme_nqn_prefix}:replica-{i}"
        )
        remote_devices.append(dev)

    print(await run_cmd_async_at(target_vm, "lsblk -o name,fstype -J"))

    for d in remote_devices:
        await run_cmd_async_at(target_vm, f"sudo mkfs.xfs {d}")

    for i in range(0, 15):
        dev = await nvme_remote_disconnect(
            target_vm, f"nvmf://{host_ip}:8420/{nvme_nqn_prefix}:replica-{i}"
        )


@pytest.mark.asyncio
async def test_lvol_unmap(mayastors, create_pool, target_vm):
    create_volumes(mayastors)
    await mkfs_on_target(target_vm, mayastors)
    delete_volumes(mayastors)

    create_volumes(mayastors)
    await mkfs_on_target(target_vm, mayastors)
    delete_volumes(mayastors)
