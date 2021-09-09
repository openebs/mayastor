from common.hdl import MayastorHandle
from common.command import run_cmd, run_cmd_async
from common.nvme import nvme_connect, nvme_disconnect
from common.fio import Fio
from common.fio_spdk import FioSpdk
from common.mayastor import containers, mayastors, create_temp_files, check_size
import pytest
import asyncio
import uuid as guid


@pytest.fixture
def create_pool_on_all_nodes(mayastors, create_temp_files):
    "Create a pool on each node."
    uuids = []

    for name, ms in mayastors.items():
        ms.pool_create(name, f"aio:///tmp/{name}.img")
        # validate we have zero replicas
        assert len(ms.replica_list().replicas) == 0

    for i in range(15):
        uuid = guid.uuid4()
        for name, ms in mayastors.items():
            before = ms.pool_list()
            ms.replica_create(name, uuid, 64 * 1024 * 1024)
            after = ms.pool_list()
            check_size(before, after, -64)
            # ensure our replica count goes up as expected
            assert len(ms.replica_list().replicas) == i + 1
        uuids.append(uuid)

    yield uuids


@pytest.mark.parametrize("times", range(3))
def test_restart(containers, mayastors, create_pool_on_all_nodes, times):
    """
    Test that when we create replicas and destroy them the count is as expected
    At this point we have 3 nodes each with 15 replica's.
    """

    node = containers.get("ms1")
    ms1 = mayastors.get("ms1")

    # kill one of the nodes and validate we indeed have 15 replica's
    node.kill()
    node.start()
    # we must reconnect grpc here..
    ms1.reconnect()
    # create does import here if found
    ms1.pool_create("ms1", "aio:///tmp/ms1.img")

    # check the list has 15 replica's
    replicas = ms1.replica_list().replicas
    assert 15 == len(replicas)

    # destroy a few
    for i in range(7):
        ms1.replica_destroy(replicas[i].uuid)

    # kill and reconnect
    node.kill()
    node.start()
    ms1.reconnect()

    # validate we have 8 replicas left
    ms1.pool_create("ms1", "aio:///tmp/ms1.img")
    replicas = ms1.replica_list().replicas

    assert len(replicas) == 8


async def kill_after(container, sec):
    "Kill the given container after sec seconds."
    await asyncio.sleep(sec)
    container.kill()


@pytest.fixture
def create_nexuses(mayastors, create_pool_on_all_nodes):
    "Create a nexus for each replica on each child node."
    nexuses = []
    ms1 = mayastors.get("ms1")
    uris = [
        [replica.uri for replica in mayastors.get(node).replica_list().replicas]
        for node in ["ms2", "ms3"]
    ]

    for children in zip(*uris):
        uuid = guid.uuid4()
        ms1.nexus_create(uuid, 60 * 1024 * 1024, list(children))
        nexuses.append(ms1.nexus_publish(uuid))

    yield nexuses

    for nexus in ms1.nexus_list():
        uuid = nexus.uuid
        ms1.nexus_unpublish(uuid)
        ms1.nexus_destroy(uuid)


@pytest.fixture
def connect_devices(create_nexuses):
    "Connect an nvmf device to each nexus."
    yield [nvme_connect(nexus) for nexus in create_nexuses]

    for nexus in create_nexuses:
        nvme_disconnect(nexus)


@pytest.fixture
async def mount_devices(connect_devices):
    "Create and mount a filesystem on each nvmf connected device."
    for dev in connect_devices:
        await run_cmd_async(f"sudo mkfs.xfs {dev}")
        await run_cmd_async(f"sudo mkdir -p /mnt{dev}")
        await run_cmd_async(f"sudo mount {dev} /mnt{dev}")

    yield

    for dev in connect_devices:
        await run_cmd_async(f"sudo umount /mnt{dev}")


@pytest.mark.asyncio
async def test_multiple(containers, connect_devices):
    fio_cmd = Fio(f"job-raw", "randwrite", connect_devices).build()
    print(fio_cmd)

    to_kill = containers.get("ms3")
    await asyncio.gather(run_cmd_async(fio_cmd), kill_after(to_kill, 3))


@pytest.mark.asyncio
async def test_multiple_fs(containers, connect_devices, mount_devices):
    # we're now writing to files not raw devices
    files = [f"/mnt{dev}/file.dat" for dev in connect_devices]
    fio_cmd = Fio(
        f"job-fs",
        "randwrite",
        files,
        optstr="--verify=crc32 --verify_fatal=1 --verify_async=2 --size=50mb",
    ).build()
    print(fio_cmd)

    to_kill = containers.get("ms3")
    await asyncio.gather(run_cmd_async(fio_cmd), kill_after(to_kill, 3))


@pytest.mark.asyncio
async def test_multiple_spdk(containers, create_nexuses):
    fio_cmd = FioSpdk(f"job-spdk", "randwrite", create_nexuses).build()

    to_kill = containers.get("ms3")
    await asyncio.gather(run_cmd_async(fio_cmd), kill_after(to_kill, 3))
