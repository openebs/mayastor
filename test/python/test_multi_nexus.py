from common.hdl import MayastorHandle
from common.command import run_cmd, run_cmd_async
from common.nvme import nvme_connect, nvme_disconnect
from common.fio import Fio
from common.fio_spdk import FioSpdk
from common.mayastor import containers, mayastors, create_temp_files, check_size
import pytest
import asyncio
import uuid as guid

UUID = "0000000-0000-0000-0000-000000000001"
NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"


@pytest.fixture
def create_pool_on_all_nodes(mayastors, create_temp_files):
    "Create a pool on each node."
    uuids = []

    for name, ms in mayastors.items():
        ms.pool_create(f"{name}", f"aio:///tmp/{name}.img")
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

    return uuids


@pytest.mark.parametrize("times", range(2))
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


@pytest.mark.asyncio
async def test_multiple(containers, mayastors, create_pool_on_all_nodes):

    ms1 = mayastors.get("ms1")
    rlist_m2 = mayastors.get("ms2").replica_list().replicas
    rlist_m3 = mayastors.get("ms3").replica_list().replicas
    nexus_list = []
    to_kill = containers.get("ms3")

    devs = []

    for _ in range(15):
        uuid = guid.uuid4()
        ms1.nexus_create(
            uuid, 60 * 1024 * 1024, [rlist_m2.pop().uri, rlist_m3.pop().uri]
        )
        nexus_list.append(ms1.nexus_publish(uuid))

    for nexus in nexus_list:
        dev = nvme_connect(nexus)
        devs.append(dev)

    fio_cmd = Fio(f"job-raw", "randwrite", devs).build()

    await asyncio.gather(run_cmd_async(fio_cmd), kill_after(to_kill, 3))

    for nexus in nexus_list:
        nvme_disconnect(nexus)


@pytest.mark.asyncio
async def test_multiple_fs(containers, mayastors, create_pool_on_all_nodes):

    ms1 = mayastors.get("ms1")
    rlist_m2 = mayastors.get("ms2").replica_list().replicas
    rlist_m3 = mayastors.get("ms3").replica_list().replicas
    nexus_list = []
    to_kill = containers.get("ms3")

    devs = []

    for _ in range(15):
        uuid = guid.uuid4()
        ms1.nexus_create(
            uuid, 60 * 1024 * 1024, [rlist_m2.pop().uri, rlist_m3.pop().uri]
        )
        nexus_list.append(ms1.nexus_publish(uuid))

    for nexus in nexus_list:
        dev = nvme_connect(nexus)
        devs.append(dev)

    for d in devs:
        await run_cmd_async(f"sudo mkfs.xfs {d}")
        await run_cmd_async(f"sudo mkdir -p /mnt{d}")
        await run_cmd_async(f"sudo mount {d} /mnt{d}")

    # as we are mounted now we need to ensure we write to files not raw devices
    files = []
    for d in devs:
        files.append(f"/mnt{d}/file.dat")

    fio_cmd = Fio(
        f"job-fs",
        "randwrite",
        files,
        optstr="--verify=crc32 --verify_fatal=1 --verify_async=2 --size=50mb",
    ).build()
    print(fio_cmd)

    await asyncio.gather(run_cmd_async(fio_cmd), kill_after(to_kill, 3))

    for d in devs:
        await run_cmd_async(f"sudo umount {d}")

    for nexus in nexus_list:
        nvme_disconnect(nexus)


@pytest.mark.asyncio
async def test_multiple_spdk(containers, mayastors, create_pool_on_all_nodes):

    ms1 = mayastors.get("ms1")
    rlist_m2 = mayastors.get("ms2").replica_list().replicas
    rlist_m3 = mayastors.get("ms3").replica_list().replicas
    nexus_list = []
    to_kill = containers.get("ms3")

    devs = []

    for i in range(15):
        uuid = guid.uuid4()
        ms1.nexus_create(
            uuid, 60 * 1024 * 1024, [rlist_m2.pop().uri, rlist_m3.pop().uri]
        )
        nexus_list.append(ms1.nexus_publish(uuid))

    fio_cmd = FioSpdk(f"job-1", "randwrite", nexus_list).build()

    await asyncio.gather(run_cmd_async(fio_cmd), kill_after(to_kill, 3))
