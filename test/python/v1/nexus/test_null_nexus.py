from common.command import run_cmd_async
from common.nvme import nvme_connect, nvme_disconnect
from common.fio import Fio
from v1.mayastor import containers, mayastors
import pytest
import asyncio
import uuid as guid
import mayastor_pb2 as pb

NEXUS_COUNT = 15


def check_nexus_state(ms, state=pb.NEXUS_ONLINE):
    for nexus in ms.nexus_list(None):
        assert nexus.state == state
        for child in nexus.children:
            assert child.state == pb.CHILD_ONLINE


@pytest.fixture
def device_nodes():
    yield ["ms0", "ms1", "ms2"]


@pytest.fixture
def nexus_node():
    yield "ms3"


@pytest.fixture
def min_cntlid():
    """NVMe minimum controller ID to be used."""
    min_cntlid = 50
    return min_cntlid


@pytest.fixture
def resv_key():
    """NVMe reservation key to be used."""
    resv_key = 0xABCDEF0012345678
    return resv_key


@pytest.fixture
def create_null_devices(mayastors, device_nodes):
    for node in device_nodes:
        ms = mayastors.get(node)
        for i in range(NEXUS_COUNT):
            ms.bdev_create("null:///null{:02d}?blk_size=512&size_mb=100".format(i))
    yield
    for node in device_nodes:
        ms = mayastors.get(node)
        for dev in ms.bdev_list():
            ms.bdev_destroy(dev.uri)


@pytest.fixture
def share_null_devices(mayastors, device_nodes, create_null_devices):
    for node in device_nodes:
        ms = mayastors.get(node)
        for dev in ms.bdev_list():
            ms.bdev_share(dev.name)
    yield
    for node in device_nodes:
        ms = mayastors.get(node)
        for dev in ms.bdev_list():
            ms.bdev_unshare(dev.name)


@pytest.fixture
def create_nexuses(
    mayastors, device_nodes, nexus_node, min_cntlid, resv_key, share_null_devices
):
    ms = mayastors.get(nexus_node)
    uris = [
        [dev.share_uri for dev in mayastors.get(node).bdev_list()]
        for node in device_nodes
    ]
    for children in zip(*uris):
        ms.nexus_create(
            str(guid.uuid4()),
            guid.uuid4(),
            60 * 1024 * 1024,
            min_cntlid,
            min_cntlid + 9,
            resv_key,
            0,
            list(children),
        )

    yield
    for nexus in ms.nexus_list(None):
        ms.nexus_destroy(nexus.uuid)


@pytest.fixture
def publish_nexuses(mayastors, nexus_node, create_nexuses):
    nexuses = []
    ms = mayastors.get(nexus_node)
    for nexus in ms.nexus_list(None):
        nexuses.append(ms.nexus_publish(nexus.uuid))
    yield nexuses
    for nexus in ms.nexus_list(None):
        ms.nexus_unpublish(nexus.uuid)


@pytest.fixture
def connect_devices(publish_nexuses):
    yield [nvme_connect(nexus) for nexus in publish_nexuses]
    for nexus in publish_nexuses:
        nvme_disconnect(nexus)


@pytest.mark.asyncio
async def test_null_nexus(mayastors, nexus_node, connect_devices):
    ms = mayastors.get(nexus_node)
    check_nexus_state(ms)

    job = Fio("job1", "randwrite", connect_devices).build()
    await run_cmd_async(job)
