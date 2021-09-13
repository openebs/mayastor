from common.command import run_cmd_async
from common.nvme import nvme_connect, nvme_disconnect
from common.fio import Fio
from common.mayastor import containers, mayastors
import pytest
import asyncio
import mayastor_pb2 as pb

# Reusing nexus UUIDs to avoid the need to disconnect between tests
nexus_uuids = [
    "78c0e836-ef26-47c2-a136-2a99b538a9a8",
    "fc2bd1bf-301c-46e7-92e7-71e7a062e2dd",
    "1edc6a04-74b0-450e-b953-14237a6795de",
    "70bb42e6-4924-4079-a755-3798015e1319",
    "9aae12d7-48dd-4fa6-a554-c4d278a9386a",
    "af8107b6-2a9b-4097-9676-7a0941ba9bf9",
    "7fde42a4-758b-466e-9755-825328131f67",
    "6956b466-7491-4b8f-9a97-731bf7c9fd9c",
    "c5fa226d-b1c7-4102-83b3-7f69ff1b311b",
    "0cc74b98-fcd9-4a95-93ea-7c921d56c8cc",
]


def check_nexus_state(ms, state=pb.NEXUS_ONLINE):
    nl = ms.nexus_list()
    for nexus in nl:
        assert nexus.state == state
        for child in nexus.children:
            assert child.state == pb.CHILD_ONLINE


def destroy_nexus(ms, list):
    for uuid in list:
        ms.nexus_destroy(uuid)


@pytest.fixture
def create_nexus_devices(mayastors, share_null_devs):

    rlist_m0 = mayastors.get("ms0").bdev_list()
    rlist_m1 = mayastors.get("ms1").bdev_list()
    rlist_m2 = mayastors.get("ms2").bdev_list()

    assert len(rlist_m0) == len(rlist_m1) == len(rlist_m2)

    ms = mayastors.get("ms3")

    uris = []

    for uuid in nexus_uuids:
        ms.nexus_create(
            uuid,
            94 * 1024 * 1024,
            [
                rlist_m0.pop().share_uri,
                rlist_m1.pop().share_uri,
                rlist_m2.pop().share_uri,
            ],
        )

    for uuid in nexus_uuids:
        uri = ms.nexus_publish(uuid)
        uris.append(uri)

    assert len(ms.nexus_list()) == len(nexus_uuids)

    return uris


@pytest.fixture
def create_null_devs(mayastors):
    for node in ["ms0", "ms1", "ms2"]:
        ms = mayastors.get(node)

        for i in range(len(nexus_uuids)):
            ms.bdev_create(f"null:///null{i}?blk_size=512&size_mb=100")


@pytest.fixture
def share_null_devs(mayastors, create_null_devs):

    for node in ["ms0", "ms1", "ms2"]:
        ms = mayastors.get(node)
        names = ms.bdev_list()
        for n in names:
            ms.bdev_share(n.name)


@pytest.fixture
def connect_to_uris(create_nexus_devices):
    devices = {}

    for uri in create_nexus_devices:
        devices[uri] = nvme_connect(uri)

    yield devices.values()

    for uri in devices.keys():
        try:
            nvme_disconnect(uri)
        except Exception:
            pass


@pytest.mark.asyncio
async def test_null_nexus(connect_to_uris, mayastors):
    devices = connect_to_uris
    ms = mayastors.get("ms3")
    check_nexus_state(ms)

    job = Fio("job1", "randwrite", devices).build()
    await run_cmd_async(job)
