from common.hdl import MayastorHandle
from common.command import run_cmd, run_cmd_async
from common.fio import Fio
from common.fio_spdk import FioSpdk
from common.mayastor import containers, mayastors
from common.volume import Volume
import logging
import pytest
import uuid as guid
import grpc
import asyncio
import time
import mayastor_pb2 as pb
from common.nvme import (
    nvme_discover,
    nvme_connect,
    nvme_disconnect,
    nvme_id_ctrl,
    nvme_resv_report,
)


@pytest.fixture
def create_nexus(mayastors, nexus_uuid, create_replica):
    hdls = mayastors
    replicas = [k.uri for k in create_replica]

    NEXUS_UUID, size_mb = nexus_uuid

    hdls["ms3"].nexus_create(NEXUS_UUID, 64 * 1024 * 1024, replicas)
    uri = hdls["ms3"].nexus_publish(NEXUS_UUID)

    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    assert len(hdls["ms1"].pool_list().pools) == 1
    assert len(hdls["ms2"].pool_list().pools) == 1

    yield uri
    hdls["ms3"].nexus_destroy(NEXUS_UUID)


@pytest.fixture
def create_nexus_v2(
    mayastors, nexus_name, nexus_uuid, create_replica, min_cntlid, resv_key
):
    hdls = mayastors
    replicas = [k.uri for k in create_replica]

    NEXUS_UUID, size_mb = nexus_uuid

    hdls["ms3"].nexus_create_v2(
        nexus_name,
        NEXUS_UUID,
        size_mb,
        min_cntlid,
        min_cntlid + 9,
        resv_key,
        0,
        replicas,
    )

    uri = hdls["ms3"].nexus_publish(nexus_name)

    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    assert len(hdls["ms1"].pool_list().pools) == 1
    assert len(hdls["ms2"].pool_list().pools) == 1

    yield uri
    hdls["ms3"].nexus_destroy(nexus_name)


@pytest.fixture
def create_nexus_2_v2(
    mayastors, nexus_name, nexus_uuid, min_cntlid_2, resv_key, resv_key_2
):
    """Create a 2nd nexus on ms0 with the same 2 replicas but with resv_key_2
    and preempt resv_key"""
    hdls = mayastors
    NEXUS_NAME = nexus_name

    replicas = []
    list = mayastors.get("ms3").nexus_list_v2()
    nexus = next(n for n in list if n.name == NEXUS_NAME)
    replicas.append(nexus.children[0].uri)
    replicas.append(nexus.children[1].uri)

    NEXUS_UUID, size_mb = nexus_uuid

    hdls["ms0"].nexus_create_v2(
        NEXUS_NAME,
        NEXUS_UUID,
        size_mb,
        min_cntlid_2,
        min_cntlid_2 + 9,
        resv_key_2,
        resv_key,
        replicas,
    )
    uri = hdls["ms0"].nexus_publish(NEXUS_NAME)
    assert len(hdls["ms0"].bdev_list()) == 1
    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    yield uri
    hdls["ms0"].nexus_destroy(nexus_name)


@pytest.fixture
def pool_config():
    """
    The idea is this used to obtain the pool types and names that should be
    created.
    """
    pool = {}
    pool["name"] = "tpool"
    pool["uri"] = "malloc:///disk0?size_mb=100"
    return pool


@pytest.fixture
def replica_uuid():
    """Replica UUID to be used."""
    UUID = "0000000-0000-0000-0000-000000000001"
    size_mb = 64 * 1024 * 1024
    return (UUID, size_mb)


@pytest.fixture
def nexus_name():
    """Nexus name to be used."""
    NEXUS_NAME = "nexus0"
    return NEXUS_NAME


@pytest.fixture
def nexus_uuid():
    """Nexus UUID's to be used."""
    NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"
    size_mb = 64 * 1024 * 1024
    return (NEXUS_UUID, size_mb)


@pytest.fixture
def min_cntlid():
    """NVMe minimum controller ID to be used."""
    min_cntlid = 50
    return min_cntlid


@pytest.fixture
def min_cntlid_2():
    """NVMe minimum controller ID to be used for 2nd nexus."""
    min_cntlid = 60
    return min_cntlid


@pytest.fixture
def resv_key():
    """NVMe reservation key to be used."""
    resv_key = 0xABCDEF0012345678
    return resv_key


@pytest.fixture
def resv_key_2():
    """NVMe reservation key to be used for 2nd nexus."""
    resv_key = 0x1234567890ABCDEF
    return resv_key


@pytest.fixture
def create_pools(containers, mayastors, pool_config):
    hdls = mayastors

    cfg = pool_config
    pools = []

    pools.append(hdls["ms1"].pool_create(cfg.get("name"), cfg.get("uri")))
    pools.append(hdls["ms2"].pool_create(cfg.get("name"), cfg.get("uri")))

    for p in pools:
        assert p.state == pb.POOL_ONLINE

    yield pools
    try:
        hdls["ms1"].pool_destroy(cfg.get("name"))
        hdls["ms2"].pool_destroy(cfg.get("name"))
    except Exception:
        pass


@pytest.fixture
def create_replica(mayastors, replica_uuid, create_pools):
    hdls = mayastors
    pools = create_pools
    replicas = []

    UUID, size_mb = replica_uuid

    replicas.append(hdls["ms1"].replica_create(pools[0].name, UUID, size_mb))
    replicas.append(hdls["ms2"].replica_create(pools[0].name, UUID, size_mb))

    yield replicas
    try:
        hdls["ms1"].replica_destroy(UUID)
        hdls["ms2"].replica_destroy(UUID)
    except Exception as e:
        logging.debug(e)


def test_enospace_on_volume(mayastors, create_replica):
    nodes = mayastors
    pools = []
    uuid = guid.uuid4()

    pools.append(nodes["ms2"].pools_as_uris()[0])
    pools.append(nodes["ms1"].pools_as_uris()[0])
    nexus_node = nodes["ms3"].as_target()

    v = Volume(uuid, nexus_node, pools, 100 * 1024 * 1024)

    with pytest.raises(grpc.RpcError) as error:
        v.create()
    assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED


async def kill_after(container, sec):
    """Kill the given container after sec seconds."""
    await asyncio.sleep(sec)
    logging.info(f"killing container {container}")
    container.kill()


@pytest.mark.asyncio
async def test_nexus_2_mirror_kill_one(containers, mayastors, create_nexus):

    uri = create_nexus
    nvme_discover(uri)
    dev = nvme_connect(uri)
    try:
        job = Fio("job1", "rw", dev).build()
        print(job)

        to_kill = containers.get("ms2")
        await asyncio.gather(run_cmd_async(job), kill_after(to_kill, 5))

    finally:
        # disconnect target before we shutdown
        nvme_disconnect(uri)


@pytest.mark.asyncio
async def test_nexus_2_remote_mirror_kill_one(
    containers, mayastors, nexus_uuid, create_nexus
):
    """
    This test does the following steps:

        - creates mayastor instances
        - creates pools on mayastor 1 and 2
        - creates replicas on those pools
        - creates a nexus on mayastor 3
        - starts fio on a remote VM (vixos1) for 15 secondsj
        - kills mayastor 2 after 4 seconds
        - assume the test to succeed
        - disconnect the VM from mayastor 3 when FIO completes
        - removes the nexus from mayastor 3
        - removes the replicas but as mayastor 2 is down, will swallow errors
        - removes the pool

    The bulk of this is done by reusing fixtures those fitures are not as
    generic as one might like at this point so look/determine if you need them
    to begin with.

    By yielding from fixtures, after the tests the function is resumed where
    yield is called.
    """

    uri = create_nexus
    dev = nvme_connect(uri)
    try:
        job = Fio("job1", "randwrite", dev).build()
        print(job)

        to_kill = containers.get("ms2")

        # create an event loop polling the async processes for completion
        await asyncio.gather(run_cmd_async(job), kill_after(to_kill, 4))

        list = mayastors.get("ms3").nexus_list()

        NEXUS_UUID, size_mb = nexus_uuid
        nexus = next(n for n in list if n.uuid == NEXUS_UUID)

        assert nexus.state == pb.NEXUS_DEGRADED
        assert nexus.children[1].state == pb.CHILD_DEGRADED

    finally:
        # disconnect target before we shutdown
        nvme_disconnect(uri)


@pytest.mark.asyncio
async def test_nexus_2_remote_mirror_kill_one_spdk(
    containers, mayastors, nexus_uuid, create_nexus
):
    """
    Identical to the previous test except fio uses the SPDK ioengine
    """

    uri = create_nexus

    job = FioSpdk("job1", "randwrite", uri).build()
    print(job)

    to_kill = containers.get("ms2")
    await asyncio.gather(run_cmd_async(job), kill_after(to_kill, 4))

    list = mayastors.get("ms3").nexus_list()

    NEXUS_UUID, _ = nexus_uuid
    nexus = next(n for n in list if n.uuid == NEXUS_UUID)

    assert nexus.state == pb.NEXUS_DEGRADED
    assert nexus.children[1].state == pb.CHILD_DEGRADED


@pytest.mark.asyncio
async def test_nexus_cntlid(create_nexus_v2, min_cntlid):
    """Test create_nexus_v2 NVMe controller ID"""

    uri = create_nexus_v2

    dev = nvme_connect(uri)
    try:
        id_ctrl = nvme_id_ctrl(dev)
        assert id_ctrl["cntlid"] == min_cntlid

    finally:
        # disconnect target before we shut down
        nvme_disconnect(uri)


def test_nexus_resv_key(create_nexus_v2, nexus_name, nexus_uuid, mayastors, resv_key):
    """Test create_nexus_v2 replica NVMe reservation key"""

    uri = create_nexus_v2
    NEXUS_UUID, _ = nexus_uuid

    list = mayastors.get("ms3").nexus_list_v2()
    nexus = next(n for n in list if n.name == nexus_name)
    assert nexus.uuid == NEXUS_UUID
    child_uri = nexus.children[0].uri

    dev = nvme_connect(child_uri)
    try:
        report = nvme_resv_report(dev)
        print(report)

        assert (
            report["rtype"] == 5
        ), "should have write exclusive, all registrants reservation"
        assert report["regctl"] == 1, "should have 1 registered controller"
        assert report["ptpls"] == 0, "should have Persist Through Power Loss State of 0"
        assert (
            report["regctlext"][0]["cntlid"] == 0xFFFF
        ), "should have dynamic controller ID"

        # reservation status reserved
        assert (report["regctlext"][0]["rcsts"] & 0x1) == 1
        assert report["regctlext"][0]["rkey"] == resv_key

    finally:
        nvme_disconnect(child_uri)


def test_nexus_preempt_key(
    create_nexus_v2,
    create_nexus_2_v2,
    nexus_name,
    nexus_uuid,
    mayastors,
    resv_key_2,
):
    """Create a nexus on ms3 and ms0, with the latter preempting the NVMe
    reservation key registered by ms3, verify that ms3 is no longer registered.
    Verify that writes succeed via the nexus on ms0 but not ms3."""

    NEXUS_UUID, _ = nexus_uuid

    list = mayastors.get("ms3").nexus_list_v2()
    nexus = next(n for n in list if n.name == nexus_name)
    assert nexus.uuid == NEXUS_UUID
    child_uri = nexus.children[0].uri
    assert nexus.state == pb.NEXUS_ONLINE
    assert nexus.children[0].state == pb.CHILD_ONLINE
    assert nexus.children[1].state == pb.CHILD_ONLINE

    dev = nvme_connect(child_uri)
    try:
        report = nvme_resv_report(dev)
        print(report)

        assert (
            report["rtype"] == 5
        ), "should have write exclusive, all registrants reservation"
        assert report["regctl"] == 1, "should have 1 registered controller"
        assert report["ptpls"] == 0, "should have Persist Through Power Loss State of 0"
        assert (
            report["regctlext"][0]["cntlid"] == 0xFFFF
        ), "should have dynamic controller ID"

        # reservation status reserved
        assert (report["regctlext"][0]["rcsts"] & 0x1) == 1
        assert report["regctlext"][0]["rkey"] == resv_key_2

    finally:
        nvme_disconnect(child_uri)

    # verify write with nexus on ms0
    uri = create_nexus_2_v2
    dev = nvme_connect(uri)
    job = "sudo dd if=/dev/urandom of={0} bs=512 count=1".format(dev)

    try:
        run_cmd(job)

    finally:
        nvme_disconnect(uri)

    list = mayastors.get("ms0").nexus_list_v2()
    nexus = next(n for n in list if n.name == nexus_name)
    assert nexus.state == pb.NEXUS_ONLINE
    assert nexus.children[0].state == pb.CHILD_ONLINE
    assert nexus.children[1].state == pb.CHILD_ONLINE

    # verify write error with nexus on ms3
    uri = create_nexus_v2
    dev = nvme_connect(uri)
    job = "sudo dd if=/dev/urandom of={0} bs=512 count=1".format(dev)

    try:
        run_cmd(job)

    finally:
        nvme_disconnect(uri)

    list = mayastors.get("ms3").nexus_list_v2()
    nexus = next(n for n in list if n.name == nexus_name)
    assert nexus.state == pb.NEXUS_FAULTED
    assert nexus.children[0].state == pb.CHILD_FAULTED
    assert nexus.children[1].state == pb.CHILD_FAULTED


@pytest.mark.asyncio
async def test_nexus_2_remote_mirror_kill_1(
    containers, mayastors, create_nexus, nexus_uuid
):
    """Create a nexus on ms3 with replicas on ms1 and ms2. Sleep for 10s. Kill
    ms2 after 4s, verify that the second child is degraded.
    """

    uri = create_nexus
    NEXUS_UUID, _ = nexus_uuid

    job = "sleep 10"

    try:
        # create an event loop polling the async processes for completion
        await asyncio.gather(
            run_cmd_async(job),
            kill_after(containers.get("ms2"), 4),
        )
    except Exception as e:
        raise (e)
    finally:
        list = mayastors.get("ms3").nexus_list()
        nexus = next(n for n in list if n.uuid == NEXUS_UUID)

        assert nexus.state == pb.NEXUS_DEGRADED

        assert nexus.children[0].state == pb.CHILD_ONLINE
        assert nexus.children[1].state == pb.CHILD_DEGRADED


@pytest.mark.asyncio
async def test_nexus_2_remote_mirror_kill_all_fio(
    containers, mayastors, create_nexus, nexus_uuid
):
    """Create a nexus on ms3 with replicas on ms1 and ms2. Start fio_spdk for
    15s. Kill ms2 after 4s, ms1 after 4s. Assume the fail with a
    ChildProcessError is due to fio bailing out. Remove the nexus from ms3.
    """

    uri = create_nexus
    NEXUS_UUID, _ = nexus_uuid

    job = FioSpdk("job1", "randwrite", uri).build()

    try:
        # create an event loop polling the async processes for completion
        await asyncio.gather(
            run_cmd_async(job),
            kill_after(containers.get("ms2"), 4),
            kill_after(containers.get("ms1"), 4),
        )
    except ChildProcessError:
        pass
    except Exception as e:
        # if it's not a child process error fail the test
        raise (e)
    finally:
        list = mayastors.get("ms3").nexus_list()
        nexus = next(n for n in list if n.uuid == NEXUS_UUID)

        assert nexus.state == pb.NEXUS_FAULTED

        assert nexus.children[0].state == pb.CHILD_DEGRADED
        assert nexus.children[1].state == pb.CHILD_DEGRADED
