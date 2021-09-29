import pytest
import logging
from pytest_bdd import given, scenario, then, when, parsers

from common.command import run_cmd
from common.fio import Fio
from common.mayastor import container_mod, mayastor_mod

import grpc
import mayastor_pb2 as pb
from common.nvme import (
    nvme_connect,
    nvme_disconnect,
    nvme_list_subsystems,
)


@scenario(
    "features/nexus-multipath.feature", "running IO against an ANA NVMe controller"
)
def test_running_io_against_ana_nvme_ctrlr():
    "Running IO against an ANA NVMe controller."


@pytest.fixture(scope="module")
def create_nexus(
    mayastor_mod, nexus_name, nexus_uuid, create_replica, min_cntlid, resv_key
):
    """ Create a nexus on ms3 with 2 replicas """
    hdls = mayastor_mod
    replicas = create_replica
    replicas = [k.uri for k in replicas]

    NEXUS_UUID, size_mb = nexus_uuid
    NEXUS_NAME = nexus_name

    hdls["ms3"].nexus_create_v2(
        NEXUS_NAME,
        NEXUS_UUID,
        size_mb,
        min_cntlid,
        min_cntlid + 9,
        resv_key,
        0,
        replicas,
    )
    uri = hdls["ms3"].nexus_publish(NEXUS_NAME)
    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    assert len(hdls["ms1"].pool_list().pools) == 1
    assert len(hdls["ms2"].pool_list().pools) == 1

    dev = nvme_connect(uri)

    yield dev
    nvme_disconnect(uri)
    hdls["ms3"].nexus_destroy(NEXUS_NAME)


@pytest.fixture(scope="module")
def create_nexus_2(mayastor_mod, nexus_name, nexus_uuid, min_cntlid_2, resv_key_2):
    """ Create a 2nd nexus on ms0 with the same 2 replicas but with resv_key_2 """
    hdls = mayastor_mod
    NEXUS_NAME = nexus_name

    replicas = []
    list = mayastor_mod.get("ms3").nexus_list_v2()
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
        0,
        replicas,
    )
    uri = hdls["ms0"].nexus_publish(NEXUS_NAME)
    assert len(hdls["ms0"].bdev_list()) == 1
    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    dev = nvme_connect(uri)

    yield dev
    nvme_disconnect(uri)
    hdls["ms0"].nexus_destroy(NEXUS_NAME)


@pytest.fixture(scope="module")
def pool_config():
    pool = {}
    pool["name"] = "tpool"
    pool["uri"] = "malloc:///disk0?size_mb=100"
    return pool


@pytest.fixture(scope="module")
def replica_uuid():
    UUID = "0000000-0000-0000-0000-000000000001"
    size_mb = 64 * 1024 * 1024
    return (UUID, size_mb)


@pytest.fixture(scope="module")
def nexus_name():
    NEXUS_NAME = "nexus0"
    return NEXUS_NAME


@pytest.fixture(scope="module")
def nexus_uuid():
    NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"
    size_mb = 64 * 1024 * 1024
    return (NEXUS_UUID, size_mb)


@pytest.fixture(scope="module")
def min_cntlid():
    """NVMe minimum controller ID."""
    min_cntlid = 50
    return min_cntlid


@pytest.fixture(scope="module")
def min_cntlid_2():
    """NVMe minimum controller ID for 2nd nexus."""
    min_cntlid = 60
    return min_cntlid


@pytest.fixture(scope="module")
def resv_key():
    """NVMe reservation key."""
    resv_key = 0xABCDEF0012345678
    return resv_key


@pytest.fixture(scope="module")
def resv_key_2():
    """NVMe reservation key for 2nd nexus."""
    resv_key = 0x1234567890ABCDEF
    return resv_key


@pytest.fixture(scope="module")
def create_pools(mayastor_mod, pool_config):
    hdls = mayastor_mod

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


@pytest.fixture(scope="module")
def create_replica(mayastor_mod, replica_uuid, create_pools):
    hdls = mayastor_mod
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


@given(
    "a client connected to two controllers to the same namespace",
    target_fixture="get_nvme_client",
)
def get_nvme_client(create_nexus, create_nexus_2):
    dev = create_nexus
    dev2 = create_nexus_2
    return dev, dev2


@given("both controllers are online")
def check_controllers_online(get_nvme_client):
    devs = get_nvme_client
    assert devs[0] == devs[1], "should have one namespace"
    desc = nvme_list_subsystems(devs[0])
    paths = desc["Subsystems"][0]["Paths"]
    assert len(paths) == 2, "should have 2 paths"

    for p in paths:
        assert p["State"] == "live"


@when("I start Fio")
def start_io(get_nvme_client):
    devs = get_nvme_client
    job = Fio("job1", "randwrite", devs[0]).build()
    run_cmd(job)


@then("I should be able to see IO flowing to one path only")
def check_io_one_path(mayastor_mod):
    hdls = mayastor_mod

    # default NUMA io_policy has all IO going to the first controller
    stat = hdls["ms3"].stat_nvme_controllers()
    assert stat[0].stats.num_write_ops > 1000
    assert stat[1].stats.num_write_ops > 1000

    stat = hdls["ms0"].stat_nvme_controllers()
    assert stat[0].stats.num_write_ops == 0
    assert stat[1].stats.num_write_ops == 0


@given("a local mayastor instance")
def local_mayastor_instance(nexus_instance):
    pass


@given("a remote mayastor instance")
def remote_mayastor_instance(remote_instance):
    pass


@pytest.fixture(scope="module")
def remote_instance():
    yield "ms0"


@pytest.fixture(scope="module")
def nexus_instance():
    yield "ms1"
