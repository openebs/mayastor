import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

from common.nvme import nvme_connect, nvme_disconnect, nvme_disconnect_all

from v1.mayastor import container_mod, mayastor_mod

from common.fio import Fio
import time
import grpc
import uuid as guid
import pool_pb2 as pool_pb
import replica_pb2 as replica_pb
import common_pb2 as common_pb
import nexus_pb2 as nexus_pb
import subprocess

FIO_RUNTIME = 5
SNAP_ENTITY_ATTR = "entity_id"
SNAP_TXN_ATTR = "txn_id"
SNAP_NAME_ATTR = "snapshot_name"
SNAP_UUID_AATR = "snapshot_uuid"
SNAP_REPLICA_UUID = "af68d693-e9cd-4846-9726-4178d8823cee"


@scenario(
    "features/snapshot.feature",
    "creating a snapshot for published healthy one replica nexus with active I/O",
)
def test_one_replica_nexus_fio_snapshot(setup):
    """creating a snapshot for published healthy one replica nexus with active I/O"""


# """
#  Given a published one replica nexus
#  Given fio client is performing I/O on the published nexus
#  When a nexus snapshot is created
#  Then snapshot should be created on all nexus replicas
#  And fio should complete without I/O errors
# """


@given("a published one replica nexus")
def given_a_published_one_replica_nexus(connect_nexus_1):
    pass


@given(
    "fio client is performing I/O on the published nexus",
    target_fixture="start_fio_client",
)
def start_fio_client(connect_nexus_1):
    dev = connect_nexus_1
    fio = Fio("job1", "randwrite", dev, runtime=FIO_RUNTIME).build()
    f = subprocess.Popen(fio, shell=True)
    yield f
    f.communicate()


@when("a nexus snapshot is created")
def create_nexus_snapshot(
    connect_nexus_1, mayastor_mod, nexus_cfg, replica_cfg, snapshot1_cfg
):
    replicas = [[replica_cfg[0], snapshot1_cfg[SNAP_UUID_AATR]]]

    return mayastor_mod["ms3"].nexus_create_snapshot(
        nexus_cfg[0],
        snapshot1_cfg[SNAP_ENTITY_ATTR],
        snapshot1_cfg[SNAP_TXN_ATTR],
        snapshot1_cfg[SNAP_NAME_ATTR],
        replicas,
        [],
    )


@then("snapshot should be created on all nexus replicas")
def check_nexus_snapshot(connect_nexus_1, snapshot1_cfg, mayastor_mod, replica_cfg):
    snapshots = mayastor_mod["ms1"].list_snapshots()
    assert len(snapshots.snapshots) == 1, "No replica snapshot found"
    snapshot = snapshots.snapshots[0]

    for a in [SNAP_UUID_AATR, SNAP_NAME_ATTR, SNAP_ENTITY_ATTR, SNAP_TXN_ATTR]:
        assert getattr(snapshot, a) == snapshot1_cfg[a], (
            "Snapshot attribute '%s' mismatches" % a
        )


@then("fio should complete without I/O errors")
def check_fio_completion(start_fio_client):
    start_fio_client.wait()
    assert start_fio_client.returncode == 0


@pytest.fixture(scope="module")
def setup(container_mod):
    nvme_disconnect_all()
    yield
    nvme_disconnect_all()


@pytest.fixture
def snapshot1_cfg():
    cfg = {}

    cfg[SNAP_ENTITY_ATTR] = "71f81dfb-6896-4085-b76d-2957a19266f6"
    cfg[SNAP_TXN_ATTR] = "8ec22d5e-5a1d-44b8-8f22-d9e48b7ec781"
    cfg[SNAP_UUID_AATR] = "d6dabcb4-ca97-49c1-85ff-4e393ad974b7"
    cfg[SNAP_NAME_ATTR] = "snapshot1"

    return cfg


@pytest.fixture
def min_cntlid():
    """NVMe minimum controller ID to be used."""
    min_cntlid = 25
    return min_cntlid


@pytest.fixture
def replica_cfg():
    """Replica UUID to be used."""
    UUID = "cd2cecdc-1e49-49ca-993d-92606155a4ad"
    size_mb = 64 * 1024 * 1024
    return (UUID, size_mb)


@pytest.fixture
def replica_name():
    return "replica1"


@pytest.fixture
def nexus_cfg():
    """Nexus UUID to be used."""
    NEXUS_UUID = "af68d693-e9cd-4846-9726-4178d8823cee"
    size_mb = 64 * 1024 * 1024
    return (NEXUS_UUID, size_mb)


@pytest.fixture
def create_nexus_1(create_nexus_1_no_destroy, mayastor_mod, nexus_name):
    hdls = mayastor_mod
    NEXUS_NAME = nexus_name
    uri = create_nexus_1_no_destroy
    yield uri
    hdls["ms3"].nexus_destroy(NEXUS_NAME)


@pytest.fixture
def connect_nexus_1(create_nexus_1_no_destroy):
    dev = nvme_connect(create_nexus_1_no_destroy)
    return dev


@pytest.fixture
def resv_key():
    """NVMe reservation key to be used."""
    resv_key = 0xABCDEF0012345678
    return resv_key


@pytest.fixture
def create_nexus_1_no_destroy(
    mayastor_mod, nexus_name, nexus_cfg, create_replica, min_cntlid, resv_key
):
    """Create a nexus on ms3 with 2 replicas"""
    hdls = mayastor_mod
    replicas = create_replica
    replicas = [k.uri for k in replicas]

    NEXUS_UUID, size_mb = nexus_cfg
    NEXUS_NAME = nexus_name

    hdls["ms3"].nexus_create(
        NEXUS_NAME,
        NEXUS_UUID,
        size_mb,
        min_cntlid,
        min_cntlid + 9,
        resv_key,
        0,
        replicas,
    )
    uri = hdls["ms3"].nexus_publish(NEXUS_UUID)
    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    assert len(hdls["ms1"].pool_list().pools) == 1
    assert len(hdls["ms2"].pool_list().pools) == 1

    return uri


@pytest.fixture
def create_replica(mayastor_mod, replica_cfg, create_pools, replica_name):
    hdls = mayastor_mod
    pools = create_pools
    replicas = []

    UUID, size_mb = replica_cfg

    replicas.append(
        hdls["ms1"].replica_create(pools[0].uuid, replica_name, UUID, size_mb)
    )

    yield replicas
    try:
        hdls["ms1"].replica_destroy(UUID)
    except Exception as e:
        pass


@pytest.fixture
def create_pools(mayastor_mod, pool_config):
    hdls = mayastor_mod

    cfg = pool_config
    pools = []

    pools.append(
        hdls["ms1"].pool_create(cfg.get("name"), cfg.get("uuid"), cfg.get("disks"))
    )
    pools.append(
        hdls["ms2"].pool_create(cfg.get("name"), cfg.get("uuid"), cfg.get("disks"))
    )

    for p in pools:
        assert p.state == pool_pb.POOL_ONLINE
    yield pools
    try:
        hdls["ms1"].pool_destroy(cfg.get("name"))
        hdls["ms2"].pool_destroy(cfg.get("name"))
    except Exception:
        pass


def pool_uuid():
    return "1c2a1220-f297-4d3a-a57a-5a8bac21d128"


@pytest.fixture
def pool_config():
    pool = {}
    pool["name"] = "tpool"
    pool["uuid"] = pool_uuid()
    pool["disks"] = ["malloc:///disk0?size_mb=128"]
    return pool


@pytest.fixture
def nexus_name():
    """Nexus name to be used."""
    NEXUS_NAME = "nexus0"
    return NEXUS_NAME
