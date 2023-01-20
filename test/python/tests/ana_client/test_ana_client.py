import pytest
from common.mayastor import container_mod, mayastor_mod
from common.nvme import (
    nvme_connect,
    nvme_disconnect,
    nvme_disconnect_all,
    nvme_list_subsystems,
    identify_namespace,
)
import uuid
import mayastor_pb2 as pb
import os
import glob


POOL_NAME = "pool1"
NEXUS_GUID = "afebdeb9-ff44-1111-2222-254f810ba34a"


@pytest.fixture
def create_replicas(mayastor_mod):
    ms0 = mayastor_mod.get("ms0")
    ms1 = mayastor_mod.get("ms1")

    replicas = []

    for m in (ms0, ms1):
        p = m.pool_create(POOL_NAME, "malloc:///disk0?size_mb=100")
        assert p.state == pb.POOL_ONLINE
        r = m.replica_create(POOL_NAME, str(uuid.uuid4()), 32 * 1024 * 1024)
        replicas.append(r.uri)

    yield replicas

    for m in (ms0, ms1):
        try:
            m.pool_destroy(POOL_NAME)
        except Exception:
            pass


@pytest.fixture
def create_nexuses(mayastor_mod, create_replicas):
    uris = []

    nvme_disconnect_all()

    for n in ["ms2", "ms3"]:
        ms = mayastor_mod.get(n)
        ms.nexus_create(NEXUS_GUID, 32 * 1024 * 1024, create_replicas)
        uri = ms.nexus_publish(NEXUS_GUID)
        uris.append(uri)

    yield uris

    nvme_disconnect_all()

    for n in ["ms2", "ms3"]:
        ms = mayastor_mod.get(n)
        ms.nexus_destroy(NEXUS_GUID)


def connect_multipath_nexuses(uris):
    dev1 = nvme_connect(uris[0])
    dev2 = None

    try:
        dev2 = nvme_connect(uris[1])
    except Exception:
        # The first connect is allowed to fail due to controller ID collision.
        pass

    if dev2 is None:
        dev2 = nvme_connect(uris[1])

    return (dev1, dev2)


@pytest.mark.asyncio
async def test_io_policy(create_replicas, create_nexuses, mayastor_mod):
    devs = connect_multipath_nexuses(create_nexuses)
    assert devs[0] == devs[1], "Paths are different for multipath nexus"

    # Make sure all we see exactly 2 paths and all paths are 'live optimized'
    device = devs[0]
    descr = nvme_list_subsystems(device)
    paths = descr["Subsystems"][0]["Paths"]
    assert len(paths) == 2, "Number of paths to Nexus mismatches"

    for p in paths:
        assert p["State"] == "live"
        assert p["ANAState"] == "optimized"

    # Make sure there are 2 virtual NVMe controllers for the namespace.
    ns = os.path.basename(device)
    cname = ns.replace("n1", "c*n1")
    vctls = glob.glob("/sys/block/%s" % cname)
    assert len(vctls) == 2, "Namespace must have 2 virtual NVMe controllers"
    for cpath in vctls:
        l = os.readlink(cpath)
        assert l.startswith(
            "../devices/virtual/nvme-fabrics/ctl/"
        ), "Path device is not a virtual controller"

    # Make sure virtual NVMe namespace exists for multipath nexus.
    l = os.readlink("/sys/block/%s" % ns)
    assert l.startswith(
        "../devices/virtual/nvme-subsystem/nvme-subsys"
    ), "No virtual NVMe subsystem exists for multipath Nexus"

    # Make sure I/O policy is NUMA.
    subsys = descr["Subsystems"][0]["Name"]
    pfile = "/sys/class/nvme-subsystem/%s/iopolicy" % subsys
    assert os.path.isfile(pfile), "No iopolicy file exists"
    with open(pfile) as f:
        iopolicy = f.read().strip()
        assert iopolicy == "numa", "I/O policy is not NUMA"

    # Make sure ANA state is reported properly for both nexuses.
    for n in ["ms2", "ms3"]:
        ms = mayastor_mod.get(n)
        nexuses = ms.nexus_list_v2()
        assert len(nexuses) == 1, "Number of nexuses mismatches"
        assert (
            nexuses[0].ana_state == pb.NVME_ANA_OPTIMIZED_STATE
        ), "ANA state of nexus mismatches"


@pytest.mark.asyncio
async def test_namespace_guid(create_replicas, create_nexuses, mayastor_mod):
    uri = create_nexuses[0]
    device = nvme_connect(uri)
    ns = identify_namespace(device)
    nvme_disconnect(uri)

    # Namespace's GUID must match Nexus GUID.
    assert uuid.UUID(ns["nguid"]) == uuid.UUID(
        NEXUS_GUID
    ), "Namespace NGID doesn't match Nexus GUID"

    # Extended Unique Identifier must be zero.
    assert ns["eui64"] == "0000000000000000", "Namespace EUI64 is not zero"
