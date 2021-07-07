import pytest
from common.mayastor import containers_mod, mayastor_mod
from common.msclient import get_msclient
import mayastor_pb2 as pb
import uuid
from urllib.parse import urlparse
from common.command import run_cmd_async
from common.fio_spdk import FioSpdk


POOL_NAME = "pool1"
NEXUS_GUID = "9febdeb9-cb33-4166-a89d-254b810ba34a"


@pytest.fixture
def create_replicas(mayastor_mod):
    ms1 = mayastor_mod.get("ms1")
    ms2 = mayastor_mod.get("ms2")

    replicas = []

    for m in (ms1, ms2):
        p = m.pool_create(POOL_NAME, "malloc:///disk0?size_mb=100")
        assert p.state == pb.POOL_ONLINE
        r = m.replica_create(POOL_NAME, str(uuid.uuid4()), 32 * 1024 * 1024)
        replicas.append(r.uri)
    yield replicas
    try:
        for m in (ms1, ms2):
            m.pool_destroy(POOL_NAME)
    except Exception:
        pass


@pytest.fixture
def create_nexus(mayastor_mod, create_replicas):
    ms3 = mayastor_mod.get("ms3")
    ms3.nexus_create(NEXUS_GUID, 32 * 1024 * 1024, create_replicas)
    uri = ms3.nexus_publish(NEXUS_GUID)
    yield uri
    ms3.nexus_destroy(NEXUS_GUID)


def assure_controllers(mscli, replicas):
    """Check that target mayastor contains all the given controllers."""
    output = mscli("controller", "list")
    assert len(output["controllers"]) == len(replicas)
    names = [c["name"] for c in output["controllers"]]

    for r in replicas:
        c = ctrl_name_from_uri(r)
        assert c in names, "Controller for replica %s not found" % c


def ctrl_name_from_uri(uri):
    """Form controller name from the full replica URL."""
    u = urlparse(uri)
    return "%s%sn1" % (u.netloc, u.path)


@pytest.mark.asyncio
async def test_controller_list(create_replicas, create_nexus, mayastor_mod):
    replica1 = mayastor_mod.get("ms1")
    replica2 = mayastor_mod.get("ms2")
    replicas = [replica1, replica2]
    nexus = mayastor_mod.get("ms3")
    mscli = get_msclient().with_json_output()

    # Should not see any controllers on replica instances.
    for r in replicas:
        output = mscli.with_url(r.ip_address())("controller", "list")
        assert len(output["controllers"]) == 0

    # Should see exactly 2 controllers on the nexus instance.
    mscli.with_url(nexus.ip_address())
    assure_controllers(mscli, create_replicas)

    # Should not see a controller for the removed replica.
    nexus.nexus_remove_replica(NEXUS_GUID, create_replicas[0])
    assure_controllers(mscli, create_replicas[1:])

    # Should see controller for the newly added replica.
    nexus.nexus_add_replica(NEXUS_GUID, create_replicas[0], True)
    assure_controllers(mscli, create_replicas)


@pytest.mark.asyncio
async def test_controller_stats(
    create_replicas, create_nexus, mayastor_mod, containers_mod
):
    nexus = mayastor_mod.get("ms3")
    mscli = get_msclient().with_json_output().with_url(nexus.ip_address())

    # Should see exactly 2 controllers on the nexus instance.
    assure_controllers(mscli, create_replicas)

    # Check that stats exist for all controllers and are initially empty.
    output = mscli("controller", "stats")
    assert len(output["controllers"]) == len(create_replicas)
    names = [c["name"] for c in output["controllers"]]

    for r in create_replicas:
        c = ctrl_name_from_uri(r)
        assert c in names, "Controller for replica %s not found" % c

    for s in output["controllers"]:
        stats = s["stats"]
        for n in stats.keys():
            assert stats[n] == 0, "Stat %s is not zero for a new controller" % n

    # Issue I/O to replicas and make sure stats reflect that.
    job = FioSpdk("job1", "readwrite", create_nexus, runtime=5).build()
    await run_cmd_async(job)

    target_stats = ["num_read_ops", "num_write_ops", "bytes_read", "bytes_written"]
    cached_stats = {"num_write_ops": 0, "bytes_written": 0}

    output = mscli("controller", "stats")
    assert len(output["controllers"]) == 2
    for c in output["controllers"]:
        stats = c["stats"]

        # Make sure all related I/O stats are counted.
        for s in target_stats:
            assert stats[s] > 0, "I/O stat %s is zero after active I/O operations" % s

        # Make sure IOPS and number of bytes are sane (fio uses 4k block).
        assert (
            stats["bytes_read"] == stats["num_read_ops"] * 4096
        ), "Read IOPs don't match number of bytes"
        assert (
            stats["bytes_written"] == stats["num_write_ops"] * 4096
        ), "Write IOPs don't match number of bytes"

        # Check that write-related stats are equal for all controllers in the same nexus
        # and make sure all unrelated stats remained untouched.
        for s in stats.keys():
            if s in cached_stats:
                # Cache I/O stat to check across all controllers against the same value.
                if cached_stats[s] == 0:
                    cached_stats[s] = stats[s]
                else:
                    # I/O stats for all controllers in a nexus must be equal.
                    assert cached_stats[s] == stats[s], (
                        "I/O statistics %s for replicas mismatch" % s
                    )
            elif s not in target_stats:
                assert stats[s] == 0, "Unrelated I/O stat %s got impacted by I/O" % s
