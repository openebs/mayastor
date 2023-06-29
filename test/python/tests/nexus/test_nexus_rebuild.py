from common.hdl import MayastorHandle
from common.command import run_cmd, run_cmd_async
from common.nvme import nvme_connect, nvme_disconnect
from common.fio import Fio
from common.fio_spdk import FioSpdk
from common.mayastor import containers, mayastors, create_temp_files, check_size
import pytest
import asyncio
import uuid as guid
import time
import subprocess
import mayastor_pb2 as pb

NEXUS_COUNT = 10
NEXUS_SIZE = 500 * 1024 * 1024
REPL_SIZE = NEXUS_SIZE
POOL_SIZE = REPL_SIZE * NEXUS_COUNT + 100 * 1024 * 1024


@pytest.fixture
def local_files(mayastors):
    files = []
    for name, ms in mayastors.items():
        path = f"/tmp/disk-{name}.img"
        pool_size_mb = int(POOL_SIZE / 1024 / 1024)
        subprocess.run(
            ["sudo", "sh", "-c", f"rm -f '{path}'; truncate -s {pool_size_mb}M '{path}'"],
            check=True,
        )
        files.append(path)

    yield
    for path in files:
        subprocess.run(["sudo", "rm", "-f", path], check=True)


@pytest.fixture
def create_replicas_on_all_nodes(local_files, mayastors, create_temp_files):
    uuids = []

    for name, ms in mayastors.items():
        ms.pool_create(name, f"aio:///tmp/disk-{name}.img")
        # verify we have zero replicas
        assert len(ms.replica_list().replicas) == 0

    for i in range(NEXUS_COUNT):
        uuid = guid.uuid4()
        for name, ms in mayastors.items():
            before = ms.pool_list()
            ms.replica_create(name, uuid, REPL_SIZE)
            after = ms.pool_list()
        uuids.append(uuid)

    yield uuids


@pytest.fixture
def create_nexuses(mayastors, create_replicas_on_all_nodes):
    nexuses = []
    nexuses_uris = []

    uris = [
        [replica.uri for replica in mayastors.get(node).replica_list().replicas]
        for node in ["ms1", "ms2", "ms3"]
    ]
    
    ms = mayastors.get("ms0")
    for children in zip(*uris):
        uuid = guid.uuid4()
        nexus = ms.nexus_create(uuid, NEXUS_SIZE, list(children))
        nexuses.append(nexus)
        nexuses_uris.append(ms.nexus_publish(uuid))

    yield nexuses


@pytest.mark.parametrize("times", range(10))
def test_rebuild_failure(containers, mayastors, times, create_nexuses):
    ms0 = mayastors.get("ms0")
    ms3 = mayastors.get("ms3")

    # Restart container with replica #3 (ms3).
    node3 = containers.get("ms3")
    node3.stop()
    time.sleep(5)
    node3.start()

    # Reconnect ms3, and import the existing pool.
    ms3.reconnect()
    ms3.pool_create("ms1", "aio:///tmp/disk-ms1.img")
    time.sleep(1)

    # Add the replicas to the nexuses for rebuild.
    for (idx, nexus) in enumerate(ms0.nexus_list()):
        child = list(filter(lambda child: child.state == pb.CHILD_FAULTED, list(nexus.children)))[0]
        if nexus.state != pb.NEXUS_FAULTED:
            try:
                ms0.nexus_remove_replica(nexus.uuid, child.uri)
                ms0.nexus_add_replica(nexus.uuid, child.uri, False)
            except:
                print(f"Failed to remove child {child.uri} from {nexus}")

    time.sleep(5)

    rebuilds = 0
    for nexus in ms0.nexus_list():
        for child in nexus.children:
            if child.rebuild_progress > -1:
                rebuilds += 1
                print("nexus", nexus.uuid, "rebuilding", child.uri, f"{child.rebuild_progress}")

    assert rebuilds > 0

    # Stop ms3 again. Rebuild jobs in progress must terminate.
    node3.stop()

    time.sleep(10)

    # All rebuild jobs must finish.
    for nexus in ms0.nexus_list():
        for child in nexus.children:
            assert child.rebuild_progress == -1
        ms0.nexus_destroy(nexus.uuid)
