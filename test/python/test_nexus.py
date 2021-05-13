import logging
import pytest
import mayastor_pb2 as pb
import uuid as guid
import grpc
import asyncio
from common.nvme import nvme_discover, nvme_connect, nvme_disconnect
from common.volume import Volume
from common.fio import Fio
UUID = "0000000-0000-0000-0000-000000000001"
NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"


@pytest.mark.skip
@pytest.fixture
def destroy_all(wait_for_mayastor):
    hdls = wait_for_mayastor

    hdls["ms3"].nexus_destroy(NEXUS_UUID)

    hdls["ms1"].replica_destroy(UUID)
    hdls["ms2"].replica_destroy(UUID)

    hdls["ms1"].pool_destroy("tpool")
    hdls["ms2"].pool_destroy("tpool")

    hdls["ms1"].replica_destroy(UUID)
    hdls["ms2"].replica_destroy(UUID)
    hdls["ms3"].nexus_destroy(NEXUS_UUID)

    hdls["ms1"].pool_destroy("tpool")
    hdls["ms2"].pool_destroy("tpool")

    assert len(hdls["ms1"].pool_list().pools) == 0
    assert len(hdls["ms2"].pool_list().pools) == 0

    assert len(hdls["ms1"].bdev_list().bdevs) == 0
    assert len(hdls["ms2"].bdev_list().bdevs) == 0
    assert len(hdls["ms3"].bdev_list().bdevs) == 0


@pytest.mark.skip
def test_multi_volume_local(wait_for_mayastor, create_aio_pools):
    hdls = wait_for_mayastor
    # contains the replicas

    ms = hdls.get('ms1')

    for i in range(6):
        uuid = guid.uuid4()
        replicas = []

        ms.replica_create("tpool", uuid, 8 * 1024 * 1024)

        replicas.append("bdev:///{}".format(uuid))
        print(ms.nexus_create(uuid, 4 * 1024 * 1024, replicas))


@pytest.mark.parametrize("times", range(50))
@pytest.mark.skip
def test_create_nexus_with_two_replica(times, create_nexus):
    nexus, uri, hdls = create_nexus
    nvme_discover(uri.device_uri)
    device = nvme_connect(uri.device_uri)
    nvme_disconnect(uri.device_uri)
    destroy_all


@pytest.mark.skip
def test_enospace_on_volume(wait_for_mayastor, create_pools):
    nodes = wait_for_mayastor
    pools = []
    uuid = guid.uuid4()

    pools.append(nodes["ms2"].pools_as_uris()[0])
    pools.append(nodes["ms1"].pools_as_uris()[0])
    nexus_node = nodes["ms3"].as_target()

    v = Volume(uuid, nexus_node, pools, 100 * 1024 * 1024)

    with pytest.raises(grpc.RpcError, match='RESOURCE_EXHAUSTED'):
        _ = v.create()
    print("expected failed")


@pytest.mark.timeout(60)
def test_nexus_2_mirror_kill_one(container_ref, create_nexus):
    """Kill the given container after sec seconds."""
    async def kill_after(container, sec):
        await asyncio.sleep(sec)
        logging.info(f"killing container {container}")
        containers.get(container).kill()

    containers = container_ref
    uri = create_nexus
    print(uri)

    # XXX this needs to go to VMs
    nvme_discover(uri)
    dev = nvme_connect(uri)

    job = Fio("job1", "rw", dev).run()

    kill_ms1 = kill_after("ms1", 5)

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    loop.run_until_complete(asyncio.gather(job, kill_ms1))

    nvme_disconnect(uri)
