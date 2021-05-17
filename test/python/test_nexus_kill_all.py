"""Test that will delete all replica's while under load from the nexus."""

import logging
import pytest
import asyncio
from common.nvme import nvme_remote_disconnect, nvme_remote_connect
from common.fio import Fio
from common.command import run_cmd_async_at
import mayastor_pb2 as pb

NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"


async def kill_after(container, sec):
    """Kill the given container after sec seconds."""
    await asyncio.sleep(sec)
    logging.info(f"killing container {container}")
    container.kill()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_nexus_2_remote_mirror_kill_all(
        container_ref, wait_for_mayastor, create_nexus):

    """

    This test does the following steps:

        - creates mayastor instances
        - creates pools on mayastor 1 and 2
        - creates replicas on those pools
        - creates a nexus on mayastor 3
        - starts fio on a remote VM (vixos1) for 15 secondsj
        - kills mayastor 2 after 4 seconds
        - kills mayastor 1 after 5 seconds
        - assume the fail with a ChildProcessError due to Fio bailing out
        - disconnect the VM from mayastor 3 when has failed
        - removes the nexus from mayastor 3
    """

    containers = container_ref
    uri = create_nexus

    dev = await nvme_remote_connect("vixos1", uri)

    job = Fio("job1", "rw", dev).build()

    try:
        # create an event loop polling the async processes for completion
        await asyncio.gather(
            run_cmd_async_at("vixos1", job),
            kill_after(containers.get("ms2"), 4),
            kill_after(containers.get("ms1"), 5))
    except ChildProcessError:
        pass
    except Exception as e:
        # if its not a child processe error fail the test
        raise(e)
    finally:
        list = wait_for_mayastor.get("ms3").nexus_list()
        nexus = next(n for n in list if n.uuid == NEXUS_UUID)

        assert nexus.state == pb.NEXUS_FAULTED

        nexus.children[0].state == pb.CHILD_FAULTED
        nexus.children[1].state == pb.CHILD_FAULTED

        # disconnect the VM from our target before we shutdown
        await nvme_remote_disconnect("vixos1", uri)
