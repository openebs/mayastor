"""Test that will delete all replica's while under load from the nexus."""

import logging
import pytest
import asyncio
from common.nvme import nvme_discover, nvme_connect, nvme_disconnect
from common.fio import Fio


@pytest.mark.xfail
@pytest.mark.timeout(60)
def test_nexus_2_mirror_kill_all(container_ref, create_nexus):
    """Kill the given container after sec seconds."""
    async def kill_after(container, sec):
        await asyncio.sleep(sec)
        logging.info(f"killing container {container}")
        containers.get(container).kill()

    containers = container_ref
    uri = create_nexus

    # XXX this needs to go to VMs
    nvme_discover(uri)
    dev = nvme_connect(uri)

    job = Fio("job1", "rw", dev).run()
    kill_ms1 = kill_after("ms1", 5)
    kill_ms2 = kill_after("ms2", 6)

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(asyncio.gather(job, kill_ms1, kill_ms2))
    except ChildProcessError:
        pass
    finally:
        nvme_disconnect(uri)
