import pytest
import logging
from pytest_bdd import given, scenario, then, when, parsers

from common.command import run_cmd
from common.fio import Fio
from common.mayastor import container_mod, mayastor_mod

import grpc
import subprocess
import mayastor_pb2 as pb
from common.nvme import (
    nvme_connect,
    nvme_disconnect_all,
    nvme_list_subsystems,
    nvme_delete_controller,
)

FIO_RUNTIME = 5
NEXUS_NAME = "nexus0"
NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"
NEXUS_SIZE = 64 * 1024 * 1024
REPLICA_UUID = "0000000-0000-0000-0000-000000000001"
REPLICA_SIZE = 62 * 1024 * 1024
POOL_NAME = "pool1"
POOL_DEV_URI = "malloc:///disk0?size_mb=100"


@pytest.fixture(scope="module")
def setup(container_mod):
    nvme_disconnect_all()
    yield
    nvme_disconnect_all()


@scenario("features/shutdown.feature", "shutting down a nexus with active fio client")
def test_shutting_down_a_nexus_with_active_fio_client(setup):
    """shutting down a nexus with active fio client"""


@scenario("features/shutdown.feature", "idempotency of nexus shutdown")
def test_nexus_shutdown_idempotency(setup):
    """idempotency of nexus shutdown"""


@given("a published and connected nexus", target_fixture="get_connected_nexus")
def given_a_published_nexus(create_connected_nexus):
    return create_connected_nexus


@given("a shutdown nexus", target_fixture="get_a_shutdown_nexus")
def a_shutdown_nexus(create_published_nexus, mayastor_mod):
    nexus_name = create_published_nexus[1]

    # Shutdown nexus and check its state.
    mayastor_mod["ms3"].nexus_shutdown(nexus_name)
    check_shutdown_nexus_state(mayastor_mod)

    # Pass nexus name further.
    return nexus_name


def check_shutdown_nexus_state(mayastor_mod):
    """Assure that nexus is in shutdown state with all proper state changes"""
    # Check that nexus/replica states reflect shutdown operation.
    nexuses = [n for n in mayastor_mod["ms3"].nexus_list() if n.uuid == NEXUS_NAME]
    assert len(nexuses) == 1, "No nexus found after shutdown"

    nexus = nexuses[0]
    assert nexus.state == pb.NEXUS_SHUTDOWN

    for c in nexus.children:
        assert c.state == pb.CHILD_DEGRADED, "Replica must be in DEGRADED state"
        assert (
            c.reason == pb.CHILD_STATE_REASON_CLOSED
        ), "Replica must be in CHILD_STATE_REASON_CLOSED state"


@given(
    "fio client is running against target nexus",
    target_fixture="run_fio_against_nexus",
)
def fio_client_is_running_against_target_nexus(get_connected_nexus):
    dev = get_connected_nexus

    desc = nvme_list_subsystems(dev)
    assert (
        len(desc["Subsystems"]) == 1
    ), "Must be exactly one NVMe subsystem for target nexus"

    subsystem = desc["Subsystems"][0]
    assert len(subsystem["Paths"]) == 1, "Must be exactly one I/O path to target nexus"
    assert subsystem["Paths"][0]["State"] == "live", "I/O path is not healthy"
    # Launch fio in background and let it always run along with the test.
    fio = Fio("job2", "randwrite", dev, runtime=FIO_RUNTIME).build()
    return subprocess.Popen(fio, shell=True)


@when("nexus is shutdown")
def when_nexus_is_shutdown(run_fio_against_nexus, mayastor_mod):
    mayastor_mod["ms3"].nexus_shutdown(NEXUS_NAME)

    # Check that nexus/replica states reflect shutdown operation.
    nexuses = [n for n in mayastor_mod["ms3"].nexus_list() if n.uuid == NEXUS_NAME]
    assert len(nexuses) == 1, "No nexus found after shutdown"

    nexus = nexuses[0]
    assert nexus.state == pb.NEXUS_SHUTDOWN

    for c in nexus.children:
        assert c.state == pb.CHILD_DEGRADED, "Replica must be in DEGRADED state"
        assert (
            c.reason == pb.CHILD_STATE_REASON_CLOSED
        ), "Replica must be in CHILD_STATE_REASON_CLOSED state"


@then("fio should not experience I/O errors and wait on I/O")
def check_fio_is_active(run_fio_against_nexus):
    try:
        run_fio_against_nexus.wait(timeout=FIO_RUNTIME * 2)
        assert False, "FIO completed with shutdown nexus"
    except subprocess.TimeoutExpired:
        # Since FIO is waiting on I/O whilst nexus is shutdown,
        # wait till timeout hits.
        pass


@when("nexus is shutdown again")
def when_nexus_is_shutdown_again(get_a_shutdown_nexus, mayastor_mod):
    mayastor_mod["ms3"].nexus_shutdown(get_a_shutdown_nexus)


@then("operation should succeed")
def check_second_nexus_shutdown_succeeded(get_a_shutdown_nexus, mayastor_mod):
    check_shutdown_nexus_state(mayastor_mod)


@pytest.fixture(scope="module")
def create_published_nexus(mayastor_mod, create_replica):
    """ Create and publish a nexus on ms3 with 2 replicas """
    hdls = mayastor_mod
    replicas = create_replica
    replicas = [k.uri for k in replicas]

    hdls["ms3"].nexus_create_v2(
        NEXUS_NAME,
        NEXUS_UUID,
        NEXUS_SIZE,
        10,  # Min controller id
        19,  # Max controller id
        0xABCDEF0012345678,  # Reservation key
        0,
        replicas,
    )
    uri = hdls["ms3"].nexus_publish(NEXUS_NAME)
    assert len(hdls["ms1"].bdev_list()) == 2
    assert len(hdls["ms2"].bdev_list()) == 2
    assert len(hdls["ms3"].bdev_list()) == 1

    assert len(hdls["ms1"].pool_list().pools) == 1
    assert len(hdls["ms2"].pool_list().pools) == 1

    yield (uri, NEXUS_NAME)

    try:
        hdls["ms3"].nexus_destroy(NEXUS_NAME)
    except:
        pass


@pytest.fixture(scope="module")
def create_connected_nexus(mayastor_mod, create_published_nexus):
    """ Create a nexus on ms3 with 2 replicas """
    uri = create_published_nexus[0]

    dev = nvme_connect(uri)
    yield dev

    # Forcibly delete the controller instead of disconnecting gracefully
    nvme_delete_controller(dev)


@pytest.fixture(scope="module")
def create_pools(mayastor_mod):
    hdls = mayastor_mod

    pools = []
    pools.append(hdls["ms1"].pool_create(POOL_NAME, POOL_DEV_URI))
    pools.append(hdls["ms2"].pool_create(POOL_NAME, POOL_DEV_URI))

    for p in pools:
        assert p.state == pb.POOL_ONLINE
    yield pools

    try:
        hdls["ms1"].pool_destroy(POOL_NAME)
        hdls["ms2"].pool_destroy(POOL_NAME)
    except Exception:
        pass


@pytest.fixture(scope="module")
def create_replica(mayastor_mod, create_pools):
    hdls = mayastor_mod
    pools = create_pools
    replicas = []

    replicas.append(
        hdls["ms1"].replica_create(pools[0].name, REPLICA_UUID, REPLICA_SIZE)
    )
    replicas.append(
        hdls["ms2"].replica_create(pools[0].name, REPLICA_UUID, REPLICA_SIZE)
    )

    yield replicas
    try:
        hdls["ms1"].replica_destroy(REPLICA_UUID)
        hdls["ms2"].replica_destroy(REPLICA_UUID)
    except Exception as e:
        logging.debug(e)
