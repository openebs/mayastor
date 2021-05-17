from common.hdl import MayastorHandle
from common.command import run_cmd
import pytest
import uuid as guid

UUID = "0000000-0000-0000-0000-000000000001"
NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"


@pytest.fixture(scope="function")
def create_temp_files(containers):
    """Create temp files for each run so we start out clean."""
    for name in containers:
        run_cmd(f"rm -rf /tmp/{name}.img", True)
    for name in containers:
        run_cmd(f"truncate -s 1G /tmp/{name}.img", True)


def check_size(prev, current, delta):
    """Validate that replica creation consumes space on the pool."""
    before = prev.pools[0].used
    after = current.pools[0].used
    assert delta == (before - after) >> 20


@pytest.fixture(scope="function")
def mayastors(docker_project, function_scoped_container_getter):
    """Fixture to get a reference to mayastor handles."""
    project = docker_project
    handles = {}
    for name in project.service_names:
        # because we use static networks .get_service() does not work
        services = function_scoped_container_getter.get(name)
        ip_v4 = services.get(
            "NetworkSettings.Networks.python_mayastor_net.IPAddress")
        handles[name] = MayastorHandle(ip_v4)
    yield handles


@pytest.fixture(scope="function")
def containers(docker_project, function_scoped_container_getter):
    """Fixture to get handles to mayastor as well as the containers."""
    project = docker_project
    containers = {}
    for name in project.service_names:
        containers[name] = function_scoped_container_getter.get(name)
    yield containers


@pytest.fixture
def create_pool_on_all_nodes(create_temp_files, containers, mayastors):
    """Create a pool on each node."""
    uuids = []

    for name, h in mayastors.items():
        h.pool_create(f"{name}", f"aio:///tmp/{name}.img")
        # validate we have zero replicas
        assert len(h.replica_list().replicas) == 0

    for i in range(15):
        uuid = guid.uuid4()
        for name, h in mayastors.items():
            before = h.pool_list()
            h.replica_create(name, uuid, 64 * 1024 * 1024)
            after = h.pool_list()
            check_size(before, after, -64)
            # ensure our replica count goes up as expected
            assert len(h.replica_list().replicas) == i + 1

        uuids.append(uuid)
    return uuids


@pytest.mark.parametrize("times", range(2))
def test_restart(
        times,
        create_pool_on_all_nodes,
        containers,
        mayastors):
    """
    Test that when we create replicas and destroy them the count is as expected
    At this point we have 3 nodes each with 15 replica's.
    """

    node = containers.get("ms1")
    ms1 = mayastors.get("ms1")

    # kill one of the nodes and validate we indeed have 15 replica's
    node.kill()
    node.start()
    # we must reconnect grpc here..
    ms1.reconnect()
    # create does import here if found
    ms1.pool_create("ms1", "aio:///tmp/ms1.img")

    # check the list has 15 replica's
    replicas = ms1.replica_list().replicas
    assert 15 == len(replicas)

    # destroy a few
    for i in range(7):
        ms1.replica_destroy(replicas[i].uuid)

    # kill and reconnect
    node.kill()
    node.start()
    ms1.reconnect()

    # validate we have 8 replicas left
    ms1.pool_create("ms1", "aio:///tmp/ms1.img")
    replicas = ms1.replica_list().replicas

    assert 8 == len(replicas)
