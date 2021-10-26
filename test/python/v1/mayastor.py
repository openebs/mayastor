"Default fixtures that are considered to be reusable."
import pytest
from v1.hdl import MayastorHandle
from common.command import run_cmd

pytest_plugins = ["docker_compose"]


def check_size(prev, current, delta):
    "Validate that replica creation consumes space on the pool."
    before = prev.pools[0].used
    after = current.pools[0].used
    assert delta == (before - after) >> 20


@pytest.fixture(scope="function")
def containers(docker_project, function_scoped_container_getter):
    "Fixture to get handles to mayastor containers."
    containers = {}
    for name in docker_project.service_names:
        containers[name] = function_scoped_container_getter.get(name)
    yield containers


@pytest.fixture(scope="function")
def mayastors(docker_project, containers):
    "Fixture to get a reference to mayastor gRPC handles"
    handles = {}
    for name, container in containers.items():
        handles[name] = MayastorHandle(
            container.get("NetworkSettings.Networks.mayastor_net.IPAddress")
        )
    yield handles


@pytest.fixture(scope="function")
def create_temp_files(containers):
    "Create temp files for each run so we start out clean."
    for name in containers.keys():
        run_cmd(f"rm -f /tmp/{name}.img", True)
    for name in containers.keys():
        run_cmd(f"truncate -s 1G /tmp/{name}.img", True)


@pytest.fixture(scope="module")
def container_mod(docker_project, module_scoped_container_getter):
    "Fixture to get handles to mayastor containers."
    containers = {}
    for name in docker_project.service_names:
        containers[name] = module_scoped_container_getter.get(name)
    yield containers


@pytest.fixture(scope="module")
def mayastor_mod(docker_project, container_mod):
    "Fixture to get a reference to mayastor gRPC handles."
    handles = {}
    for name, container in container_mod.items():
        handles[name] = MayastorHandle(
            container.get("NetworkSettings.Networks.mayastor_net.IPAddress")
        )
    yield handles
