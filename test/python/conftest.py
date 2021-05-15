"""Default fixtures that are considered to be reusable. These are all function scoped."""

import logging

import pytest
from common.hdl import MayastorHandle
import mayastor_pb2 as pb
import os

pytest_plugins = ["docker_compose"]


@pytest.fixture
def target_vm():
    try:
        return os.environ.get("TARGET_VM")
    except Exception as e:
        print("the environment variable TARGET_VM must be set to a valid host")
        raise(e)


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
    """Fixture to get a reference to mayastor gRPC handles."""
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
