"""Default fixtures that are considered to be reusable."""

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
        print("to use this fixture the environ TARGET_VM variable must be set")
        raise(e)



@pytest.fixture(scope="module")
def wait_for_mayastor(docker_project, module_scoped_container_getter):
    """Fixture to get a reference to mayastor handles."""
    project = docker_project
    handles = {}
    for name in project.service_names:
        # because we use static networks .get_service() does not work
        services = module_scoped_container_getter.get(name)
        ip_v4 = services.get(
            "NetworkSettings.Networks.python_mayastor_net.IPAddress")
        handles[name] = MayastorHandle(ip_v4)
    yield handles


@pytest.fixture(scope="module")
def container_ref(docker_project, module_scoped_container_getter):
    """Fixture to get handles to mayastor as well as the containers."""
    project = docker_project
    containers = {}
    for name in project.service_names:
        containers[name] = module_scoped_container_getter.get(name)
    yield containers


@pytest.fixture
def pool_config():
    """
    The idea is this used to obtain the pool types and names that should be
    created.
    """
    pool = {}
    pool['name'] = "tpool"
    pool['uri'] = "malloc:///disk0?size_mb=100"
    return pool


@pytest.fixture
def replica_uuid():
    """Replica UUID's to be used."""
    UUID = "0000000-0000-0000-0000-000000000001"
    size_mb = 64 * 1024 * 1024
    return (UUID, size_mb)


@pytest.fixture
def nexus_uuid():
    """Nexus UUID's to be used."""
    NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"
    size_mb = 64 * 1024 * 1024
    return (NEXUS_UUID, size_mb)


@pytest.fixture
def create_pools(
        wait_for_mayastor,
        container_ref,
        pool_config):
    hdls = wait_for_mayastor

    cfg = pool_config
    pools = []

    pools.append(hdls['ms1'].pool_create(cfg.get('name'),
                                         cfg.get('uri')))

    pools.append(hdls['ms2'].pool_create(cfg.get('name'),
                                         cfg.get('uri')))

    for p in pools:
        assert p.state == pb.POOL_ONLINE
    yield pools
    try:
        hdls['ms1'].pool_destroy(cfg.get('name'))
        hdls['ms2'].pool_destroy(cfg.get('name'))
    except Exception:
        pass


@pytest.fixture
def create_replica(
        wait_for_mayastor,
        pool_config,
        replica_uuid,
        create_pools):
    hdls = wait_for_mayastor
    pools = create_pools
    replicas = []

    UUID, size_mb = replica_uuid

    replicas.append(hdls['ms1'].replica_create(pools[0].name,
                                               UUID, size_mb))
    replicas.append(hdls['ms2'].replica_create(pools[0].name,
                                               UUID, size_mb))

    yield replicas
    try:
        hdls['ms1'].replica_destroy(UUID)
        hdls['ms2'].replica_destroy(UUID)
    except Exception as e:
        logging.debug(e)


@pytest.fixture
def create_nexus(wait_for_mayastor, container_ref, nexus_uuid, create_replica):
    hdls = wait_for_mayastor
    replicas = create_replica
    replicas = [k.uri for k in replicas]

    NEXUS_UUID, size_mb = nexus_uuid

    hdls['ms3'].nexus_create(NEXUS_UUID, 64 * 1024 * 1024, replicas)
    uri = hdls['ms3'].nexus_publish(NEXUS_UUID)

    assert len(hdls['ms1'].bdev_list().bdevs) == 2
    assert len(hdls['ms2'].bdev_list().bdevs) == 2
    assert len(hdls['ms3'].bdev_list().bdevs) == 1

    assert len(hdls['ms1'].pool_list().pools) == 1
    assert len(hdls['ms2'].pool_list().pools) == 1

    yield uri
    hdls['ms3'].nexus_destroy(NEXUS_UUID)
