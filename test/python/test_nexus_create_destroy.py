from common.hdl import MayastorHandle
import pytest
import mayastor_pb2 as pb


@pytest.fixture
def pool_config():
    pool = {}
    pool['name'] = "tpool"
    pool['uri'] = "malloc:///disk0?size_mb=100"
    return pool


@pytest.fixture(scope="module")
def containers(docker_project, module_scoped_container_getter):
    project = docker_project
    containers = {}
    for name in project.service_names:
        containers[name] = module_scoped_container_getter.get(name)
    yield containers


@pytest.fixture(scope="module")
def wait_for_mayastor(docker_project, module_scoped_container_getter):
    project = docker_project
    handles = {}
    for name in project.service_names:
        # because we use static networks .get_service() does not work
        services = module_scoped_container_getter.get(name)
        ip_v4 = services.get(
            "NetworkSettings.Networks.python_mayastor_net.IPAddress")
        handles[name] = MayastorHandle(ip_v4)
    yield handles


@pytest.fixture
def replica_uuid():
    UUID = "0000000-0000-0000-0000-000000000001"
    size_mb = 64 * 1024 * 1024
    return (UUID, size_mb)


@pytest.fixture
def nexus_uuid():
    NEXUS_UUID = "3ae73410-6136-4430-a7b5-cbec9fe2d273"
    size_mb = 64 * 1024 * 1024
    return (NEXUS_UUID, size_mb)


@pytest.fixture
def create_pools(
        wait_for_mayastor,
        containers,
        pool_config):
    hdls = wait_for_mayastor

    cfg = pool_config
    pools = []

    pools.append(hdls['ms1'].pool_create(cfg.get('name'),
                                         cfg.get('uri')))

    pools.append(hdls['ms2'].pool_create(cfg.get('name'),
                                         cfg.get('uri')))

    pools.append(hdls['ms3'].pool_create(cfg.get('name'),
                                         cfg.get('uri')))

    for p in pools:
        assert p.state == pb.POOL_ONLINE
    yield pools


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
    replicas.append(hdls['ms3'].replica_create(pools[0].name,
                                               UUID, size_mb, 0))

    yield replicas


@pytest.mark.timeout(60)
def test_nexus_create_destroy(wait_for_mayastor, nexus_uuid, create_replica):
    replicas = create_replica
    replicas = [k.uri for k in replicas]

    hdls = wait_for_mayastor

    NEXUS_UUID, size_mb = nexus_uuid

    assert len(hdls['ms1'].bdev_list()) == 2
    assert len(hdls['ms2'].bdev_list()) == 2
    assert len(hdls['ms3'].bdev_list()) == 2
    assert len(hdls['ms1'].pool_list().pools) == 1
    assert len(hdls['ms2'].pool_list().pools) == 1
    assert len(hdls['ms3'].pool_list().pools) == 1

    for i in range(10):
        hdls['ms3'].nexus_create(NEXUS_UUID, 64 * 1024 * 1024, replicas)
        assert len(hdls['ms3'].nexus_list()) == 1
        assert len(hdls['ms3'].bdev_list()) == 3
        hdls['ms3'].nexus_destroy(NEXUS_UUID)
        assert len(hdls['ms3'].nexus_list()) == 0
        assert len(hdls['ms3'].bdev_list()) == 2
