import pytest
from pytest_bdd import given, scenario, then, when
from common.mayastor import container_mod, mayastor_mod
import mayastor_pb2 as pb

POOL_NAME = "pool0"
REPLICA_NAME = "replica-4cb9-8097"
REPLICA_UUID = "45d2fd3e-38f2-42bf-8b5f-acddccf0ff53"
REPLICA_SIZE = 1024 * 1024 * 16

REPLICA_NAME_V1 = "replica-56e8-443f"


@scenario(
    "features/replica-guid.feature", "create a replica with explicit UUID and name"
)
def test_create_replica_with_guid():
    "Create a replica with explicit UUID and name"


@scenario(
    "features/replica-guid.feature",
    "list replicas created with explicit UUIDs and names",
)
def test_list_replica_with_guid():
    "List replicas created with explicit UUIDs and names"


@scenario(
    "features/replica-guid.feature",
    "list replicas created using v1 api",
)
def test_list_v1_replicas():
    "List replicas created using v1 api"


@pytest.fixture(scope="module")
def mayastor_instance(mayastor_mod):
    yield mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def mayastor_pool(mayastor_instance):
    pool = mayastor_instance.pool_create(POOL_NAME, "malloc:///disk0?size_mb=64")
    assert pool.state == pb.POOL_ONLINE
    yield POOL_NAME
    try:
        mayastor_instance.pool_destroy(POOL_NAME)
    except Exception:
        pass


@given("a mayastor instance")
def given_instance():
    pass


@given("a data pool created on mayastor instance")
def given_pool():
    pass


@when("a new replica is successfully created using UUID and name")
def create_replica(mayastor_instance, mayastor_pool):
    replica = mayastor_instance.replica_create_v2(
        mayastor_pool, REPLICA_NAME, REPLICA_UUID, REPLICA_SIZE
    )
    assert replica.name == REPLICA_NAME, "Replica name does not match"
    assert replica.uuid == REPLICA_UUID, "Replica UUID does not match"
    assert replica.size == REPLICA_SIZE, "Replica size does not match"
    assert replica.pool == POOL_NAME, "Pool name does not match"


@then("replica block device has the given name and UUID")
def check_replica_device(mayastor_instance):
    devs = [d for d in mayastor_instance.bdev_list() if d.name == REPLICA_NAME]
    assert len(devs) == 1, "Replica device not found among Mayastor devices"
    assert devs[0].uuid == REPLICA_UUID, "UUID for replica device does not match"

    size = devs[0].num_blocks * devs[0].blk_size
    assert size == REPLICA_SIZE, "Replica device size does not match"


@then("both UUID and name should be provided upon replicas enumeration")
def check_replica_enumeration(mayastor_instance):
    replicas = [
        r
        for r in mayastor_instance.replica_list_v2().replicas
        if r.name == REPLICA_NAME
    ]
    assert len(replicas) == 1, "Replica can not be found by its name"

    replica = replicas[0]
    assert replica.name == REPLICA_NAME
    assert replica.uuid == REPLICA_UUID
    assert replica.size == REPLICA_SIZE
    assert replica.pool == POOL_NAME


@then("replica name is returned in the response object to the caller")
def check_replica_name():
    # Already checked upon replica creation.
    pass


@when("a new replica is successfully created using v1 api")
def create_v1_replica(mayastor_instance, mayastor_pool):
    replica = mayastor_instance.replica_create(
        mayastor_pool, REPLICA_NAME_V1, REPLICA_SIZE
    )
    assert replica.uuid == REPLICA_NAME_V1, "Replica name does not match"
    assert replica.size == REPLICA_SIZE, "Replica size does not match"


@then("replica should be successfully enumerated via new replica enumeration api")
def check_v1_replica(mayastor_instance):
    replicas = [
        r
        for r in mayastor_instance.replica_list_v2().replicas
        if r.name == REPLICA_NAME_V1
    ]
    assert len(replicas) == 1, "Replica can not be found by its name"

    replica = replicas[0]
    assert replica.name == REPLICA_NAME_V1
    assert replica.size == REPLICA_SIZE
    assert replica.pool == POOL_NAME, "Pool name does not match"
    assert replica.uuid != REPLICA_NAME_V1, "Replica UUID was set to replica name"
