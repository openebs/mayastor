import pytest
from pytest_bdd import given, scenario, then, when, parsers

from common.command import run_cmd
from common.mayastor import container_mod, mayastor_mod

import grpc
import mayastor_pb2 as pb


@scenario("features/pool.feature", "creating a pool using disk with invalid block size")
def test_fail_creating_a_pool_using_disk_with_invalid_block_size():
    "Creating a pool using disk with invalid block size."


@scenario("features/pool.feature", "creating a pool with a name that already exists")
def test_creating_a_pool_with_a_name_that_already_exists():
    "Creating a pool with a name that already exists."


@scenario("features/pool.feature", "creating a pool with an AIO disk")
def test_creating_a_pool_with_an_aio_disk():
    "Creating a pool with an AIO disk."


@scenario("features/pool.feature", "creating a pool with multiple disks")
def test_fail_creating_a_pool_with_multiple_disks():
    "Creating a pool with multiple disks."


@scenario("features/pool.feature", "destroying a pool")
def test_destroying_a_pool():
    "Destroying a pool."


@scenario("features/pool.feature", "destroying a pool that does not exist")
def test_destroying_a_pool_that_does_not_exist():
    "Destroying a pool that does not exist."


@scenario("features/pool.feature", "listing pools")
def test_listing_pools():
    "Listing pools."


@pytest.fixture
def image_file():
    name = "/tmp/ms0-disk0.img"
    run_cmd(f"rm -f '{name}'", True)
    run_cmd(f"truncate -s 64M '{name}'", True)
    yield name
    run_cmd(f"rm -f '{name}'", True)


@pytest.fixture
def find_pool(get_mayastor_instance):
    def find(name):
        for pool in get_mayastor_instance.ms.ListPools(pb.Null()).pools:
            if pool.name == name:
                return pool
        return None

    yield find


@pytest.fixture
def replica_pools(get_mayastor_instance):
    pools = {}
    yield pools
    for name in pools.keys():
        get_mayastor_instance.ms.DestroyPool(pb.DestroyPoolRequest(name=name))


@pytest.fixture
def create_pool(get_mayastor_instance, replica_pools):
    def create(name, disks):
        pool = get_mayastor_instance.ms.CreatePool(
            pb.CreatePoolRequest(name=name, disks=disks)
        )
        replica_pools[name] = pool

    yield create


@given(
    parsers.parse('a mayastor instance "{name}"'),
    target_fixture="get_mayastor_instance",
)
def get_mayastor_instance(mayastor_mod, name):
    return mayastor_mod[name]


@given(parsers.parse('a pool "{name}"'), target_fixture="get_pool_name")
def get_pool_name(get_mayastor_instance, create_pool, name):
    create_pool(name, ["malloc:///disk0?size_mb=100"])
    return name


@when("the user creates a pool specifying a URI representing an aio disk")
def create_pool_from_aio_disk(get_mayastor_instance, create_pool, image_file):
    create_pool("p0", [f"aio://{image_file}"])


@when("the user attempts to create a pool specifying a disk with an invalid block size")
def attempt_to_create_pool_from_disk_with_invalid_block_size(
    get_mayastor_instance, create_pool
):
    with pytest.raises(grpc.RpcError) as error:
        create_pool("p0", "malloc:///disk0?size_mb=100&blk_size=1024")
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user attempts to create a pool specifying multiple disks")
def attempt_to_create_pool_from_multiple_disks(get_mayastor_instance, create_pool):
    with pytest.raises(grpc.RpcError) as error:
        create_pool(
            "p0", ["malloc:///disk0?size_mb=100", "malloc:///disk1?size_mb=100"]
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when(
    "the user creates a pool with the name of an existing pool",
    target_fixture="create_pool_that_already_exists",
)
def create_pool_that_already_exists(get_mayastor_instance, create_pool, get_pool_name):
    create_pool(get_pool_name, ["malloc:///disk0?size_mb=100"])


@when("the user destroys a pool that does not exist")
def destroy_pool_that_does_not_exist(get_mayastor_instance, find_pool):
    assert find_pool("p0") == None
    get_mayastor_instance.ms.DestroyPool(pb.DestroyPoolRequest(name="p0"))


@when("the user destroys the pool")
def destroy_pool(get_mayastor_instance, replica_pools, get_pool_name):
    pool = replica_pools[get_pool_name]
    get_mayastor_instance.ms.DestroyPool(pb.DestroyPoolRequest(name=pool.name))
    del replica_pools[get_pool_name]


@when("the user lists the current pools", target_fixture="list_pools")
def list_pools(get_mayastor_instance):
    return get_mayastor_instance.ms.ListPools(pb.Null(), wait_for_ready=True).pools


@then("the pool creation should fail")
def pool_creation_should_fail(find_pool):
    assert find_pool("p0") == None


@then("the pool create command should succeed")
@then("the pool should be created")
def pool_creation_should_succeed(find_pool):
    assert find_pool("p0") != None


@then("the pool destroy command should succeed")
@then("the pool should be destroyed")
def pool_destruction_should_succeed(find_pool):
    assert find_pool("p0") == None


@then("the pool should appear in the output list")
def pool_should_appear_in_output(get_pool_name, list_pools):
    assert get_pool_name in [pool.name for pool in list_pools]
