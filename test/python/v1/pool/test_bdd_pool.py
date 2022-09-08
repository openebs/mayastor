"""Mayastor pool management feature tests."""

import pytest

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

from common.command import run_cmd
from v1.mayastor import container_mod, mayastor_mod

import grpc
import pool_pb2 as pb


@scenario("features/pool.feature", "creating a pool using disk with invalid block size")
def test_creating_a_pool_using_disk_with_invalid_block_size():
    """creating a pool using disk with invalid block size."""


@scenario("features/pool.feature", "creating a pool using invalid uuid")
def test_creating_a_pool_using_disk_with_invalid_uuid():
    """creating a pool using invalid uuid."""


@scenario("features/pool.feature", "creating a pool using valid uuid")
def test_creating_a_pool_using_disk_with_valid_uuid():
    """creating a pool using valid uuid."""


@scenario("features/pool.feature", "creating a pool with a name that already exists")
def test_creating_a_pool_with_a_name_that_already_exists():
    """creating a pool with a name that already exists."""


@scenario("features/pool.feature", "creating a pool with an AIO disk")
def test_creating_a_pool_with_an_aio_disk():
    """creating a pool with an AIO disk."""


@scenario("features/pool.feature", "creating a pool with multiple disks")
def test_creating_a_pool_with_multiple_disks():
    """creating a pool with multiple disks."""


@scenario("features/pool.feature", "destroying a pool")
def test_destroying_a_pool():
    """destroying a pool."""


@scenario("features/pool.feature", "destroying a pool that does not exist")
def test_destroying_a_pool_that_does_not_exist():
    """destroying a pool that does not exist."""


@scenario("features/pool.feature", "exporting a pool removes the pool from the system")
def test_exporting_a_pool_removes_the_pool_from_the_system():
    """exporting a pool removes the pool from the system."""


@scenario("features/pool.feature", "importing a pool")
def test_importing_a_pool():
    """importing a pool."""


@scenario("features/pool.feature", "importing a pool with uuid")
def test_importing_a_pool_with_uuid():
    """importing a pool with uuid."""


@scenario("features/pool.feature", "importing a pool with invalid uuid")
def test_importing_a_pool_with_invalid_uuid():
    """importing a pool with invalid uuid."""


@scenario("features/pool.feature", "listing pool by name")
def test_listing_pool_by_name():
    """listing pool by name."""


@scenario("features/pool.feature", "listing pool by name which does not exists")
def test_listing_pool_by_name_which_does_not_exists():
    """listing pool by name which does not exists."""


@scenario("features/pool.feature", "listing pools")
def test_listing_pools():
    """listing pools."""


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
        for pool in get_mayastor_instance.pool_rpc.ListPools(
            pb.ListPoolOptions()
        ).pools:
            if pool.name == name:
                return pool
        return None

    yield find


@pytest.fixture
def replica_pools(get_mayastor_instance):
    pools = {}
    yield pools
    for name in pools.keys():
        opts = pb.ListPoolOptions()
        opts.name.value = name
        pools = get_mayastor_instance.pool_rpc.ListPools(opts).pools
        if len(pools) != 0:
            opts = pb.DestroyPoolRequest()
            opts.name = pools[0].name
            opts.uuid.value = pools[0].uuid
            get_mayastor_instance.pool_rpc.DestroyPool(opts)


@pytest.fixture
def create_pool(get_mayastor_instance, replica_pools):
    def create(name, disks, uuid):
        opts = pb.CreatePoolRequest(name=name, disks=disks)
        if uuid is not None:
            opts.uuid.value = uuid
        pool = get_mayastor_instance.pool_rpc.CreatePool(opts)
        replica_pools[name] = pool

    yield create


@pytest.fixture
def list_by_name(get_mayastor_instance):
    def list(name):
        opts = pb.ListPoolOptions()
        opts.name.value = name
        return get_mayastor_instance.pool_rpc.ListPools(opts, wait_for_ready=True).pools

    yield list


@given(
    parsers.parse('a mayastor instance "{name}"'),
    target_fixture="get_mayastor_instance",
)
def get_mayastor_instance(mayastor_mod, name):
    return mayastor_mod[name]


@given(parsers.parse('a pool "{name}" on "{disk}"'), target_fixture="get_pool_name")
def get_pool_name(create_pool, name, disk):
    create_pool(name, [f"malloc:///{disk}?size_mb=100"], None)
    return name


@given("an aio pool", target_fixture="an_aio_pool")
def an_aio_pool(create_pool, image_file):
    create_pool("p0", [f"aio://{image_file}"], None)
    return "p0"


@given("an aio pool with uuid", target_fixture="an_aio_pool")
def an_aio_pool_with_uuid(create_pool, image_file):
    create_pool("p0", [f"aio://{image_file}"], "45c09d69-3f6c-4029-b763-e055c37f06a5")
    return "p0"


@when("the user attempts to create a pool specifying a disk with an invalid block size")
def attempt_to_create_pool_from_disk_with_invalid_block_size(create_pool):
    with pytest.raises(grpc.RpcError) as error:
        create_pool("p0", "malloc:///disk0?size_mb=100&blk_size=1024", None)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user attempts to create a pool specifying an invalid uuid")
def attempt_to_create_pool__with_invalid_uuid(create_pool):
    with pytest.raises(grpc.RpcError) as error:
        create_pool("p0", "malloc:///disk0?size_mb=100", "invalid-uuid")
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user attempts to create a pool specifying a valid uuid")
def attempt_to_create_pool_with_valid_uuid(create_pool):
    create_pool(
        "p0", ["malloc:///disk0?size_mb=100"], "45c09d69-3f6c-4029-b763-e055c37f06a5"
    )


@when("the user attempts to create a pool specifying multiple disks")
def attempt_to_create_pool_from_multiple_disks(create_pool):
    with pytest.raises(grpc.RpcError) as error:
        create_pool(
            "p0", ["malloc:///disk0?size_mb=100", "malloc:///disk1?size_mb=100"], None
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user creates a pool specifying a URI representing an aio disk")
def create_pool_from_aio_disk(create_pool, image_file):
    create_pool("p0", [f"aio://{image_file}"], None)


@when(
    "the user creates a pool with the name of an existing pool",
    target_fixture="create_pool_that_already_exists",
)
def create_pool_that_already_exists(create_pool, get_pool_name):
    with pytest.raises(grpc.RpcError) as error:
        create_pool(get_pool_name, ["malloc:///disk0?size_mb=100"], None)
    return error


@when(
    "the user destroys a pool that does not exist",
    target_fixture="destroy_non_existing_pool",
)
def destroy_non_existing_pool(get_mayastor_instance):
    with pytest.raises(grpc.RpcError) as error:
        get_mayastor_instance.pool_rpc.DestroyPool(
            pb.DestroyPoolRequest(name="no-pool")
        )
    return error


@when("the user destroys the pool")
def destroy_pool(get_mayastor_instance, replica_pools, get_pool_name):
    pool = replica_pools[get_pool_name]
    get_mayastor_instance.pool_rpc.DestroyPool(pb.DestroyPoolRequest(name=pool.name))
    del replica_pools[get_pool_name]


@when("the user exports the pool")
def the_user_exports_the_pool(get_mayastor_instance, find_pool):
    pool = find_pool("p0")
    get_mayastor_instance.pool_rpc.ExportPool(pb.ExportPoolRequest(name=pool.name))


@when("then imports the pool")
def then_imports_the_pool(get_mayastor_instance):
    get_mayastor_instance.pool_rpc.ImportPool(
        pb.ImportPoolRequest(name="p0", disks=["aio:///tmp/ms0-disk0.img"])
    )


@when("then imports the pool with incorrect uuid")
def then_imports_the_pool_with_incorrect_uuid(get_mayastor_instance):
    with pytest.raises(grpc.RpcError) as error:
        opts = pb.ImportPoolRequest(name="p0", disks=["aio:///tmp/ms0-disk0.img"])
        opts.uuid.value = "invalid"
        get_mayastor_instance.pool_rpc.ImportPool(opts)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user lists the current pools", target_fixture="list_pools")
def list_pools(get_mayastor_instance):
    return get_mayastor_instance.pool_rpc.ListPools(
        pb.ListPoolOptions(), wait_for_ready=True
    ).pools


@when(
    "the user lists the pool with name filter as p0",
    target_fixture="list_existing_pool",
)
def list_existing_pool(get_mayastor_instance):
    opts = pb.ListPoolOptions()
    opts.name.value = "p0"
    return get_mayastor_instance.pool_rpc.ListPools(opts, wait_for_ready=True).pools


@when(
    "the user lists the pool with name filter as p1",
    target_fixture="list_non_existing_pool",
)
def list_non_existing_pool(get_mayastor_instance):
    opts = pb.ListPoolOptions()
    opts.name.value = "p1"
    return get_mayastor_instance.pool_rpc.ListPools(opts, wait_for_ready=True).pools


@then("the pool creation should fail")
@then("the pool should be not imported")
def pool_creation_should_fail(find_pool):
    assert find_pool("p0") == None


@then("the pool create command should fail")
def the_pool_create_command_should_fail(create_pool_that_already_exists):
    assert create_pool_that_already_exists.value.code() == grpc.StatusCode.ALREADY_EXISTS


@then("the pool destroy command should fail")
def the_pool_destroy_command_should_fail(destroy_non_existing_pool):
    assert destroy_non_existing_pool.value.code() == grpc.StatusCode.NOT_FOUND


@then("the pool should appear in the output list")
def pool_should_appear_in_output(get_pool_name, list_pools):
    assert get_pool_name in [pool.name for pool in list_pools]


@then("only the pool p0 should appear in the output list")
def only_the_pool_p0_should_appear_in_the_output_list(list_existing_pool):
    pools = list_existing_pool
    assert len(pools) == 1
    assert "p0" in [pool.name for pool in pools]


@then("no pool should not appear in the output list")
def no_pool_should_not_appear_in_the_output_list(list_non_existing_pool):
    assert len(list_non_existing_pool) == 0


@then("the pool should be created")
@then("the pool should be imported")
def pool_creation_should_succeed(find_pool):
    assert find_pool("p0") != None


@then("the pool should be created with same uuid")
@then("the pool with same uuid should be imported")
def pool_created_with_same_uuid(find_pool):
    assert find_pool("p0").uuid == "45c09d69-3f6c-4029-b763-e055c37f06a5"


@then("the pool should be destroyed")
@then("the pool should be exported")
def pool_destruction_should_succeed(find_pool):
    assert find_pool("p0") == None
