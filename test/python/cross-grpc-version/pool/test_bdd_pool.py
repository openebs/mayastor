"""Mayastor pool management feature tests."""

import pytest

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

from common.mayastor import container_mod, mayastor_mod
from v1.mayastor import container_mod as container_mod_v1
from v1.mayastor import mayastor_mod as mayastor_mod_v1

import grpc
import mayastor_pb2 as pb
import pool_pb2 as pb2


@given(
    parsers.parse('a mayastor instance "{name}"'),
    target_fixture="get_mayastor_instance",
)
def get_mayastor_instance(mayastor_mod, name):
    return mayastor_mod[name]


@pytest.fixture
def v1_mayastor_instance(mayastor_mod_v1):
    return mayastor_mod_v1["ms0"]


@scenario(
    "features/pool.feature",
    "creating a v1 version pool with a v0 version pool name that already exists",
)
def test_creating_a_v1_version_pool_with_a_v0_version_pool_name_that_already_exists():
    """creating a v1 version pool with a v0 version pool name that already exists."""


@scenario(
    "features/pool.feature", "listing pools created with v0 version using v1 grpc call"
)
def test_listing_pools_created_with_v0_version_using_v1_grpc_call():
    """listing pools created with v0 version using v1 gRPC call."""


@scenario(
    "features/pool.feature",
    "listing pool by name created with v0 version using v1 grpc call",
)
def test_listing_pool_by_name_created_with_v0_version_using_v1_grpc_call():
    """listing pool by name created with v0 version using v1 gRPC call."""


@scenario(
    "features/pool.feature",
    "listing v0 pool by name which does not exists using v1 grpc call",
)
def test_listing_v0_pool_by_name_which_does_not_exists_using_v1_grpc_call():
    """listing v0 pool by name which does not exists using v1 grpc call."""


@scenario(
    "features/pool.feature",
    "destroying a pool created with v0 version using v1 grpc call",
)
def test_destroying_a_pool_created_with_v0_version_using_v1_grpc_call():
    """destroying a pool created with v0 version using v1 grpc call."""


@pytest.fixture
def v0_replica_pools(get_mayastor_instance):
    pools = {}
    yield pools
    for name in pools.keys():
        get_mayastor_instance.ms.DestroyPool(pb.DestroyPoolRequest(name=name))


@pytest.fixture
def create_v0_pool(get_mayastor_instance, v0_replica_pools):
    def create(name, disks):
        pool = get_mayastor_instance.ms.CreatePool(
            pb.CreatePoolRequest(name=name, disks=disks)
        )
        v0_replica_pools[name] = pool

    yield create


@pytest.fixture
def v1_replica_pools(v1_mayastor_instance):
    pools = {}
    yield pools
    for name in pools.keys():
        opts = pb2.ListPoolOptions()
        opts.name.value = name
        pools = v1_mayastor_instance.pool_rpc.ListPools(opts).pools
        if len(pools) != 0:
            opts = pb2.DestroyPoolRequest()
            opts.name = pools[0].name
            opts.uuid.value = pools[0].uuid
            v1_mayastor_instance.pool_rpc.DestroyPool(opts)


@pytest.fixture
def create_v1_pool(v1_mayastor_instance, v1_replica_pools):
    def create(name, disks, uuid):
        opts = pb2.CreatePoolRequest(name=name, disks=disks)
        if uuid is not None:
            opts.uuid.value = uuid
        pool = v1_mayastor_instance.pool_rpc.CreatePool(opts)
        v1_replica_pools[name] = pool

    yield create


@pytest.fixture
def find_pool(v1_mayastor_instance):
    def find(name):
        for pool in v1_mayastor_instance.pool_rpc.ListPools(
            pb2.ListPoolOptions()
        ).pools:
            if pool.name == name:
                return pool
        return None

    yield find


@given(
    parsers.parse('a v0 version pool "{name}" on "{disk}"'),
    target_fixture="get_pool_name",
)
def get_pool_name(create_v0_pool, name, disk):
    create_v0_pool(name, [f"malloc:///{disk}?size_mb=100"])
    return name


@when(
    "the user creates a v1 version pool with the name of an existing pool",
    target_fixture="create_pool_that_already_exists",
)
def create_pool_that_already_exists(create_v1_pool, get_pool_name):
    with pytest.raises(grpc.RpcError) as error:
        create_v1_pool(get_pool_name, ["malloc:///disk0?size_mb=100"], None)
    return error


@when("the user lists the current pools", target_fixture="list_pools")
def list_pools(v1_mayastor_instance):
    return v1_mayastor_instance.pool_rpc.ListPools(
        pb2.ListPoolOptions(), wait_for_ready=True
    ).pools


@when(
    "the user lists the pool with name filter as p0",
    target_fixture="list_existing_pool",
)
def list_existing_pool(v1_mayastor_instance):
    opts = pb2.ListPoolOptions()
    opts.name.value = "p0"
    return v1_mayastor_instance.pool_rpc.ListPools(opts, wait_for_ready=True).pools


@when(
    "the user lists the pool with name filter as p1",
    target_fixture="list_non_existing_pool",
)
def list_non_existing_pool(v1_mayastor_instance):
    opts = pb2.ListPoolOptions()
    opts.name.value = "p1"
    return v1_mayastor_instance.pool_rpc.ListPools(opts, wait_for_ready=True).pools


@when("the user destroys the pool")
def destroy_pool(v1_mayastor_instance, v0_replica_pools, get_pool_name):
    pool = v0_replica_pools[get_pool_name]
    v1_mayastor_instance.pool_rpc.DestroyPool(pb2.DestroyPoolRequest(name=pool.name))
    del v0_replica_pools[get_pool_name]


@then("the pool create command should fail")
def the_pool_create_command_should_fail(create_pool_that_already_exists):
    assert create_pool_that_already_exists.value.code() == grpc.StatusCode.INTERNAL


@then("the pool should appear in the output list")
def pool_should_appear_in_output(get_pool_name, list_pools):
    assert get_pool_name in [pool.name for pool in list_pools]


@then("only the pool p0 should appear in the output list")
def only_the_pool_p0_should_appear_in_the_output_list(list_existing_pool):
    pools = list_existing_pool
    assert len(pools) == 1
    assert "p0" in [pool.name for pool in pools]


@then("no pool should appear in the output list")
def no_pool_should_appear_in_the_output_list(list_non_existing_pool):
    assert len(list_non_existing_pool) == 0


@then("the pool should be destroyed")
def pool_destruction_should_succeed(find_pool):
    assert find_pool("p0") == None
