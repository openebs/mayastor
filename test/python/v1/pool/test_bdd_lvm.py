"""LVM pool and replica support feature tests."""

import subprocess
import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

from common.command import run_cmd
from v1.mayastor import mayastor_mod, container_mod
import grpc
import pool_pb2 as pb


@scenario(
    "features/lvm.feature", "Determine if the mayastor instance supports LVM pools"
)
def test_determine_if_the_mayastor_instance_supports_lvm_pools():
    """Determine if the mayastor instance supports LVM pools."""


@scenario("features/lvm.feature", "creating a pool with an losetup disk")
def test_creating_a_pool_with_an_losetup_disk():
    """creating a pool with an losetup disk."""


@scenario("features/lvm.feature", "creating a lvs pool with an AIO disk")
def test_creating_a_pool_with_an_aio_disk():
    """creating a pool with an AIO disk."""


@scenario("features/lvm.feature", "Listing LVM pools and LVS pools")
def test_listing_lvm_pools_and_lvs_pools():
    """Listing LVM pools and LVS pools."""


@scenario("features/lvm.feature", "destroying a lvm pool with an losetup disk")
def test_destroying_a_lvm_pool_with_an_losetup_disk():
    """destroying a lvm pool with an losetup disk."""


@scenario("features/lvm.feature", "destroying a lvs pool with an AIO disk")
def test_destroying_a_lvs_pool_with_an_aio_disk():
    """destroying a lvs pool with an AIO disk."""


@pytest.fixture
def get_mayastor_info(get_mayastor_instance):
    return get_mayastor_instance.mayastor_info()


@given(
    parsers.parse('a mayastor instance "{name}"'),
    target_fixture="get_mayastor_instance",
)
def get_mayastor_instance(mayastor_mod, name):
    return mayastor_mod[f"{name}"]


@pytest.fixture
def volgrp_with_losetup_disk():
    file = "/tmp/ms0-disk0.img"
    name = "lvmpool"
    run_cmd(f"rm -f '{file}'", True)
    run_cmd(f"truncate -s 64M '{file}'", True)
    out = subprocess.run(
        f"sudo -E losetup -f '{file}' --show",
        shell=True,
        check=True,
        capture_output=True,
    )
    disk = out.stdout.decode("ascii").strip("\n")
    run_cmd(f"nix-sudo pvcreate '{disk}'", True)
    run_cmd(f"nix-sudo vgcreate '{name}' '{disk}'", True)
    pytest.disk = disk
    yield name
    run_cmd(f"nix-sudo losetup -d '{disk}'", True)
    run_cmd(f"rm -f '{file}'", True)


@pytest.fixture
def image_file():
    name = "/tmp/ms0-disk1.img"
    run_cmd(f"rm -f '{name}'", True)
    run_cmd(f"truncate -s 64M '{name}'", True)
    yield name
    run_cmd(f"rm -f '{name}'", True)


@pytest.fixture
def create_pool(get_mayastor_instance):
    def create(name, disks, pooltype):
        get_mayastor_instance.pool_rpc.CreatePool(
            pb.CreatePoolRequest(name=name, disks=disks, pooltype=pooltype)
        )

    yield create


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


@when("the user calls the gRPC mayastor info request", target_fixture="get_lvm_feature")
def get_lvm_feature(get_mayastor_instance, get_mayastor_info):
    return get_mayastor_info.registration_info.features.logicalVolumeManager


@then("the instance shall report if it supports the LVM feature")
def the_instance_shall_report_if_it_supports_the_lvm_feature(
    get_mayastor_instance, get_mayastor_info, get_lvm_feature
):
    assert get_lvm_feature


@when("the user creates a pool specifying a URI representing an loop disk")
def the_user_creates_a_pool_specifying_a_uri_representing_an_loop_disk(
    get_mayastor_instance, volgrp_with_losetup_disk, create_pool
):
    create_pool(f"{volgrp_with_losetup_disk}", [pytest.disk], pb.Lvm)


@then("the lvm pool should be created")
def the_lvm_pool_should_be_created(find_pool):
    assert find_pool("lvmpool") is not None


@when("the user destroys a pool specifying type as lvm")
def the_user_destroys_a_pool_specifying_type_as_lvm(get_mayastor_instance):
    get_mayastor_instance.pool_rpc.DestroyPool(pb.DestroyPoolRequest(name="lvmpool"))


@then("the lvm pool should be removed")
def the_lvm_pool_should_be_removed(find_pool):
    assert find_pool("lvmpool") is None


@when("the user creates a lvs pool specifying a URI representing an aio disk")
def create_pool_from_aio_disk(get_mayastor_instance, create_pool, image_file):
    create_pool("lvspool", [f"aio://{image_file}"], pb.Lvs)


@then("the lvs pool should be created")
def the_lvm_pool_should_be_created(find_pool):
    assert find_pool("lvspool") is not None


@when("the user destroys a pool specifying type as lvs")
def the_user_destroys_a_pool_specifying_type_as_lvs(get_mayastor_instance):
    get_mayastor_instance.pool_rpc.DestroyPool(pb.DestroyPoolRequest(name="lvspool"))


@then("the lvs pool should be removed")
def the_lvs_pool_should_be_removed(find_pool):
    assert find_pool("lvspool") is None


@when("a user calls the listPool() gRPC method", target_fixture="list_pools")
def list_pools(get_mayastor_instance):
    return get_mayastor_instance.pool_rpc.ListPools(pb.ListPoolOptions()).pools


@then("the pool should be listed reporting the correct pooltype field")
def the_pool_should_be_listed_reporting_the_correct_pooltype_field(list_pools):
    for pool in list_pools:
        assert pool.capacity == 62914560
        assert pool.used == 0
        assert pool.state == pb.POOL_ONLINE
        if pool.name == "lvmpool":
            assert pool.pooltype == pb.Lvm
        if pool.name == "lvspool":
            assert pool.pooltype == pb.Lvs
