"""LVM replica support feature tests."""

import pytest
from test_bdd_replica import replica_uuid, replica_size
from test_bdd_lvm import image_file
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)
from common.command import run_cmd
from common.mayastor import mayastor_mod
import mayastor_pb2 as pb
import subprocess


@scenario('features/lvm_replica.feature', 'Creating an lvm volume on an imported lvm volume group')
def test_creating_an_lvm_volume_on_an_imported_lvm_volume_group():
    """Creating an lvm volume on an imported lvm volume group."""

@scenario('features/lvm_replica.feature', 'Destroying a replica backed by lvm pool')
def test_destroying_a_replica_backed_by_lvm_pool():
    """Destroying a replica backed by lvm pool"""

@scenario('features/lvm_replica.feature', 'Listing replicas from either an LVS or LVM pool')
def test_listing_replicas_from_either_an_lvs_or_lvm_pool():
    """Listing replicas from either an LVS or LVM pool"""

@pytest.fixture
def create_replica(get_mayastor_instance):
    def create(uuid, pool, size, share, pooltype):
        get_mayastor_instance.ms.CreateReplica (
            pb.CreateReplicaRequest(uuid=uuid, pool=pool, size=size, share=share, pooltype=pooltype)
        )
    yield create

@pytest.fixture
def find_replica(get_mayastor_instance):
    def find(uuid):
        for replica in get_mayastor_instance.ms.ListReplicas(pb.Null()).replicas:
            if replica.uuid == uuid:
                return replica
            return None

    yield find

@pytest.fixture
def create_pool(get_mayastor_instance):
    def create(name, disks, pooltype):
        get_mayastor_instance.ms.CreatePool(
            pb.CreatePoolRequest(name=name, disks=disks, pooltype=pooltype)
        )

    yield create

@pytest.fixture
def volgrp_with_losetup_disk():
    pool_name="lvmpool"
    p = subprocess.run(f"sudo vgs {pool_name}", shell=True, check=False)
    # if volume group already exists then dont create it again
    if p.returncode != 0:
        file = "/tmp/ms0-disk0.img"
        run_cmd(f"rm -f '{file}'", True)
        run_cmd(f"truncate -s 128M '{file}'", True)
        out = subprocess.run(f"sudo losetup -f '{file}' --show", shell=True, check=True, capture_output=True)
        disk = out.stdout.decode('ascii').strip('\n')
        run_cmd(f"sudo pvcreate '{disk}'", True)
        run_cmd(f"sudo vgcreate '{pool_name}' '{disk}'", True)
    yield pool_name

@given(
    parsers.parse('a mayastor instance "{name}"'),
    target_fixture="get_mayastor_instance",
)
def get_mayastor_instance(mayastor_mod, name):
    return mayastor_mod[f"{name}"]


@given(
    parsers.parse('an LVM VG backed pool called "{pool_name}"'),
    target_fixture="create_pool_on_vol_group",
)
def create_pool_on_vol_group(volgrp_with_losetup_disk, create_pool, pool_name):
    create_pool(f"{volgrp_with_losetup_disk}", [], pb.Lvm)

@given('an LVS pool with a replica')
def an_lvs_pool_with_a_replica(create_pool, create_replica, replica_size, image_file):
    create_pool("lvspool", [f"aio://{image_file}"], pb.Lvs)
    create_replica("uuid1", "lvspool", replica_size, pb.REPLICA_NONE, pb.Lvs)

@when(
    parsers.parse('a user calls the createreplica on pool "{pool_name}"'),
    target_fixture="a_user_calls_the_create_replica",
)
@given('an LVM backed replica')
def a_user_calls_the_create_replica(create_replica, pool_name, replica_uuid, replica_size):
   create_replica(replica_uuid, pool_name, replica_size, pb.REPLICA_NONE, pb.Lvm)

@then('a vol should be created on that pool with the given uuid as its name')
def a_vol_should_be_created_on_that_pool_with_the_given_uuid_as_its_name(find_replica, replica_uuid):
    assert find_replica(replica_uuid) != None

@when('a user calls destroy replica')
def a_user_calls_destroy_replica(get_mayastor_instance, replica_uuid):
    get_mayastor_instance.ms.DestroyReplica(
        pb.DestroyReplicaRequest(uuid=replica_uuid)
    )

@then('the replica gets destroyed')
def the_replica_gets_destroyed(find_replica, replica_uuid):
    assert find_replica(replica_uuid) == None

@when('a user calls list replicas', target_fixture="list_replicas")
def list_replicas(get_mayastor_instance):
    return get_mayastor_instance.ms.ListReplicas(pb.Null()).replicas

@then('all replicas should be listed')
def all_replicas_should_be_listed(list_replicas, replica_uuid, replica_size):
    for replica in list_replicas:
        assert replica.size == replica_size
        if replica.uuid == replica_uuid: 
            assert replica.pooltype == pb.Lvm
        if replica.uuid == "uuid1":
            assert replica.pooltype == pb.Lvs
