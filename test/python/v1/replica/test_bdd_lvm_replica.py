"""LVM replica support feature tests."""

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
import pool_pb2 as pool_pb
import replica_pb2 as pb
import common_pb2 as common_pb
import subprocess

LVS_LV_UUID = "5b3d904f-d695-4a28-b3d6-b9fc1cbb39a3"
LVM_LV_UUID = "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
REPLICA_SIZE = 32 * 1024 * 1024


@scenario(
    "features/lvm_replica.feature",
    "Creating an lvm volume on an imported lvm volume group",
)
def test_creating_an_lvm_volume_on_an_imported_lvm_volume_group():
    """Creating an lvm volume on an imported lvm volume group."""


@scenario("features/lvm_replica.feature", "Destroying a replica backed by lvm pool")
def test_destroying_a_replica_backed_by_lvm_pool():
    """Destroying a replica backed by lvm pool"""


@scenario(
    "features/lvm_replica.feature", "Listing replicas from either an LVS or LVM pool"
)
def test_listing_replicas_from_either_an_lvs_or_lvm_pool():
    """Listing replicas from either an LVS or LVM pool"""


@pytest.fixture
def create_replica(get_mayastor_instance):
    def create(uuid, pool, size, share, pooltype):
        get_mayastor_instance.replica_rpc.CreateReplica(
            pb.CreateReplicaRequest(
                name=uuid,
                uuid=uuid,
                pooluuid=pool,
                size=size,
                share=share,
            )
        )

    yield create


@pytest.fixture
def find_replica(get_mayastor_instance):
    def find(uuid, pooltype):
        for replica in get_mayastor_instance.replica_rpc.ListReplicas(
            pb.ListReplicaOptions(pooltypes=[pooltype])
        ).replicas:
            if replica.uuid == uuid:
                return replica
            return None

    yield find


@pytest.fixture
def create_pool(get_mayastor_instance):
    def create(name, disks, pooltype):
        return get_mayastor_instance.pool_rpc.CreatePool(
            pool_pb.CreatePoolRequest(name=name, disks=disks, pooltype=pooltype)
        )

    yield create


@pytest.fixture
def volgrp_with_losetup_disk(container_mod):
    pool_name = "lvmpool"
    p = subprocess.run(f"sudo vgs {pool_name}", shell=True, check=False)
    file = "/tmp/ms0-disk0.img"
    # if volume group already exists then don't create it again
    if p.returncode != 0:
        run_cmd(f"rm -f '{file}'", True)
        run_cmd(f"truncate -s 128M '{file}'", True)
        out = subprocess.run(
            f"sudo -E losetup -f '{file}' --show",
            shell=True,
            check=True,
            capture_output=True,
        )
        disk = out.stdout.decode("ascii").strip("\n")
        run_cmd(f"sudo -E pvcreate '{disk}'", True)
        run_cmd(f"sudo -E vgcreate '{pool_name}' '{disk}'", True)
    out = subprocess.run(
        f"sudo -E pvs -opv_name --select=vg_name={pool_name} --noheadings",
        shell=True,
        check=True,
        capture_output=True,
    )
    disk = out.stdout.decode("ascii").strip("\n").lstrip()
    pytest.disk = disk
    out = subprocess.run(
        f"sudo -E vgs lvmpool -ovg_uuid --noheadings",
        shell=True,
        check=True,
        capture_output=True,
    )
    pytest.vg_uuid = out.stdout.decode("ascii").strip("\n").lstrip()
    yield pool_name
    try:
        run_cmd(f"sudo -E vgremove -y {pool_name}", True)
    except:
        pass
    if p.returncode != 0:
        run_cmd(f"sudo -E losetup -d {disk}", True)
    run_cmd(f"rm -f '{file}'", True)


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
def create_pool_on_vol_group(
    get_mayastor_instance, volgrp_with_losetup_disk, create_pool
):
    name = f"{volgrp_with_losetup_disk}"
    create_pool(name, [pytest.disk], pool_pb.Lvm)
    yield
    try:
        get_mayastor_instance.pool_rpc.DestroyPool(
            pool_pb.DestroyPoolRequest(name=name)
        )
    except:
        pass


@given("an LVS pool with a replica")
def an_lvs_pool_with_a_replica(create_pool, create_replica):
    pool = create_pool("lvspool", ["malloc:///disk0?size_mb=64"], pool_pb.Lvs)
    create_replica(
        LVS_LV_UUID,
        pool.uuid,
        REPLICA_SIZE,
        share_protocol("none"),
        pool_pb.Lvs,
    )


@when(
    parsers.parse('a user calls the createreplica on pool "{pool_name}"'),
    target_fixture="a_user_calls_the_create_replica",
)
@given("an LVM backed replica")
def a_user_calls_the_create_replica(get_mayastor_instance, create_replica):
    create_replica(
        LVM_LV_UUID, pytest.vg_uuid, REPLICA_SIZE, share_protocol("none"), pool_pb.Lvm
    )
    yield
    try:
        get_mayastor_instance.replica_rpc.DestroyReplica(
            pb.DestroyReplicaRequest(uuid=LVM_LV_UUID)
        )
    except grpc.RpcError as rpc_error:
        if rpc_error.code() == grpc.StatusCode.NOT_FOUND:
            pass


@then("an lv should be created on the lvmpool")
def an_lv_should_be_created_on_the_lvmpool(find_replica):
    assert find_replica(LVM_LV_UUID, pool_pb.Lvm) is not None


@when("a user calls destroy replica")
def a_user_calls_destroy_replica(get_mayastor_instance):
    get_mayastor_instance.replica_rpc.DestroyReplica(
        pb.DestroyReplicaRequest(uuid=LVM_LV_UUID)
    )


@then("the replica gets destroyed")
def the_replica_gets_destroyed(find_replica):
    assert find_replica(LVM_LV_UUID, pool_pb.Lvm) is None


@when("a user calls list replicas", target_fixture="list_replicas")
def list_replicas(get_mayastor_instance):
    return get_mayastor_instance.replica_rpc.ListReplicas(
        pb.ListReplicaOptions(pooltypes=[pool_pb.Lvm, pool_pb.Lvs])
    ).replicas


@then("all replicas should be listed")
def all_replicas_should_be_listed(list_replicas):
    for replica in list_replicas:
        assert replica.size == REPLICA_SIZE
        if replica.uuid == LVM_LV_UUID:
            assert replica.pooltype == pool_pb.Lvm
        if replica.uuid == LVS_LV_UUID:
            assert replica.pooltype == pool_pb.Lvs


def share_protocol(name):
    PROTOCOLS = {
        "none": common_pb.NONE,
        "nvmf": common_pb.NVMF,
        "iscsi": common_pb.ISCSI,
    }
    return PROTOCOLS[name]
