"""LVM replica support feature tests."""

import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)
from common.command import run_cmd, losetup_disk, losetup_detach
from v1.mayastor import mayastor_mod, container_mod
import grpc
import pool_pb2 as pool_pb
import replica_pb2 as pb
import common_pb2 as common_pb
import stats_pb2 as stats_pb
import subprocess

LVS_LV_UUID = "5b3d904f-d695-4a28-b3d6-b9fc1cbb39a3"
LVM_LV_UUID = "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
LVM_LV_NAME = "lvm-replica-1"
REPLICA_SIZE = 32 * 1024 * 1024


@scenario(
    "features/lvm_replica.feature",
    "Creating an lvm volume on an imported lvm volume group",
)
def test_creating_an_lvm_volume_on_an_imported_lvm_volume_group():
    """Creating an lvm volume on an imported lvm volume group."""


@scenario(
    "features/lvm_replica.feature",
    "Creating an lvm volume on a pool identified by name",
)
def test_creating_an_lvm_volume_on_a_pool_identified_by_name():
    """Creating an lvm volume on a pool identified by name."""


@scenario("features/lvm_replica.feature", "Getting io stats of an lvm replica")
def test_getting_io_stats_of_an_lvm_replica():
    """Getting io stats of an lvm replica."""


@scenario(
    "features/lvm_replica.feature",
    "Getting pool io stats while an lvm pool exists",
)
def test_getting_pool_io_stats_while_an_lvm_pool_exists():
    """Getting pool io stats while an lvm pool exists."""


@scenario("features/lvm_replica.feature", "Expanding an lvm pool whose disk has grown")
def test_expanding_an_lvm_pool_whose_disk_has_grown():
    """Expanding an lvm pool whose disk has grown."""


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
    def create(uuid, pool, size, share, pooltype, name=None):
        get_mayastor_instance.replica_rpc.CreateReplica(
            pb.CreateReplicaRequest(
                name=name or uuid,
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
    p = subprocess.run(f"nix-sudo vgs {pool_name}", shell=True, check=False)
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
        run_cmd(f"nix-sudo pvcreate '{disk}'", True)
        run_cmd(f"nix-sudo vgcreate '{pool_name}' '{disk}'", True)
    out = subprocess.run(
        f"nix-sudo pvs -opv_name --select=vg_name={pool_name} --noheadings",
        shell=True,
        check=True,
        capture_output=True,
    )
    disk = out.stdout.decode("ascii").strip("\n").lstrip()
    pytest.disk = disk
    out = subprocess.run(
        f"nix-sudo vgs lvmpool -ovg_uuid --noheadings",
        shell=True,
        check=True,
        capture_output=True,
    )
    pytest.vg_uuid = out.stdout.decode("ascii").strip("\n").lstrip()
    yield pool_name
    try:
        run_cmd(f"nix-sudo vgremove -y {pool_name}", True)
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


@given("an LVM backed replica with a name of its own")
def an_lvm_backed_replica_with_a_name_of_its_own(get_mayastor_instance, create_replica):
    create_replica(
        LVM_LV_UUID,
        pytest.vg_uuid,
        REPLICA_SIZE,
        share_protocol("none"),
        pool_pb.Lvm,
        name=LVM_LV_NAME,
    )
    yield
    try:
        get_mayastor_instance.replica_rpc.DestroyReplica(
            pb.DestroyReplicaRequest(uuid=LVM_LV_UUID)
        )
    except grpc.RpcError as rpc_error:
        if rpc_error.code() == grpc.StatusCode.NOT_FOUND:
            pass


@when("a user calls the createreplica with the pool name instead of its uuid")
def a_user_calls_the_create_replica_by_pool_name(get_mayastor_instance, create_replica):
    # pooluuid carries either a uuid or a name, so the name has to resolve too
    create_replica(
        LVM_LV_UUID, "lvmpool", REPLICA_SIZE, share_protocol("none"), pool_pb.Lvm
    )
    yield
    try:
        get_mayastor_instance.replica_rpc.DestroyReplica(
            pb.DestroyReplicaRequest(uuid=LVM_LV_UUID)
        )
    except grpc.RpcError as rpc_error:
        if rpc_error.code() == grpc.StatusCode.NOT_FOUND:
            pass


GROW_POOL = "lvmgrowpool"
GROW_IMG = "/tmp/ms0-grow-disk0.img"


@given("an lvm pool on a disk of its own", target_fixture="grow_pool_disk")
def grow_pool_disk(get_mayastor_instance, create_pool):
    # its own disk and vg, because growing the shared one would change the
    # capacity the other scenarios see
    disk = losetup_disk(GROW_IMG, "128M")
    create_pool(GROW_POOL, [disk], pool_pb.Lvm)
    yield disk
    try:
        get_mayastor_instance.pool_rpc.DestroyPool(
            pool_pb.DestroyPoolRequest(name=GROW_POOL)
        )
    except grpc.RpcError:
        pass
    losetup_detach(disk, GROW_IMG)


def pool_capacity(mayastor, name):
    pools = mayastor.pool_rpc.ListPools(pool_pb.ListPoolOptions()).pools
    return next(p.capacity for p in pools if p.name == name)


@when(
    "the disk is expanded and the user expands the pool",
    target_fixture="grown_capacities",
)
def the_disk_is_expanded_and_the_user_expands_the_pool(
    get_mayastor_instance, grow_pool_disk
):
    before = pool_capacity(get_mayastor_instance, GROW_POOL)
    run_cmd(f"truncate -s 256M '{GROW_IMG}'", True)
    run_cmd(f"sudo -E losetup -c {grow_pool_disk}", True)
    grown = get_mayastor_instance.pool_rpc.GrowPoolV2(
        pool_pb.GrowPoolRequest(name=GROW_POOL)
    )
    return (before, grown.capacity)


@then("the pool reports the larger capacity")
def the_pool_reports_the_larger_capacity(get_mayastor_instance, grown_capacities):
    before, reported = grown_capacities
    # the response itself has to show the growth, not just a later list
    assert reported > before
    assert pool_capacity(get_mayastor_instance, GROW_POOL) == reported


@given("an LVS pool", target_fixture="lvs_pool")
def an_lvs_pool(get_mayastor_instance, create_pool):
    # its own name and disk, so it cannot clash with the other lvs pool
    name = "lvsstatspool"
    yield create_pool(name, ["malloc:///disk1?size_mb=64"], pool_pb.Lvs)
    try:
        get_mayastor_instance.pool_rpc.DestroyPool(
            pool_pb.DestroyPoolRequest(name=name)
        )
    except grpc.RpcError:
        pass


@when("a user calls get replica io stats", target_fixture="replica_io_stats")
def a_user_calls_get_replica_io_stats(get_mayastor_instance):
    return get_mayastor_instance.stats_rpc.GetReplicaIoStats(
        stats_pb.ListStatsOption()
    ).stats


@then("the lvm replica is listed with its own name and pool")
def the_lvm_replica_is_listed_with_its_own_name_and_pool(replica_io_stats):
    # the lv's bdev is named after its device path, so check that the
    # replica's own name and uuid are reported
    stats = next(s for s in replica_io_stats if s.stats.uuid == LVM_LV_UUID)
    assert stats.stats.name == LVM_LV_NAME
    assert stats.poolname == "lvmpool"
    assert stats.pooluuid == pytest.vg_uuid


@when("a user calls get pool io stats", target_fixture="pool_io_stats")
def a_user_calls_get_pool_io_stats(get_mayastor_instance):
    return get_mayastor_instance.stats_rpc.GetPoolIoStats(
        stats_pb.ListStatsOption()
    ).stats


@then("the lvm pool and the lvs pool are both reported")
def the_lvm_pool_and_the_lvs_pool_are_both_reported(pool_io_stats):
    # an lvm pool has no pool level device, so it reports the total of its
    # replicas under the volume group's own name and uuid
    lvm = next(s for s in pool_io_stats if s.name == "lvmpool")
    assert lvm.uuid == pytest.vg_uuid
    assert lvm.tick_rate > 0
    assert any(s.name == "lvsstatspool" for s in pool_io_stats)


@then("the lvm pool stats are the total of its replica stats")
def the_lvm_pool_stats_are_the_total_of_its_replica_stats(
    get_mayastor_instance, pool_io_stats
):
    lvm = next(s for s in pool_io_stats if s.name == "lvmpool")
    replicas = [
        s.stats
        for s in get_mayastor_instance.stats_rpc.GetReplicaIoStats(
            stats_pb.ListStatsOption()
        ).stats
        if s.pooluuid == pytest.vg_uuid
    ]
    assert any(r.uuid == LVM_LV_UUID for r in replicas)
    for counter in [
        "num_read_ops",
        "bytes_read",
        "num_write_ops",
        "bytes_written",
        "num_unmap_ops",
        "bytes_unmapped",
        "read_latency_ticks",
        "write_latency_ticks",
        "unmap_latency_ticks",
    ]:
        assert getattr(lvm, counter) == sum(getattr(r, counter) for r in replicas)


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
