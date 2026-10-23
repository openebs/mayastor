"""LVM thin replica, snapshot and clone support feature tests."""

import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)
from common.command import losetup_disk, losetup_detach, run_cmd
from v1.mayastor import mayastor_mod, container_mod
import grpc
import pool_pb2 as pool_pb
import replica_pb2 as pb
import snapshot_pb2 as snap_pb
import subprocess

THIN_POOL = "lvmthinpool"
PLAIN_POOL = "lvmplainpool"
ROLLBACK_POOL = "lvmrollbackpool"
REPLICA_UUID = "22ca10d3-4f2b-4b95-9814-9181c025cc2b"
REPLICA_SIZE = 32 * 1024 * 1024
SNAP_UUID = "3f49d30d-a446-4b40-b3f6-f439345f1ce9"
CLONE_UUID = "4c58e12a-b557-4c51-c4e7-054a456e2dfa"
THICK_UUID = "5d69f41e-c668-4d62-a5f8-165b567e3e0b"
# written to the replica before it is snapshotted, read back from the clone
PATTERN_FILE = "/tmp/ms0-thin-pattern.bin"
PATTERN_MIB = 4


def lv_path(vg, uuid):
    """The device path of a replica or clone, which the lv is named after."""
    return f"/dev/{vg}/{uuid}"


@scenario(
    "features/lvm_thin_replica.feature", "creating a pool with a thin pool option"
)
def test_creating_a_pool_with_a_thin_pool_option():
    """creating a pool with a thin pool option."""


@scenario(
    "features/lvm_thin_replica.feature",
    "recreating a pool with the same thinpool option",
)
def test_recreating_a_pool_with_the_same_thinpool_option():
    """recreating a pool with the same thinpool option."""


@scenario(
    "features/lvm_thin_replica.feature",
    "recreating a pool with different thinpool options",
)
def test_recreating_a_pool_with_different_thinpool_options():
    """recreating a pool with different thinpool options."""


@scenario(
    "features/lvm_thin_replica.feature",
    "recreating a pool without a thin pool with a thinpool option",
)
def test_recreating_a_pool_without_a_thin_pool_with_a_thinpool_option():
    """recreating a pool without a thin pool with a thinpool option."""


@scenario(
    "features/lvm_thin_replica.feature",
    "creating a pool with a thin pool larger than the disk",
)
def test_creating_a_pool_with_a_thin_pool_larger_than_the_disk():
    """creating a pool with a thin pool larger than the disk."""


@scenario("features/lvm_thin_replica.feature", "creating a thin replica on a thin pool")
def test_creating_a_thin_replica_on_a_thin_pool():
    """creating a thin replica on a thin pool."""


@scenario(
    "features/lvm_thin_replica.feature", "creating a thin replica without a thin pool"
)
def test_creating_a_thin_replica_without_a_thin_pool():
    """creating a thin replica without a thin pool."""


@scenario("features/lvm_thin_replica.feature", "snapshotting a thin replica")
def test_snapshotting_a_thin_replica():
    """snapshotting a thin replica."""


@scenario("features/lvm_thin_replica.feature", "snapshotting a thick replica")
def test_snapshotting_a_thick_replica():
    """snapshotting a thick replica."""


@scenario("features/lvm_thin_replica.feature", "creating the same snapshot twice")
def test_creating_the_same_snapshot_twice():
    """creating the same snapshot twice."""


@scenario("features/lvm_thin_replica.feature", "cloning a snapshot")
def test_cloning_a_snapshot():
    """cloning a snapshot."""


@scenario("features/lvm_thin_replica.feature", "creating the same clone twice")
def test_creating_the_same_clone_twice():
    """creating the same clone twice."""


@scenario(
    "features/lvm_thin_replica.feature",
    "recreating a pool whose thin pool has grown",
)
def test_recreating_a_pool_whose_thin_pool_has_grown():
    """recreating a pool whose thin pool has grown."""


@scenario(
    "features/lvm_thin_replica.feature",
    "recreating a pool whose volumes were deactivated",
)
def test_recreating_a_pool_whose_volumes_were_deactivated():
    """recreating a pool whose volumes were deactivated."""


@scenario(
    "features/lvm_thin_replica.feature",
    "creating an lvs pool with lvm pool options",
)
def test_creating_an_lvs_pool_with_lvm_pool_options():
    """creating an lvs pool with lvm pool options."""


@given(
    parsers.parse('a mayastor instance "{name}"'),
    target_fixture="get_mayastor_instance",
)
def get_mayastor_instance(mayastor_mod, name):
    return mayastor_mod[name]


@pytest.fixture
def thin_disk():
    file = "/tmp/ms0-thin-disk0.img"
    disk = losetup_disk(file, "128M")
    yield disk
    losetup_detach(disk, file)


MIB = 1024 * 1024
THIN_POOL_SIZE = 64 * MIB
THIN_CHUNK = 128 * 1024


def thin_pool_request(name, disk, size, chunk=THIN_CHUNK):
    """A create request for an lvm pool with a thin pool, whose chunk size is
    the cluster size."""
    return pool_pb.CreatePoolRequest(
        name=name,
        disks=[disk],
        pooltype=pool_pb.Lvm,
        cluster_size=chunk,
        lvm=pool_pb.LvmPoolArgs(thin_pool_size=size),
    )


def create_thin_pool(mayastor, disk, size=THIN_POOL_SIZE):
    return mayastor.pool_rpc.CreatePool(thin_pool_request(THIN_POOL, disk, size))


@given("an lvm pool with a thin pool", target_fixture="thin_pool")
def thin_pool(get_mayastor_instance, thin_disk):
    pool = create_thin_pool(get_mayastor_instance, thin_disk)
    yield pool
    try:
        get_mayastor_instance.pool_rpc.DestroyPool(
            pool_pb.DestroyPoolRequest(name=THIN_POOL)
        )
    except grpc.RpcError:
        pass


@pytest.fixture
def plain_disk():
    file = "/tmp/ms0-plain-disk0.img"
    disk = losetup_disk(file, "64M")
    yield disk
    losetup_detach(disk, file)


@given("an lvm pool without a thin pool", target_fixture="plain_pool")
def plain_pool(get_mayastor_instance, plain_disk):
    pool = get_mayastor_instance.pool_rpc.CreatePool(
        pool_pb.CreatePoolRequest(
            name=PLAIN_POOL, disks=[plain_disk], pooltype=pool_pb.Lvm
        )
    )
    yield pool
    try:
        get_mayastor_instance.pool_rpc.DestroyPool(
            pool_pb.DestroyPoolRequest(name=PLAIN_POOL)
        )
    except grpc.RpcError:
        pass


@given("a disk without an lvm pool", target_fixture="bare_disk")
def bare_disk():
    file = "/tmp/ms0-bare-disk0.img"
    disk = losetup_disk(file, "128M")
    yield disk
    losetup_detach(disk, file)


@when(
    "the user creates a pool with an oversized thin pool",
    target_fixture="oversized_pool_error",
)
def oversized_pool_error(get_mayastor_instance, bare_disk):
    with pytest.raises(grpc.RpcError) as error:
        get_mayastor_instance.pool_rpc.CreatePool(
            # the chunk size makes lvcreate print warnings ahead of the out of
            # space error, which the error mapping must look past
            thin_pool_request(ROLLBACK_POOL, bare_disk, 8192 * MIB, 512 * 1024)
        )
    return error.value


@then("the pool creation fails for lack of space")
def the_pool_creation_fails_for_lack_of_space(oversized_pool_error):
    assert oversized_pool_error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED


@then("no volume group is left on the disk")
def no_volume_group_is_left_on_the_disk():
    out = subprocess.run(
        f"nix-sudo vgs {ROLLBACK_POOL} -ovg_name --noheadings",
        shell=True,
        check=False,
        capture_output=True,
    )
    # vgs must fail because the vg is gone, not for some other reason
    assert out.returncode != 0
    assert f'Volume group "{ROLLBACK_POOL}" not found' in out.stderr.decode("ascii")


@then("no physical volume is left on the disk")
def no_physical_volume_is_left_on_the_disk(bare_disk):
    out = subprocess.run(
        f"nix-sudo pvs {bare_disk} -opv_name --noheadings",
        shell=True,
        check=False,
        capture_output=True,
    )
    assert out.returncode != 0
    assert "No physical volume label read" in out.stderr.decode("ascii")


def create_thin_replica(mayastor, pool_uuid):
    return mayastor.replica_rpc.CreateReplica(
        pb.CreateReplicaRequest(
            name="r-thin",
            uuid=REPLICA_UUID,
            pooluuid=pool_uuid,
            size=REPLICA_SIZE,
            thin=True,
        )
    )


def create_snapshot(mayastor):
    return mayastor.snapshot_rpc.CreateReplicaSnapshot(
        snap_pb.CreateReplicaSnapshotRequest(
            replica_uuid=REPLICA_UUID,
            snapshot_uuid=SNAP_UUID,
            snapshot_name="snap-1",
            entity_id="volume-1",
            txn_id="txn-1",
        )
    )


@given("a thin replica", target_fixture="thin_replica")
@when("the user creates a thin replica", target_fixture="thin_replica")
def thin_replica(get_mayastor_instance, thin_pool):
    return create_thin_replica(get_mayastor_instance, thin_pool.uuid)


@when("the user creates a snapshot of the replica", target_fixture="replica_snapshot")
def replica_snapshot(get_mayastor_instance, thin_replica):
    return create_snapshot(get_mayastor_instance)


@given("a thin replica with a snapshot", target_fixture="replica_snapshot")
def thin_replica_with_snapshot(get_mayastor_instance, thin_pool):
    create_thin_replica(get_mayastor_instance, thin_pool.uuid)
    run_cmd(f"head -c {PATTERN_MIB}M /dev/urandom > {PATTERN_FILE}")
    run_cmd(
        f"nix-sudo dd if={PATTERN_FILE} of={lv_path(THIN_POOL, REPLICA_UUID)}"
        " bs=1M oflag=direct status=none"
    )
    return create_snapshot(get_mayastor_instance)


@when("the user creates the same snapshot again", target_fixture="duplicate_error")
def the_user_creates_the_same_snapshot_again(get_mayastor_instance, replica_snapshot):
    with pytest.raises(grpc.RpcError) as error:
        create_snapshot(get_mayastor_instance)
    return error.value


@given("a thick replica on the thin pool", target_fixture="thick_replica")
def thick_replica(get_mayastor_instance, thin_pool):
    return get_mayastor_instance.replica_rpc.CreateReplica(
        pb.CreateReplicaRequest(
            name="r-thick",
            uuid=THICK_UUID,
            pooluuid=thin_pool.uuid,
            size=REPLICA_SIZE,
            thin=False,
        )
    )


@when(
    "the user creates a snapshot of the thick replica",
    target_fixture="thick_snapshot_error",
)
def thick_snapshot_error(get_mayastor_instance, thick_replica):
    with pytest.raises(grpc.RpcError) as error:
        get_mayastor_instance.snapshot_rpc.CreateReplicaSnapshot(
            snap_pb.CreateReplicaSnapshotRequest(
                replica_uuid=THICK_UUID,
                snapshot_uuid=SNAP_UUID,
                snapshot_name="snap-thick",
                entity_id="volume-1",
                txn_id="txn-1",
            )
        )
    return error.value


@then("the snapshot creation fails with a precondition error")
def the_snapshot_creation_fails_with_a_precondition_error(thick_snapshot_error):
    assert thick_snapshot_error.code() == grpc.StatusCode.FAILED_PRECONDITION


@when(
    "the user creates a thin replica on the plain pool",
    target_fixture="thin_replica_error",
)
def thin_replica_error(get_mayastor_instance, plain_pool):
    with pytest.raises(grpc.RpcError) as error:
        create_thin_replica(get_mayastor_instance, plain_pool.uuid)
    return error.value


def create_clone(mayastor):
    return mayastor.snapshot_rpc.CreateSnapshotClone(
        snap_pb.CreateSnapshotCloneRequest(
            snapshot_uuid=SNAP_UUID, clone_name="clone-1", clone_uuid=CLONE_UUID
        )
    )


@when("the user creates a clone from the snapshot", target_fixture="snapshot_clone")
def snapshot_clone(get_mayastor_instance, replica_snapshot):
    return create_clone(get_mayastor_instance)


@when("the user creates the same clone again", target_fixture="duplicate_error")
def the_user_creates_the_same_clone_again(get_mayastor_instance, snapshot_clone):
    # the clone's bdev already has this uuid, which the grpc layer rejects
    with pytest.raises(grpc.RpcError) as error:
        create_clone(get_mayastor_instance)
    return error.value


@then("the second creation fails as already existing")
def the_second_creation_fails_as_already_existing(duplicate_error):
    assert duplicate_error.code() == grpc.StatusCode.ALREADY_EXISTS


@when("the user creates the same pool again")
def the_user_creates_the_same_pool_again(get_mayastor_instance, thin_disk, thin_pool):
    create_thin_pool(get_mayastor_instance, thin_disk)


@when(
    "the user creates the same pool with different thinpool options",
    target_fixture="recreate_pool_error",
)
def recreate_pool_error(get_mayastor_instance, thin_disk, thin_pool):
    # a smaller size is accepted, as the thin pool may have grown since its
    # create, so ask for a bigger one
    with pytest.raises(grpc.RpcError) as error:
        create_thin_pool(get_mayastor_instance, thin_disk, 96 * MIB)
    return error.value


@then("the pool creation fails with an invalid argument error")
def the_pool_creation_fails_with_an_invalid_argument_error(recreate_pool_error):
    assert recreate_pool_error.code() == grpc.StatusCode.INVALID_ARGUMENT


@then("the thin pool keeps its original size")
def the_thin_pool_keeps_its_original_size():
    out = subprocess.run(
        f"nix-sudo lvs {THIN_POOL}/mayastor-thinpool -olv_size"
        " --units=b --nosuffix --noheadings",
        shell=True,
        check=True,
        capture_output=True,
    )
    assert int(out.stdout.decode("ascii").strip()) == THIN_POOL_SIZE


def assert_thin_pool_lv(vg):
    out = subprocess.run(
        f"nix-sudo lvs {vg} -olv_name,lv_attr --noheadings",
        shell=True,
        check=True,
        capture_output=True,
    )
    lvs = out.stdout.decode("ascii")
    assert "mayastor-thinpool" in lvs
    attr = next(
        line.split()[1] for line in lvs.splitlines() if "mayastor-thinpool" in line
    )
    assert attr.startswith("t")


@then("a thin pool logical volume exists in the volume group")
def a_thin_pool_logical_volume_exists_in_the_volume_group():
    assert_thin_pool_lv(THIN_POOL)


PLAIN_THIN_POOL_SIZE = 32 * MIB


@when("the user creates the plain pool again with a thinpool option")
def the_user_creates_the_plain_pool_again_with_a_thinpool_option(
    get_mayastor_instance, plain_disk, plain_pool
):
    get_mayastor_instance.pool_rpc.CreatePool(
        thin_pool_request(PLAIN_POOL, plain_disk, PLAIN_THIN_POOL_SIZE)
    )


@then("the plain pool has a thin pool logical volume")
def the_plain_pool_has_a_thin_pool_logical_volume():
    assert_thin_pool_lv(PLAIN_POOL)


def assert_plain_disks_and_chunk(mayastor, name, disk):
    pools = mayastor.pool_list(None).pools
    pool = next(p for p in pools if p.name == name)
    assert pool.disks == [disk]
    assert pool.cluster_size == THIN_CHUNK


@then("the plain pool reports its disk and the thin pool chunk size")
def the_plain_pool_reports_its_disk_and_the_thin_pool_chunk_size(
    get_mayastor_instance, plain_disk
):
    assert_plain_disks_and_chunk(get_mayastor_instance, PLAIN_POOL, plain_disk)


@then("the pool reports its disk and the thin pool chunk size")
def the_pool_reports_its_disk_and_the_thin_pool_chunk_size(
    get_mayastor_instance, thin_disk
):
    assert_plain_disks_and_chunk(get_mayastor_instance, THIN_POOL, thin_disk)


@then("the replica is reported as thin provisioned")
def the_replica_is_reported_as_thin_provisioned(get_mayastor_instance, thin_replica):
    replicas = get_mayastor_instance.replica_list(None).replicas
    replica = next(r for r in replicas if r.uuid == REPLICA_UUID)
    assert replica.thin
    assert replica.pooltype == pool_pb.Lvm


@then("the thin replica creation fails with a precondition error")
def the_thin_replica_creation_fails_with_a_precondition_error(thin_replica_error):
    assert thin_replica_error.code() == grpc.StatusCode.FAILED_PRECONDITION


@then("the snapshot is listed with valid parameters")
def the_snapshot_is_listed_with_valid_parameters(get_mayastor_instance):
    snapshots = get_mayastor_instance.snapshot_rpc.ListSnapshot(
        snap_pb.ListSnapshotsRequest()
    ).snapshots
    snapshot = next(s for s in snapshots if s.snapshot_uuid == SNAP_UUID)
    assert snapshot.snapshot_name == "snap-1"
    assert snapshot.source_uuid == REPLICA_UUID
    assert snapshot.entity_id == "volume-1"
    assert snapshot.txn_id == "txn-1"
    assert snapshot.valid_snapshot


def list_snapshots(mayastor):
    return mayastor.snapshot_rpc.ListSnapshot(snap_pb.ListSnapshotsRequest()).snapshots


@then("the replica can be destroyed while its snapshot exists")
def the_replica_can_be_destroyed_while_its_snapshot_exists(get_mayastor_instance):
    get_mayastor_instance.replica_destroy(REPLICA_UUID)
    replicas = get_mayastor_instance.replica_list(None).replicas
    assert all(r.uuid != REPLICA_UUID for r in replicas)
    snapshot = next(
        s for s in list_snapshots(get_mayastor_instance) if s.snapshot_uuid == SNAP_UUID
    )
    assert snapshot.source_uuid == REPLICA_UUID


@then("the clone is listed as a thin clone of the snapshot")
def the_clone_is_listed_as_a_thin_clone_of_the_snapshot(get_mayastor_instance):
    replicas = get_mayastor_instance.replica_list(None).replicas
    clone = next(r for r in replicas if r.uuid == CLONE_UUID)
    assert clone.thin
    assert clone.is_clone
    assert clone.snapshot_uuid == SNAP_UUID


def destroy_snapshot(mayastor):
    mayastor.snapshot_rpc.DestroySnapshot(
        snap_pb.DestroySnapshotRequest(snapshot_uuid=SNAP_UUID)
    )


@then("the replica and the snapshot can be destroyed while the clone exists")
def the_replica_and_the_snapshot_can_be_destroyed_while_the_clone_exists(
    get_mayastor_instance,
):
    get_mayastor_instance.replica_destroy(REPLICA_UUID)
    destroy_snapshot(get_mayastor_instance)
    assert all(
        s.snapshot_uuid != SNAP_UUID for s in list_snapshots(get_mayastor_instance)
    )
    replicas = get_mayastor_instance.replica_list(None).replicas
    assert all(r.uuid != REPLICA_UUID for r in replicas)
    clone = next(r for r in replicas if r.uuid == CLONE_UUID)
    assert clone.is_clone
    assert clone.snapshot_uuid == SNAP_UUID


@then("the clone still holds the data written to the replica")
def the_clone_still_holds_the_data_written_to_the_replica(get_mayastor_instance):
    out = subprocess.run(
        f"nix-sudo dd if={lv_path(THIN_POOL, CLONE_UUID)} bs=1M count={PATTERN_MIB}"
        " iflag=direct status=none",
        shell=True,
        check=True,
        capture_output=True,
    )
    with open(PATTERN_FILE, "rb") as pattern:
        assert out.stdout == pattern.read()
    run_cmd(f"rm -f {PATTERN_FILE}")
    get_mayastor_instance.replica_destroy(CLONE_UUID)


@given("the thin pool was extended outside the engine")
def the_thin_pool_was_extended_outside_the_engine(thin_pool):
    # what an admin, or lvm's autoextend, does to a thin pool running full
    run_cmd(f"nix-sudo lvextend -q -L+16m {THIN_POOL}/mayastor-thinpool")


def find_thin_replica(mayastor):
    replicas = mayastor.replica_list(None).replicas
    return next(r for r in replicas if r.uuid == REPLICA_UUID)


@given("a thin replica holding data", target_fixture="thin_usage")
def thin_replica_holding_data(get_mayastor_instance, thin_pool):
    create_thin_replica(get_mayastor_instance, thin_pool.uuid)
    run_cmd(f"head -c {PATTERN_MIB}M /dev/urandom > {PATTERN_FILE}")
    run_cmd(
        f"nix-sudo dd if={PATTERN_FILE} of={lv_path(THIN_POOL, REPLICA_UUID)}"
        " bs=1M oflag=direct status=none"
    )
    run_cmd(f"rm -f {PATTERN_FILE}")
    pools = get_mayastor_instance.pool_list(None).pools
    pool = next(p for p in pools if p.name == THIN_POOL)
    allocated = find_thin_replica(get_mayastor_instance).usage.allocated_bytes
    assert allocated > 0
    return pool.used, allocated


@when(
    "the pool is exported, its volumes are deactivated and it is created again",
    target_fixture="recreated_pool",
)
def the_pool_is_recreated_with_its_volumes_deactivated(
    get_mayastor_instance, thin_disk, thin_usage
):
    get_mayastor_instance.pool_rpc.ExportPool(pool_pb.ExportPoolRequest(name=THIN_POOL))
    # as after a reboot, since our volume groups are not autoactivated
    run_cmd(f"nix-sudo vgchange -q -an {THIN_POOL}")
    return create_thin_pool(get_mayastor_instance, thin_disk)


@then("the pool reports the space it used before")
def the_pool_reports_the_space_it_used_before(recreated_pool, thin_usage):
    assert recreated_pool.used == thin_usage[0]


@then("the thin replica reports the bytes it allocated before")
def the_thin_replica_reports_the_bytes_it_allocated_before(
    get_mayastor_instance, thin_usage
):
    replica = find_thin_replica(get_mayastor_instance)
    assert replica.usage.allocated_bytes == thin_usage[1]


@when(
    "the user creates an lvs pool with a thin pool size",
    target_fixture="lvs_pool_error",
)
def lvs_pool_error(get_mayastor_instance):
    with pytest.raises(grpc.RpcError) as error:
        get_mayastor_instance.pool_rpc.CreatePool(
            pool_pb.CreatePoolRequest(
                name="lvsthinpool",
                disks=["malloc:///lvsthin?size_mb=64"],
                pooltype=pool_pb.Lvs,
                lvm=pool_pb.LvmPoolArgs(thin_pool_size=THIN_POOL_SIZE),
            )
        )
    return error.value


@then("the lvs pool creation fails with an invalid argument error")
def the_lvs_pool_creation_fails_with_an_invalid_argument_error(lvs_pool_error):
    assert lvs_pool_error.code() == grpc.StatusCode.INVALID_ARGUMENT
