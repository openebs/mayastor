import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

from v1.mayastor import container_mod, mayastor_mod

import grpc
import pool_pb2 as pool_pb
import replica_pb2 as replica_pb
import common_pb2 as common_pb


@scenario("features/replica.feature", "creating a replica")
def test_creating_a_replica():
    """Creating a replica."""


@scenario("features/replica.feature", 'creating a replica shared over "iscsi"')
def test_fail_creating_a_replica_shared_over_iscsi():
    """Creating a replica shared over "iscsi"."""


@scenario("features/replica.feature", 'creating a replica shared over "nvmf"')
def test_creating_a_replica_shared_over_nvmf():
    """Creating a replica shared over "nvmf"."""


@scenario(
    "features/replica.feature", "creating a replica with a name that already exists"
)
def test_creating_a_replica_with_a_name_that_already_exists():
    """Creating a replica with a name that already exists."""


@scenario(
    "features/replica.feature", "creating a replica with a uuid that already exists"
)
def test_creating_a_replica_with_a_uuid_that_already_exists():
    """Creating a replica with a uuid that already exists."""


@scenario("features/replica.feature", "destroying a replica")
def test_destroying_a_replica():
    """Destroying a replica."""


@scenario("features/replica.feature", "destroying a replica that does not exist")
def test_fail_destroying_a_replica_that_does_not_exist():
    """Destroying a replica that does not exist."""


@scenario("features/replica.feature", "listing replicas")
def test_listing_replicas():
    """Listing replicas."""


@scenario("features/replica.feature", "listing replicas by replica name")
def test_listing_replicas_by_replica_name():
    """Listing replicas by replica name."""


@scenario(
    "features/replica.feature", "listing replicas by pool name on a pool with replicas"
)
def test_listing_replicas_by_pool_name_with_replica():
    """Listing replicas by pool name on a pool with replicas."""


@scenario(
    "features/replica.feature",
    "listing replicas by pool name with a non-existent pool name",
)
def test_listing_replicas_by_pool_name_with_non_existent_pool():
    """Listing replicas by pool name with a non-existent pool name."""


@scenario("features/replica.feature", 'sharing a replica over "iscsi"')
def test_fail_sharing_a_replica_over_iscsi():
    """Sharing a replica over "iscsi"."""


@scenario("features/replica.feature", 'sharing a replica over "nvmf"')
def test_sharing_a_replica_over_nvmf():
    """Sharing a replica over "nvmf"."""


@scenario(
    "features/replica.feature",
    "sharing a replica that is already shared with a different protocol",
)
def test_fail_sharing_a_replica_that_is_already_shared_with_a_different_protocol():
    """Sharing a replica that is already shared with a different protocol."""


@scenario(
    "features/replica.feature",
    "sharing a replica that is already shared with the same protocol",
)
def test_sharing_a_replica_that_is_already_shared_with_the_same_protocol():
    """Sharing a replica that is already shared with the same protocol."""


@scenario("features/replica.feature", "unsharing a shared replica")
def test_unsharing_a_shared_replica():
    """Unsharing a shared replica."""


@scenario("features/replica.feature", "unsharing a replica that is not shared")
def test_unsharing_a_non_shared_replica():
    """Unsharing a replica that is not shared."""


@scenario("features/replica.feature", "create a snapshot for a standalone replica")
def test_create_a_snapshot_for_a_standalone_replica():
    """create a snapshot for a standalone replica."""


@pytest.mark.skip(reason="todo")
@scenario("features/replica.feature", "reading from a shared replica")
def test_reading_from_a_shared_replica():
    """Reading from a shared replica."""


@pytest.mark.skip(reason="todo")
@scenario("features/replica.feature", "writing to a shared replica")
def test_writing_to_a_shared_replica():
    """Writing to a shared replica."""


def share_protocol(name):
    PROTOCOLS = {
        "none": common_pb.NONE,
        "nvmf": common_pb.NVMF,
        "iscsi": common_pb.ISCSI,
    }
    return PROTOCOLS[name]


@pytest.fixture
def mayastor_instance(mayastor_mod):
    yield mayastor_mod["ms0"]


@pytest.fixture
def mayastor_lvs_pool(mayastor_instance):
    pool = mayastor_instance.pool_rpc.CreatePool(
        pool_pb.CreatePoolRequest(
            name="lvs-pool0", disks=["malloc:///disk0?size_mb=512"]
        )
    )
    yield pool.name, pool.uuid
    mayastor_instance.pool_rpc.DestroyPool(pool_pb.DestroyPoolRequest(name=pool.name))


@pytest.fixture
def replica_name():
    yield "replica-1"


@pytest.fixture
def replica_uuid():
    yield "22ca10d3-4f2b-4b95-9814-9181c025cc1a"


@pytest.fixture
def replica_size():
    yield 32 * 1024 * 1024


@pytest.fixture
def find_replica(mayastor_instance, mayastor_lvs_pool):
    def find(name, uuid):
        for replica in mayastor_instance.replica_rpc.ListReplicas(
            replica_pb.ListReplicaOptions()
        ).replicas:
            if replica.name == name and replica.uuid == uuid:
                return replica
        return None

    yield find


@pytest.fixture
def current_replicas(mayastor_instance, mayastor_lvs_pool):
    replicas = {}
    yield replicas
    for uuid in replicas.keys():
        mayastor_instance.replica_rpc.DestroyReplica(
            replica_pb.DestroyReplicaRequest(uuid=uuid)
        )


@pytest.fixture
def create_lvs_replica(mayastor_instance, mayastor_lvs_pool, current_replicas):
    def create(name, uuid, size, share):
        replica = mayastor_instance.replica_rpc.CreateReplica(
            replica_pb.CreateReplicaRequest(
                pooluuid=mayastor_lvs_pool[1],
                name=name,
                uuid=uuid,
                size=size,
                share=share,
            )
        )
        current_replicas[uuid] = replica

    yield create


@given(parsers.parse('a mayastor instance "{name}"'))
def get_instance():
    pass


@given(parsers.parse('an lvs backed pool "{name}"'))
def get_pool():
    pass


@given(
    parsers.parse('a replica "{name}" with uuid "{uuid}"'),
    target_fixture="get_replica",
)
def get_replica(create_lvs_replica, name, uuid, replica_size):
    create_lvs_replica(name, uuid, replica_size, share=share_protocol("none"))


@given("a replica that is unshared")
def get_unshared_replica(create_lvs_replica, replica_name, replica_uuid, replica_size):
    create_lvs_replica(
        replica_name, replica_uuid, replica_size, share=share_protocol("none")
    )


@given(parsers.parse('a replica shared over "{share}"'), target_fixture="share_replica")
def get_shared_replica(
    create_lvs_replica, replica_name, replica_uuid, replica_size, share
):
    create_lvs_replica(
        replica_name, replica_uuid, replica_size, share=share_protocol(share)
    )


@given(
    parsers.parse('a standalone replica "{name}" uuid "{uuid}"'),
    target_fixture="get_replica_for_snapshot",
)
def get_replica_for_snapshot(create_lvs_replica, name, uuid, replica_size):
    create_lvs_replica(name, uuid, replica_size, share=share_protocol("none"))


@when(
    "the user creates a replica with a name that already exists",
    target_fixture="create_replica_with_existing_name",
)
def create_replica_with_existing_name(create_lvs_replica, replica_name, replica_size):
    with pytest.raises(grpc.RpcError) as error:
        create_lvs_replica(
            replica_name,
            "11ca10d3-4f2b-4b95-9814-9181c025cc1a",
            replica_size,
            share=share_protocol("none"),
        )
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS


@when(
    "the user creates a replica with a uuid that already exists",
    target_fixture="create_replica_with_existing_uuid",
)
def create_replica_with_existing_uuid(create_lvs_replica, replica_uuid, replica_size):
    with pytest.raises(grpc.RpcError) as error:
        create_lvs_replica(
            "replica-2", replica_uuid, replica_size, share=share_protocol("none")
        )
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS


@when("the user creates an unshared replica")
def create_unshared_replica(
    create_lvs_replica, replica_name, replica_uuid, replica_size
):
    create_lvs_replica(
        replica_name, replica_uuid, replica_size, share=share_protocol("none")
    )


@when(
    parsers.parse('the user creates a replica shared over "{protocol}"'),
)
def create_a_shared_replica(
    create_lvs_replica, replica_name, replica_uuid, replica_size, protocol
):
    create_lvs_replica(
        replica_name, replica_uuid, replica_size, share=share_protocol(protocol)
    )


@when(
    parsers.parse('the user attempts to create a replica shared over "{protocol}"'),
    target_fixture="attempt_to_create_a_shared_replica",
)
def attempt_to_create_a_shared_replica(
    create_lvs_replica, replica_name, replica_uuid, replica_size, protocol
):
    with pytest.raises(grpc.RpcError) as error:
        create_lvs_replica(
            replica_name, replica_uuid, replica_size, share=share_protocol(protocol)
        )
    return error


@when(
    "the user destroys a replica that does not exist",
    target_fixture="the_user_destroys_a_replica_that_does_not_exist",
)
def the_user_destroys_a_replica_that_does_not_exist(
    mayastor_instance, find_replica, replica_name, replica_uuid
):
    assert find_replica(replica_name, replica_uuid) == None
    with pytest.raises(grpc.RpcError) as error:
        mayastor_instance.replica_rpc.DestroyReplica(
            replica_pb.DestroyReplicaRequest(uuid=replica_uuid)
        )
    assert error.value.code() == grpc.StatusCode.NOT_FOUND


@when("the user destroys the replica")
def the_user_destroys_the_replica(mayastor_instance, current_replicas, replica_uuid):
    replica = current_replicas[replica_uuid]
    mayastor_instance.replica_rpc.DestroyReplica(
        replica_pb.DestroyReplicaRequest(uuid=replica.uuid)
    )
    del current_replicas[replica_uuid]


@when(
    "the user lists the replicas",
    target_fixture="list_replicas",
)
def list_replicas(mayastor_instance):
    return mayastor_instance.replica_rpc.ListReplicas(
        replica_pb.ListReplicaOptions()
    ).replicas


@when(
    parsers.parse('the user lists the replica by replica name "{name}"'),
    target_fixture="list_replicas_by_name",
)
def list_the_replica_by_replica_name(mayastor_instance, name):
    opts = replica_pb.ListReplicaOptions()
    opts.name.value = name
    return mayastor_instance.replica_rpc.ListReplicas(opts).replicas


@when(
    parsers.parse('the user lists the replicas by pool name "{pool_name}"'),
    target_fixture="list_replicas_by_pool_name",
)
def list_the_replica_by_pool_name(mayastor_instance, pool_name):
    opts = replica_pb.ListReplicaOptions()
    opts.poolname.value = pool_name
    return mayastor_instance.replica_rpc.ListReplicas(opts).replicas


@when("the user attempts to share the replica with a different protocol")
def attempt_to_share_replica_with_different_protocol(
    mayastor_instance, current_replicas, replica_uuid
):
    replica = current_replicas[replica_uuid]
    share = common_pb.ISCSI if replica.share == common_pb.NVMF else common_pb.NVMF
    with pytest.raises(grpc.RpcError) as error:
        mayastor_instance.replica_rpc.ShareReplica(
            replica_pb.ShareReplicaRequest(uuid=replica.uuid, share=share)
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user shares the replica with the same protocol")
def share_replica_with_the_same_protocol(
    mayastor_instance, current_replicas, replica_uuid
):
    replica = current_replicas[replica_uuid]
    mayastor_instance.replica_rpc.ShareReplica(
        replica_pb.ShareReplicaRequest(uuid=replica.uuid, share=replica.share)
    )


@when('the user attempts to share the replica over "iscsi"')
def attempt_to_share_replica_over_iscsi(mayastor_instance, replica_uuid):
    with pytest.raises(grpc.RpcError) as error:
        mayastor_instance.replica_rpc.ShareReplica(
            replica_pb.ShareReplicaRequest(
                uuid=replica_uuid, share=share_protocol("iscsi")
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when(
    parsers.parse('the user shares the replica over "{share}"'),
    target_fixture="share_replica",
)
def share_replica(mayastor_instance, replica_uuid, share):
    mayastor_instance.replica_rpc.ShareReplica(
        replica_pb.ShareReplicaRequest(uuid=replica_uuid, share=share_protocol(share))
    )


@when("the user unshares the replica")
def unshare_replica(mayastor_instance, replica_uuid):
    mayastor_instance.replica_rpc.UnshareReplica(
        replica_pb.UnshareReplicaRequest(uuid=replica_uuid)
    )


@when("the user reads from the replica")
def read_from_replica():
    raise NotImplementedError


@when("the user writes to the replica")
def write_to_replica():
    raise NotImplementedError


@when(
    parsers.parse(
        'replica snapshot creation request "{snapshot_name}" "{txn_id}" is received by I/O Agent'
    ),
    target_fixture="create_replica_snapshot",
)
def create_replica_snapshot(mayastor_instance, replica_uuid, snapshot_name, txn_id):
    """replica snapshot creation request is received by I/O Agent."""
    mayastor_instance.replica_rpc.CreateReplicaSnapshot(
        replica_pb.CreateReplicaSnapshotRequest(
            replica_uuid=replica_uuid, snapshot_name=snapshot_name, txn_id=txn_id
        )
    )
    pass


@then("replica should not be created")
@then("the replica should be destroyed")
def replica_should_not_be_present(find_replica, replica_name, replica_uuid):
    assert find_replica(replica_name, replica_uuid) == None


@then("the create replica command should return error")
def create_replica_should_return_error(attempt_to_create_a_shared_replica):
    assert (
        attempt_to_create_a_shared_replica.value.code()
        == grpc.StatusCode.INVALID_ARGUMENT
    )


@then("the create replica command should fail")
def create_replica_should_fail():
    pass


@then("replica should be present")
@then("the replica is created")
def create_replica_should_succeed(find_replica, replica_name, replica_uuid):
    assert find_replica(replica_name, replica_uuid) != None


@then(parsers.parse('"{name}" should appear in the output list'))
def replica_should_appear_in_output_list(name, list_replicas):
    assert name in [replica.name for replica in list_replicas]


@then(parsers.parse('"{name}" should appear in the output list based on replica name'))
def replica_should_appear_in_output_list(name, list_replicas_by_name):
    assert name in [replica.name for replica in list_replicas_by_name]


@then(parsers.parse('"{name}" should appear in the output list based on pool name'))
def replica_should_appear_in_output_list(name, list_replicas_by_pool_name):
    assert name in [replica.name for replica in list_replicas_by_pool_name]


@then(
    parsers.parse(
        "only {n:d} replicas should be present in the output list based on replica name"
    )
)
def no_of_replicas_in_output_list(list_replicas_by_name, n):
    assert len(list_replicas_by_name) == n


@then(
    parsers.parse(
        "only {n:d} replicas should be present in the output list based on pool name"
    )
)
def no_of_replicas_in_output_list(list_replicas_by_pool_name, n):
    assert len(list_replicas_by_pool_name) == n


@then("the share replica command should fail")
def share_replica_should_fail():
    pass


@then("the share replica command should succeed")
def share_replica_should_succeed():
    pass


@then('the share state is "nvmf"')
@then('the share state should change to "nvmf"')
def share_state_is_nvmf(find_replica, replica_name, replica_uuid):
    replica = find_replica(replica_name, replica_uuid)
    assert replica != None
    assert replica.share == common_pb.NVMF


@then("the share state should remain in unshared")
@then("the share state should change to unshared")
@then("the share state is unshared")
def share_state_is_unshared(find_replica, replica_name, replica_uuid):
    replica = find_replica(replica_name, replica_uuid)
    assert replica != None
    assert replica.share == common_pb.NONE


@then("the replica destroy command should fail")
def the_replica_destroy_command_should_fail():
    pass


@then("the read operation should succeed")
def read_operation_should_succeed():
    raise NotImplementedError


@then("the write operation should succeed")
def write_operation_should_succeed():
    raise NotImplementedError


@then("new snapshot with requested name shall be successfully created for replica")
def check_replica_snapshot():
    """new snapshot with requested name shall be successfully created for replica."""
    pass


@then("snapshot shall have the same amount of provisioned disk blocks as replica")
def check_snapshot_size():
    """snapshot shall have the same amount of provisioned disk blocks as replica."""
    pass
