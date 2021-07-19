import pytest
from pytest_bdd import given, scenario, then, when, parsers

from common.mayastor import mayastor_mod

import grpc
import mayastor_pb2 as pb


@scenario("features/replica.feature", "creating a replica")
def test_creating_a_replica():
    "Creating a replica."


@scenario("features/replica.feature", 'creating a replica shared over "iscsi"')
def test_fail_creating_a_replica_shared_over_iscsi():
    'Creating a replica shared over "iscsi".'


@scenario("features/replica.feature", 'creating a replica shared over "nvmf"')
def test_creating_a_replica_shared_over_nvmf():
    'Creating a replica shared over "nvmf".'


@scenario(
    "features/replica.feature", "creating a replica with a name that already exists"
)
def test_creating_a_replica_with_a_name_that_already_exists():
    "Creating a replica with a name that already exists."


@scenario("features/replica.feature", "destroying a replica")
def test_destroying_a_replica():
    "Destroying a replica."


@scenario("features/replica.feature", "destroying a replica that does not exist")
def test_destroying_a_replica_that_does_not_exist():
    "Destroying a replica that does not exist."


@scenario("features/replica.feature", "listing replica stats")
def test_listing_replica_stats():
    "Listing replica stats."


@scenario("features/replica.feature", "listing replicas")
def test_listing_replicas():
    "Listing replicas."


@scenario("features/replica.feature", 'sharing a replica over "iscsi"')
def test_fail_sharing_a_replica_over_iscsi():
    'Sharing a replica over "iscsi".'


@scenario("features/replica.feature", 'sharing a replica over "nvmf"')
def test_sharing_a_replica_over_nvmf():
    'Sharing a replica over "nvmf".'


@scenario(
    "features/replica.feature",
    "sharing a replica that is already shared with a different protocol",
)
def test_fail_sharing_a_replica_that_is_already_shared_with_a_different_protocol():
    "Sharing a replica that is already shared with a different protocol."


@scenario(
    "features/replica.feature",
    "sharing a replica that is already shared with the same protocol",
)
def test_sharing_a_replica_that_is_already_shared_with_the_same_protocol():
    "Sharing a replica that is already shared with the same protocol."


@scenario("features/replica.feature", "unsharing a replica")
def test_unsharing_a_replica():
    "Unsharing a replica."


@pytest.mark.skip(reason="todo")
@scenario("features/replica.feature", "reading from a shared replica")
def test_reading_from_a_shared_replica():
    "Reading from a shared replica."


@pytest.mark.skip(reason="todo")
@scenario("features/replica.feature", "writing to a shared replica")
def test_writing_to_a_shared_replica():
    "Writing to a shared replica."


def share_protocol(name):
    PROTOCOLS = {
        "none": pb.REPLICA_NONE,
        "nvmf": pb.REPLICA_NVMF,
        "iscsi": pb.REPLICA_ISCSI,
    }
    return PROTOCOLS[name]


@pytest.fixture(scope="module")
def mayastor_instance(mayastor_mod):
    yield mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def mayastor_pool(mayastor_instance):
    pool = mayastor_instance.ms.CreatePool(
        pb.CreatePoolRequest(name="p0", disks=["malloc:///disk0?size_mb=512"])
    )
    yield pool.name
    mayastor_instance.ms.DestroyPool(pb.DestroyPoolRequest(name=pool.name))


@pytest.fixture(scope="module")
def replica_uuid():
    yield "22ca10d3-4f2b-4b95-9814-9181c025cc1a"


@pytest.fixture(scope="module")
def replica_size():
    yield 32 * 1024 * 1024


@pytest.fixture(scope="module")
def find_replica(mayastor_instance, mayastor_pool):
    def find(uuid):
        for replica in mayastor_instance.ms.ListReplicas(pb.Null()).replicas:
            if replica.uuid == uuid:
                return replica
        return None

    yield find


@pytest.fixture
def current_replicas(mayastor_instance, mayastor_pool):
    replicas = {}
    yield replicas
    for uuid in replicas.keys():
        mayastor_instance.ms.DestroyReplica(pb.DestroyReplicaRequest(uuid=uuid))


@pytest.fixture
def create_replica(mayastor_instance, mayastor_pool, current_replicas):
    def create(uuid, size, share):
        replica = mayastor_instance.ms.CreateReplica(
            pb.CreateReplicaRequest(
                pool=mayastor_pool, uuid=uuid, size=size, share=share
            )
        )
        current_replicas[uuid] = replica

    yield create


@given(parsers.parse('a mayastor instance "{name}"'))
def get_instance(mayastor_instance):
    pass


@given(parsers.parse('a pool "{name}"'))
def get_pool(mayastor_pool):
    pass


@given("a replica")
@given("a replica that is unshared")
def get_replica(create_replica, replica_uuid, replica_size):
    create_replica(replica_uuid, replica_size, pb.REPLICA_NONE)


@given(parsers.parse('a replica shared over "{share}"'), target_fixture="share_replica")
def get_shared_replica(create_replica, replica_uuid, replica_size, share):
    create_replica(replica_uuid, replica_size, share=share_protocol(share))


@when("the user creates a replica")
@when("the user creates an unshared replica")
def create_unshared_replica(create_replica, replica_uuid, replica_size):
    create_replica(replica_uuid, replica_size, pb.REPLICA_NONE)


@when('the user attempts to create a replica shared over "iscsi"')
def attempt_to_create_replica_shared_over_iscsi(
    create_replica, replica_uuid, replica_size
):
    with pytest.raises(grpc.RpcError) as error:
        create_replica(replica_uuid, replica_size, pb.REPLICA_ISCSI)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when(
    parsers.parse('the user creates a replica shared over "{share}"'),
    target_fixture="share_replica",
)
def create_shared_replica(create_replica, replica_uuid, replica_size, share):
    create_replica(replica_uuid, replica_size, share=share_protocol(share))


@when("the user creates a replica that already exists")
@when("the user recreates the existing replica")
def create_replica_that_already_exists(create_replica, current_replicas, replica_uuid):
    replica = current_replicas[replica_uuid]
    create_replica(replica.uuid, replica.size, share=replica.share)


@when("the user destroys a replica that does not exist")
def the_user_destroys_a_replica_that_does_not_exist(
    mayastor_instance, find_replica, replica_uuid
):
    assert find_replica(replica_uuid) == None
    mayastor_instance.ms.DestroyReplica(pb.DestroyReplicaRequest(uuid=replica_uuid))


@when("the user destroys the replica")
def the_user_destroys_the_replica(mayastor_instance, current_replicas, replica_uuid):
    replica = current_replicas[replica_uuid]
    mayastor_instance.ms.DestroyReplica(pb.DestroyReplicaRequest(uuid=replica.uuid))
    del current_replicas[replica_uuid]


@when("the user gets replica stats", target_fixture="stat_replicas")
def stat_replicas(mayastor_instance):
    return mayastor_instance.ms.StatReplicas(pb.Null()).replicas


@when("the user lists the current replicas", target_fixture="list_replicas")
def list_replicas(mayastor_instance):
    return mayastor_instance.ms.ListReplicas(pb.Null()).replicas


@when("the user attempts to share the replica with a different protocol")
def attempt_to_share_replica_with_different_protocol(
    mayastor_instance, current_replicas, replica_uuid
):
    replica = current_replicas[replica_uuid]
    share = pb.REPLICA_ISCSI if replica.share == pb.REPLICA_NVMF else pb.REPLICA_NVMF
    with pytest.raises(grpc.RpcError) as error:
        mayastor_instance.ms.ShareReplica(
            pb.ShareReplicaRequest(uuid=replica.uuid, share=share)
        )
    assert error.value.code() == grpc.StatusCode.INTERNAL


@when("the user shares the replica with the same protocol")
def share_replica_with_the_same_protocol(
    mayastor_instance, current_replicas, replica_uuid
):
    replica = current_replicas[replica_uuid]
    mayastor_instance.ms.ShareReplica(
        pb.ShareReplicaRequest(uuid=replica.uuid, share=replica.share)
    )


@when('the user attempts to share the replica over "iscsi"')
def attempt_to_share_replica_over_iscsi(mayastor_instance, replica_uuid):
    with pytest.raises(grpc.RpcError) as error:
        mayastor_instance.ms.ShareReplica(
            pb.ShareReplicaRequest(uuid=replica_uuid, share=pb.REPLICA_ISCSI)
        )
    assert error.value.code() == grpc.StatusCode.INTERNAL


@when(
    parsers.parse('the user shares the replica over "{share}"'),
    target_fixture="share_replica",
)
def share_replica(mayastor_instance, replica_uuid, share):
    mayastor_instance.ms.ShareReplica(
        pb.ShareReplicaRequest(uuid=replica_uuid, share=share_protocol(share))
    )


@when("the user unshares the replica")
def unshare_replica(mayastor_instance, replica_uuid):
    mayastor_instance.ms.ShareReplica(
        pb.ShareReplicaRequest(uuid=replica_uuid, share=pb.REPLICA_NONE)
    )


@when("the user reads from the replica")
def read_from_replica():
    raise NotImplementedError


@when("the user writes to the replica")
def write_to_replica():
    raise NotImplementedError


@then("the create replica command should fail")
def create_replica_should_fail(find_replica, replica_uuid):
    assert find_replica(replica_uuid) == None


@then("the create replica command should succeed")
@then("the replica is created")
def create_replica_should_succeed(find_replica, replica_uuid):
    assert find_replica(replica_uuid) != None


@then("the replica destroy command should succeed")
@then("the replica should be destroyed")
def destroy_replica_should_succeed(find_replica, replica_uuid):
    assert find_replica(replica_uuid) == None


@then("the replica should appear in the output list")
def replica_should_appear_in_output(replica_uuid, list_replicas):
    assert replica_uuid in [replica.uuid for replica in list_replicas]


@then("the share replica command should fail")
def share_replica_should_fail():
    pass


@then("the share replica command should succeed")
def share_replica_should_succeed():
    pass


@then('the share state is "nvmf"')
@then('the share state should change to "nvmf"')
def share_state_is_nvmf(find_replica, replica_uuid):
    replica = find_replica(replica_uuid)
    assert replica != None
    assert replica.share == pb.REPLICA_NVMF


@then("the share state is unshared")
@then("the share state should change to unshared")
def share_state_is_unshared(find_replica, replica_uuid):
    replica = find_replica(replica_uuid)
    assert replica != None
    assert replica.share == pb.REPLICA_NONE


@then("the stats for the replica should be listed")
def stats_for_replica_should_be_listed(replica_uuid, stat_replicas):
    assert replica_uuid in [stats.uuid for stats in stat_replicas]


@then("the read operation should succeed")
def read_operation_should_succeed():
    raise NotImplementedError


@then("the write operation should succeed")
def write_operation_should_succeed():
    raise NotImplementedError
