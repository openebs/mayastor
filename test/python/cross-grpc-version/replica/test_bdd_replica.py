import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

from common.mayastor import container_mod
from common.mayastor import mayastor_mod as v0_mayastor_mod

# from v1.mayastor import container_mod as v1_container_mod
from v1.mayastor import mayastor_mod as v1_mayastor_mod

import grpc
import replica_pb2 as replica_pb
import common_pb2 as common_pb
import mayastor_pb2 as pb


@scenario(
    "features/replica.feature",
    "creating a v1 version replica with a v0 version name that already exists",
)
def test_creating_a_v1_version_replica_with_a_v0_version_name_that_already_exists():
    """creating a v1 version replica with a v0 version name that already exists."""


@scenario(
    "features/replica.feature",
    "creating a v1 version replica with a v0 version uuid that already exists",
)
def test_creating_a_v1_version_replica_with_a_v0_version_uuid_that_already_exists():
    """creating a v1 version replica with a v0 version uuid that already exists."""


@scenario("features/replica.feature", "listing replicas")
def test_listing_replicas():
    """Listing replicas."""


@scenario(
    "features/replica.feature",
    "listing v0 version replicas by replica name using v1 version gRPC call",
)
def test_listing_v0_version_replicas_by_replica_name_using_v1_version_gRPC_call():
    """listing v0 version replicas by replica name using v1 version gRPC call."""


@scenario(
    "features/replica.feature",
    "listing replicas by pool name on a pool with v0 version replicas",
)
def test_listing_replicas_by_pool_name_on_a_pool_with_v0_version_replicas():
    """listing replicas by pool name on a pool with v0 version replicas."""


@scenario(
    "features/replica.feature",
    "listing replicas by pool name with a non-existent pool name",
)
def test_listing_replicas_by_pool_name_with_a_non_existent_pool():
    """Listing replicas by pool name with a non-existent pool name."""


@scenario(
    "features/replica.feature",
    'sharing a v0 version replica over "nvmf" using v1 gRPC call',
)
def test_sharing_a_v0_version_replica_over_nvmf_using_v1_gRPC_call():
    """sharing a v0 version replica over "nvmf" using v1 gRPC call."""


@scenario(
    "features/replica.feature",
    'sharing a v0 version replica over "iscsi" using v1 gRPC call',
)
def test_fail_sharing_a_v0_version_replica_over_iscsi_using_v1_gRPC_call():
    """sharing a v0 version replica over "iscsi" using v1 gRPC call."""


@scenario(
    "features/replica.feature",
    "sharing a v0 version replica that is already shared with the same protocol using v1 gRPC call",
)
def test_sharing_a_v0_version_replica_that_is_already_shared_with_the_same_protocol_using_v1_gRPC_call():
    """sharing a v0 version replica that is already shared with the same protocol using v1 gRPC call."""


@scenario(
    "features/replica.feature",
    "sharing a v0 version replica that is already shared with a different protocol using v1 gRPC call",
)
def test_fail_sharing_a_v0_version_replica_that_is_already_shared_with_a_different_protocol_using_v1_gRPC_call():
    """sharing a v0 version replica that is already shared with a different protocol using v1 gRPC call."""


@scenario(
    "features/replica.feature", "unsharing a v0 version replica using v1 gRPC call"
)
def test_unsharing_a_v0_version_replica_using_v1_gRPC_call():
    """unsharing a v0 version replica using v1 gRPC call."""


@scenario(
    "features/replica.feature",
    "unsharing a v0 version replica that is not shared using v1 gRPC call",
)
def test_unsharing_a_v0_version_replica_that_is_not_shared_using_v1_gRPC_call():
    """unsharing a v0 version replica that is not shared using v1 gRPC call."""


@scenario(
    "features/replica.feature", "destroying a v0 version replica using v1 gRPC call"
)
def test_destroying_a_v0_version_replica_using_v1_gRPC_call():
    """destroying a v0 version replica using v1 gRPC call."""


@pytest.fixture
def replica_name():
    yield "replica-1"


@pytest.fixture
def replica_uuid():
    yield "22ca10d3-4f2b-4b95-9814-9181c025cc1a"


@pytest.fixture(scope="module")
def replica_size():
    yield 32 * 1024 * 1024


def share_protocol(name):
    PROTOCOLS = {
        "none": common_pb.NONE,
        "nvmf": common_pb.NVMF,
        "iscsi": common_pb.ISCSI,
    }
    return PROTOCOLS[name]


@pytest.fixture(scope="module")
def v0_mayastor_instance(v0_mayastor_mod):
    yield v0_mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def v1_mayastor_instance(v1_mayastor_mod):
    yield v1_mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def mayastor_pool(v0_mayastor_instance):
    pool = v0_mayastor_instance.ms.CreatePool(
        pb.CreatePoolRequest(name="p0", disks=["malloc:///disk0?size_mb=512"])
    )
    yield pool.name
    v0_mayastor_instance.ms.DestroyPool(pb.DestroyPoolRequest(name=pool.name))


@pytest.fixture
def current_replicas(v0_mayastor_instance, mayastor_pool):
    replicas = {}
    yield replicas
    for uuid in replicas.keys():
        v0_mayastor_instance.ms.DestroyReplica(pb.DestroyReplicaRequest(uuid=uuid))


@pytest.fixture
def create_v0_replica(v0_mayastor_instance, mayastor_pool, current_replicas):
    def create(name, uuid, size, share):
        replica = v0_mayastor_instance.ms.CreateReplicaV2(
            pb.CreateReplicaRequestV2(
                pool=mayastor_pool, name=name, uuid=uuid, size=size, share=share
            )
        )
        current_replicas[uuid] = replica

    yield create


@pytest.fixture
def create_v1_replica(v1_mayastor_instance, mayastor_pool, current_replicas):
    def create(name, uuid, size, share):
        replica = v1_mayastor_instance.replica_rpc.CreateReplica(
            replica_pb.CreateReplicaRequest(
                pooluuid=mayastor_pool,
                name=name,
                uuid=uuid,
                size=size,
                share=share,
            )
        )
        current_replicas[uuid] = replica

    yield create


@pytest.fixture
def find_replica(v1_mayastor_instance, mayastor_pool):
    def find(name, uuid):
        for replica in v1_mayastor_instance.replica_rpc.ListReplicas(
            replica_pb.ListReplicaOptions()
        ).replicas:
            if replica.name == name and replica.uuid == uuid:
                return replica
        return None

    yield find


@given(parsers.parse('a mayastor instance "{name}"'))
def get_instance():
    pass


@given(parsers.parse('a pool "{name}"'))
def get_pool(mayastor_pool):
    pass


@given(
    parsers.parse('a v0 version replica "{name}" with uuid "{uuid}"'),
    target_fixture="get_replica",
)
def get_replica(create_v0_replica, name, uuid, replica_size):
    create_v0_replica(name, uuid, replica_size, pb.REPLICA_NONE)


@given("a replica that is unshared")
def get_unshared_replica(create_v0_replica, replica_name, replica_uuid, replica_size):
    create_v0_replica(
        replica_name, replica_uuid, replica_size, share=share_protocol("none")
    )


@given(parsers.parse('a replica shared over "{share}"'))
def get_shared_replica(
    create_v0_replica, replica_name, replica_uuid, replica_size, share
):
    create_v0_replica(
        replica_name, replica_uuid, replica_size, share=share_protocol(share)
    )


@when("the user creates a v1 version replica with a name that already exists")
def create_v1_replica_with_existing_name(
    create_v1_replica, current_replicas, replica_uuid
):
    replica = current_replicas[replica_uuid]
    with pytest.raises(grpc.RpcError) as error:
        create_v1_replica(replica.name, replica.uuid, replica.size, share=replica.share)
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS


@when(
    "the user creates a v1 version replica with a uuid that already exists",
    target_fixture="create_v1_replica_with_existing_uuid",
)
def create_v1_replica_with_existing_uuid(create_v1_replica, replica_uuid, replica_size):
    with pytest.raises(grpc.RpcError) as error:
        create_v1_replica(
            "replica-2", replica_uuid, replica_size, share=share_protocol("none")
        )
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS


@when(
    "the user lists the replicas with v1 gRPC call",
    target_fixture="list_replicas",
)
def list_replicas(v1_mayastor_instance):
    return v1_mayastor_instance.replica_rpc.ListReplicas(
        replica_pb.ListReplicaOptions()
    ).replicas


@when(
    parsers.parse('the user lists the replica by replica name "{name}"'),
    target_fixture="list_replicas_by_name",
)
def list_replicas_by_name(v1_mayastor_instance, name):
    opts = replica_pb.ListReplicaOptions()
    opts.name.value = name
    return v1_mayastor_instance.replica_rpc.ListReplicas(opts).replicas


@when(
    parsers.parse('the user lists the replicas by pool name "{pool_name}"'),
    target_fixture="list_replicas_by_pool_name",
)
def list_replicas_by_pool_name(v1_mayastor_instance, pool_name):
    opts = replica_pb.ListReplicaOptions()
    opts.poolname.value = pool_name
    return v1_mayastor_instance.replica_rpc.ListReplicas(opts).replicas


@when(
    parsers.parse('the user shares the replica over "{share}"'),
    target_fixture="share_replica",
)
def share_replica(v1_mayastor_instance, replica_uuid, share):
    v1_mayastor_instance.replica_rpc.ShareReplica(
        replica_pb.ShareReplicaRequest(uuid=replica_uuid, share=share_protocol(share))
    )


@when('the user attempts to share the replica over "iscsi"')
def attempt_to_share_replica_over_iscsi(v1_mayastor_instance, replica_uuid):
    with pytest.raises(grpc.RpcError) as error:
        v1_mayastor_instance.replica_rpc.ShareReplica(
            replica_pb.ShareReplicaRequest(
                uuid=replica_uuid, share=share_protocol("iscsi")
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user shares the replica with the same protocol")
def share_replica_with_the_same_protocol(
    v1_mayastor_instance, current_replicas, replica_uuid
):
    replica = current_replicas[replica_uuid]
    v1_mayastor_instance.replica_rpc.ShareReplica(
        replica_pb.ShareReplicaRequest(uuid=replica.uuid, share=replica.share)
    )


@when("the user attempts to share the replica with a different protocol")
def attempt_to_share_replica_with_different_protocol(
    v1_mayastor_instance, current_replicas, replica_uuid
):
    replica = current_replicas[replica_uuid]
    share = common_pb.ISCSI if replica.share == common_pb.NVMF else common_pb.NVMF
    with pytest.raises(grpc.RpcError) as error:
        v1_mayastor_instance.replica_rpc.ShareReplica(
            replica_pb.ShareReplicaRequest(uuid=replica.uuid, share=share)
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("the user unshares the replica")
def unshare_replica(v1_mayastor_instance, replica_uuid):
    v1_mayastor_instance.replica_rpc.UnshareReplica(
        replica_pb.UnshareReplicaRequest(uuid=replica_uuid)
    )


@when("the user destroys the replica")
def the_user_destroys_the_replica(v1_mayastor_instance, current_replicas, replica_uuid):
    replica = current_replicas[replica_uuid]
    v1_mayastor_instance.replica_rpc.DestroyReplica(
        replica_pb.DestroyReplicaRequest(uuid=replica.uuid)
    )
    del current_replicas[replica_uuid]


@then("the create replica command should fail")
def create_replica_should_fail():
    pass


@then("replica should be present")
@then("the replica is created")
def create_replica_should_succeed(find_replica, replica_name, replica_uuid):
    assert find_replica(replica_name, replica_uuid) != None


@then(parsers.parse('"{name}" should appear in the output list'))
def replica_should_appear_in_output(name, list_replicas):
    assert name in [replica.name for replica in list_replicas]


@then(parsers.parse('"{name}" should appear in the output list based on replica name'))
def replica_should_appear_in_output_list(name, list_replicas_by_name):
    assert name in [replica.name for replica in list_replicas_by_name]


@then(
    parsers.parse(
        "only {n:d} replicas should be present in the output list based on replica name"
    )
)
def no_of_replicas_in_output_list(list_replicas_by_name, n):
    assert len(list_replicas_by_name) == n


@then(parsers.parse('"{name}" should appear in the output list based on pool name'))
def replica_should_appear_in_output_list(name, list_replicas_by_pool_name):
    assert name in [replica.name for replica in list_replicas_by_pool_name]


@then(
    parsers.parse(
        "only {n:d} replicas should be present in the output list based on pool name"
    )
)
def no_of_replicas_in_output_list(list_replicas_by_pool_name, n):
    assert len(list_replicas_by_pool_name) == n


@then('the share state is "nvmf"')
@then('the share state should change to "nvmf"')
def share_state_is_nvmf(find_replica, replica_name, replica_uuid):
    replica = find_replica(replica_name, replica_uuid)
    assert replica != None
    assert replica.share == common_pb.NVMF


@then("the share replica command should fail")
def share_replica_should_fail():
    pass


@then("the share replica command should succeed")
def share_replica_should_succeed():
    pass


@then("the share state should remain in unshared")
@then("the share state should change to unshared")
def share_state_is_unshared(find_replica, replica_name, replica_uuid):
    replica = find_replica(replica_name, replica_uuid)
    assert replica != None
    assert replica.share == common_pb.NONE


@then("the replica should be destroyed")
def replica_should_not_be_present(find_replica, replica_name, replica_uuid):
    assert find_replica(replica_name, replica_uuid) == None
