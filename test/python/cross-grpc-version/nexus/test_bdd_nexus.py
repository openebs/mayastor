import pytest
from pytest_bdd import given, scenario, then, when, parsers

from collections import namedtuple
import subprocess

from common.mayastor import container_mod, mayastor_mod
from v1.mayastor import container_mod as v1_container_mod
from v1.mayastor import mayastor_mod as v1_mayastor_mod
from common.volume import Volume as v0_volume
from v1.volume import Volume as v1_volume

import grpc
import mayastor_pb2 as pb
import common_pb2 as common_pb
import nexus_pb2 as nexus_pb

BaseBdev = namedtuple("BaseBdev", "name uri")

LocalFile = namedtuple("LocalFile", "path uri")


@scenario("features/nexus.feature", "creating the same nexus with v1 gRPC call")
def test_creating_the_same_nexus_with_v1_gRPC_call():
    """creating the same nexus with v1 gRPC call."""


@scenario("features/nexus.feature", "destroying a v0 version nexus with v1 gRPC call")
def test_destroying_a_v0_version_nexus_with_v1_gRPC_call():
    """destroying a v0 version nexus with v1 gRPC call."""


@scenario(
    "features/nexus.feature", "destroying a v0 version nexus without first unpublishing"
)
def test_destroying_a_v0_version_nexus_without_first_unpublishing():
    """destroying a v0 version nexus without first unpublishing."""


@scenario("features/nexus.feature", "destroying a v0 version nexus that does not exist")
def test_destroying_a_v0_version_nexus_that_does_not_exist():
    """destroying a v0 version nexus that does not exist."""


@scenario(
    "features/nexus.feature",
    "listing nexuses created with v0 version using v1 gRPC call",
)
def test_listing_nexuses_created_with_v0_version_using_v1_gRPC_call():
    """listing nexuses created with v0 version using v1 gRPC call."""


@scenario("features/nexus.feature", "listing v0 nexus by name which does not exists")
def test_listing_v0_nexus_by_name_which_does_not_exists():
    """listing v0 nexus by name which does not exists."""


@scenario(
    "features/nexus.feature",
    "removing a child from a v0 version nexus using v1 gRPC call",
)
def test_removing_a_child_from_a_v0_version_nexus_using_v1_gRPC_call():
    """removing a child from a v0 version nexus using v1 gRPC call."""


@scenario(
    "features/nexus.feature", "adding a child to a v0 version nexus using v1 gRPC call"
)
def test_adding_a_child_to_a_v0_version_nexus_using_v1_gRPC_call():
    """adding a child to a v0 version nexus using v1 gRPC call."""


@scenario("features/nexus.feature", "publishing a v0 version nexus using v1 gRPC call")
def test_publishing_a_v0_version_nexus_using_v1_gRPC_call():
    """publishing a v0 version nexus using v1 RPC call."""


@scenario(
    "features/nexus.feature", "unpublishing a v0 version nexus using v1 gRPC call"
)
def test_unpublishing_a_v0_version_nexus_using_v1_gRPC_call():
    """unpublishing a v0 version nexus using v1 gRPC call."""


@scenario(
    "features/nexus.feature",
    "unpublishing a v0 version nexus using v1 gRPC call that is not published",
)
def test_unpublishing_a_v0_version_nexus_using_v1_gRPC_call_that_is_not_published():
    """unpublishing a v0 version nexus using v1 gRPC call that is not published."""


@scenario(
    "features/nexus.feature",
    "republishing a v0 version nexus with the same protocol using v1 gRPC call",
)
def test_republishing_a_v0_version_nexus_with_the_same_protocol_using_v1_gRPC_call():
    """republishing a v0 version nexus with the same protocol using v1 gRPC call."""


@scenario(
    "features/nexus.feature",
    "republishing a v0 version nexus with a different protocol using v1 gRPC call",
)
def test_fail_republishing_a_v0_version_nexus_with_a_different_protocol_using_v1_gRPC_call():
    """republishing a v0 version nexus with a different protocol using v1 gRPC call."""


@scenario(
    "features/nexus.feature",
    "publishing a v0 version nexus with a crypto-key using v1 gRPC call",
)
def test_publishing_a_v0_version_nexus_with_a_cryptokey_using_v1_gRPC_call():
    """publishing a v0 version nexus with a crypto-key using v1 gRPC call."""


@pytest.fixture(scope="module")
def nexus_instance():
    yield "ms1"


@pytest.fixture(scope="module")
def remote_instance():
    yield "ms0"


@pytest.fixture(scope="module")
def base_instances(remote_instance, nexus_instance):
    yield [remote_instance, nexus_instance]


@pytest.fixture(scope="module")
def nexus_uuid():
    yield "86050f0e-6914-4e9c-92b9-1237fd6d17a6"


def megabytes(n):
    return n * 1024 * 1024


def min_cntl_id():
    return 1


def max_cntl_id():
    return 10


def resv_key():
    return 123


def preempt_key():
    return 0


def nexus_info_key():
    return "0"


def share_type(protocol):
    TYPES = {
        "nbd": pb.ShareProtocolNexus.NEXUS_NBD,
        "nvmf": pb.ShareProtocolNexus.NEXUS_NVMF,
        "iscsi": pb.ShareProtocolNexus.NEXUS_ISCSI,
    }
    return TYPES[protocol]


def share_protocol(name):
    PROTOCOLS = {
        "none": common_pb.NONE,
        "nvmf": common_pb.NVMF,
        "iscsi": common_pb.ISCSI,
    }
    return PROTOCOLS[name]


def get_child_uris(nexus):
    return [child.uri for child in nexus.children]


@pytest.fixture(scope="module")
def base_bdevs(mayastor_mod, base_instances):
    devices = {}
    for instance in base_instances:
        uri = "malloc:///malloc0?size_mb=64&blk_size=4096"
        name = mayastor_mod[instance].bdev.Create(pb.BdevUri(uri=uri)).name
        devices[instance] = BaseBdev(name, uri)
    yield devices
    for instance, bdev in devices.items():
        mayastor_mod[instance].bdev.Destroy(pb.BdevUri(uri=bdev.uri))


@pytest.fixture(scope="module")
def shared_remote_bdev_uri(mayastor_mod, base_bdevs, remote_instance):
    name = base_bdevs[remote_instance].name
    uri = (
        mayastor_mod[remote_instance]
        .bdev.Share(pb.BdevShareRequest(name=name, proto="nvmf"))
        .uri
    )
    yield uri
    mayastor_mod[remote_instance].bdev.Unshare(pb.BdevShareRequest(name=name))


@pytest.fixture(scope="module")
def file_types():
    yield ["aio", "uring"]


@pytest.fixture(scope="module")
def local_files(file_types):
    files = {}
    for type in file_types:
        path = f"/tmp/{type}-file.img"
        uri = f"{type}://{path}?blk_size=4096"
        subprocess.run(
            ["sudo", "sh", "-c", f"rm -f '{path}' && truncate -s 64M '{path}'"],
            check=True,
        )
        files[type] = LocalFile(path, uri)
    yield files
    for path in [file.path for file in files.values()]:
        subprocess.run(["sudo", "sh", "-c", f"rm -f '{path}'"], check=True)


@pytest.fixture(scope="module")
def local_bdev_uri():
    yield "bdev:///malloc0"


@pytest.fixture
def nexus_children(local_bdev_uri, shared_remote_bdev_uri, local_files):
    return [local_bdev_uri, shared_remote_bdev_uri] + [
        file.uri for file in local_files.values()
    ]


@pytest.fixture
def created_nexuses(mayastor_mod, nexus_instance):
    nexuses = {}
    yield nexuses
    for uuid in nexuses.keys():
        mayastor_mod[nexus_instance].ms.DestroyNexus(pb.DestroyNexusRequest(uuid=uuid))


@pytest.fixture
def create_v0_nexus(mayastor_mod, nexus_instance, created_nexuses):
    def create(uuid, size, children):
        nexus = mayastor_mod[nexus_instance].ms.CreateNexus(
            pb.CreateNexusRequest(uuid=uuid, size=size, children=children)
        )
        created_nexuses[uuid] = nexus
        return nexus

    yield create


@pytest.fixture
def create_v1_nexus(v1_mayastor_mod, nexus_instance, created_nexuses):
    def create(
        name,
        uuid,
        size,
        min_cntl_id,
        max_cntl_id,
        resv_key,
        preempt_key,
        children,
        nexus_info_key,
    ):
        nexus = v1_mayastor_mod[nexus_instance].nexus_rpc.CreateNexus(
            nexus_pb.CreateNexusRequest(
                name=name,
                uuid=uuid,
                size=size,
                minCntlId=min_cntl_id,
                maxCntlId=max_cntl_id,
                resvKey=resv_key,
                preemptKey=preempt_key,
                nexusInfoKey=nexus_info_key,
                children=children,
            )
        )
        created_nexuses[uuid] = nexus
        return nexus

    yield create


@pytest.fixture(scope="module")
def find_nexus(mayastor_mod, nexus_instance):
    def find(uuid):
        for nexus in mayastor_mod[nexus_instance].ms.ListNexus(pb.Null()).nexus_list:
            if nexus.uuid == uuid:
                return nexus
        return None

    yield find


@given("a local mayastor instance")
def local_mayastor_instance(nexus_instance):
    pass


@given("a remote mayastor instance")
def remote_mayastor_instance(remote_instance):
    pass


@given("a v0 version nexus")
@given("an unpublished v0 version nexus")
def get_nexus(create_v0_nexus, nexus_uuid, nexus_children):
    create_v0_nexus(nexus_uuid, megabytes(64), nexus_children)


@given(
    parsers.parse('a v0 version nexus "{uuid}"'),
    target_fixture="get_nexus_with_given_uuid",
)
def get_nexus_with_given_uuid(create_v0_nexus, uuid, nexus_children):
    create_v0_nexus(uuid, megabytes(64), nexus_children)


@given(
    parsers.parse('a nexus published via "{protocol}"'),
    target_fixture="get_published_nexus",
)
def get_published_nexus(
    create_v0_nexus,
    mayastor_mod,
    nexus_instance,
    nexus_uuid,
    nexus_children,
    find_nexus,
    protocol,
):
    create_v0_nexus(nexus_uuid, megabytes(64), nexus_children)
    mayastor_mod[nexus_instance].ms.PublishNexus(
        pb.PublishNexusRequest(uuid=nexus_uuid, key="", share=share_type(protocol))
    )
    nexus = find_nexus(nexus_uuid)
    assert nexus.device_uri


@given(
    parsers.parse('a v0 version nexus published via "{protocol}"'),
    target_fixture="get_published_nexus",
)
def get_published_nexus(
    create_v0_nexus,
    mayastor_mod,
    nexus_instance,
    nexus_uuid,
    nexus_children,
    find_nexus,
    protocol,
):
    create_v0_nexus(nexus_uuid, megabytes(64), nexus_children)
    mayastor_mod[nexus_instance].ms.PublishNexus(
        pb.PublishNexusRequest(uuid=nexus_uuid, key="", share=share_type(protocol))
    )
    nexus = find_nexus(nexus_uuid)
    assert nexus.device_uri


@when("creating an identical v1 version nexus")
def creating_a_nexus(create_v1_nexus, nexus_uuid, nexus_children):
    with pytest.raises(grpc.RpcError) as error:
        create_v1_nexus(
            nexus_uuid,
            nexus_uuid,
            megabytes(64),
            min_cntl_id(),
            max_cntl_id(),
            resv_key(),
            preempt_key(),
            nexus_children,
            nexus_info_key(),
        )
    assert error.value.code() == grpc.StatusCode.INTERNAL


@when("destroying the nexus")
@when("destroying the nexus without unpublishing it")
def destroying_the_nexus(v1_mayastor_mod, nexus_instance, nexus_uuid, created_nexuses):
    v1_mayastor_mod[nexus_instance].nexus_rpc.DestroyNexus(
        nexus_pb.DestroyNexusRequest(uuid=nexus_uuid)
    )
    del created_nexuses[nexus_uuid]


@when("destroying a nexus that does not exist")
def destroying_a_nexus_that_does_not_exist(v1_mayastor_mod, nexus_instance):
    with pytest.raises(grpc.RpcError) as error:
        v1_mayastor_mod[nexus_instance].nexus_rpc.DestroyNexus(
            nexus_pb.DestroyNexusRequest(uuid="e6629036-1376-494d-bbc2-0b6345ab10df")
        )
    assert error.value.code() == grpc.StatusCode.NOT_FOUND


@when("listing all nexuses", target_fixture="list_nexuses")
def list_nexuses(v1_mayastor_mod, nexus_instance):
    return (
        v1_mayastor_mod[nexus_instance]
        .nexus_rpc.ListNexus(nexus_pb.ListNexusOptions())
        .nexus_list
    )


@when(
    parsers.parse('the user lists the nexus with name filter as "{name}"'),
    target_fixture="list_non_existing_nexus",
)
def list_non_existing_nexus(v1_mayastor_mod, nexus_instance, name):
    opts = nexus_pb.ListNexusOptions()
    opts.name.value = name
    return (
        v1_mayastor_mod[nexus_instance]
        .nexus_rpc.ListNexus(opts, wait_for_ready=True)
        .nexus_list
    )


@when("removing a child")
def remove_child(v1_mayastor_mod, nexus_instance, nexus_uuid, nexus_children):
    v1_mayastor_mod[nexus_instance].nexus_rpc.RemoveChildNexus(
        nexus_pb.RemoveChildNexusRequest(uuid=nexus_uuid, uri=nexus_children[0])
    )


@when("adding the removed child")
def add_child(v1_mayastor_mod, nexus_instance, nexus_uuid, nexus_children):
    nexus = (
        v1_mayastor_mod[nexus_instance]
        .nexus_rpc.AddChildNexus(
            nexus_pb.AddChildNexusRequest(uuid=nexus_uuid, uri=nexus_children[0])
        )
        .nexus
    )
    assert (
        nexus.children[len(nexus.children) - 1].state
        == nexus_pb.ChildState.CHILD_STATE_DEGRADED
    )


@when(
    parsers.parse('publishing a nexus via "{protocol}"'), target_fixture="publish_nexus"
)
def publish_nexus(v1_mayastor_mod, nexus_instance, nexus_uuid, protocol):
    v1_mayastor_mod[nexus_instance].nexus_rpc.PublishNexus(
        nexus_pb.PublishNexusRequest(
            uuid=nexus_uuid, key="", share=share_type(protocol)
        )
    )


@when("unpublishing the nexus")
def unpublish_nexus(v1_mayastor_mod, nexus_instance, nexus_uuid):
    v1_mayastor_mod[nexus_instance].nexus_rpc.UnpublishNexus(
        nexus_pb.UnpublishNexusRequest(uuid=nexus_uuid)
    )


@when("publishing the nexus with the same protocol")
def publishing_the_nexus_with_the_same_protocol(
    find_nexus, v1_mayastor_mod, nexus_instance, nexus_uuid
):
    nexus = find_nexus(nexus_uuid)
    v1_mayastor_mod[nexus_instance].nexus_rpc.PublishNexus(
        nexus_pb.PublishNexusRequest(uuid=nexus_uuid, key="", share=common_pb.NVMF)
    )


@when("attempting to publish the nexus with a different protocol")
def attempt_to_publish_nexus_with_different_protocol(
    find_nexus, v1_mayastor_mod, nexus_instance, nexus_uuid
):
    nexus = find_nexus(nexus_uuid)
    with pytest.raises(grpc.RpcError) as error:
        v1_mayastor_mod[nexus_instance].nexus_rpc.PublishNexus(
            nexus_pb.PublishNexusRequest(uuid=nexus_uuid, key="", share=common_pb.NONE)
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("publishing the nexus using a crypto-key")
def publish_nexus_with_cryptokey(v1_mayastor_mod, nexus_instance, nexus_uuid):
    v1_mayastor_mod[nexus_instance].nexus_rpc.PublishNexus(
        nexus_pb.PublishNexusRequest(
            uuid=nexus_uuid, key="0123456789123456", share=share_protocol("nvmf")
        )
    )


@then("the operation should fail")
@then("the request should fail")
def operation_should_fail():
    pass


@then("the nexus should be destroyed")
def nexus_should_be_destroyed(find_nexus, nexus_uuid):
    assert find_nexus(nexus_uuid) == None


@then("the operation should succeed")
@then("the request should succeed")
def operation_should_succeed():
    pass


@then("the nexus should appear in the output list")
def nexus_should_appear_in_output(nexus_uuid, list_nexuses):
    assert nexus_uuid in [nexus.uuid for nexus in list_nexuses]


@then("no nexus should appear in the output list")
def no_nexus_should_not_appear_in_the_output_list(list_non_existing_nexus):
    assert len(list_non_existing_nexus) == 0


@then("the child should be successfully removed")
def the_child_should_be_successfully_removed(find_nexus, nexus_uuid, nexus_children):
    nexus = find_nexus(nexus_uuid)
    assert len(nexus.children) + 1 == len(nexus_children)
    assert nexus_children[0] not in get_child_uris(nexus)


@then("the child should be successfully added")
def the_child_should_be_successfully_added(find_nexus, nexus_uuid, nexus_children):
    nexus = find_nexus(nexus_uuid)
    assert sorted(get_child_uris(nexus)) == sorted(nexus_children)
    assert nexus.state == pb.NexusState.NEXUS_DEGRADED


@then("the nexus should be successfully published")
def nexus_successfully_published(find_nexus, nexus_uuid):
    nexus = find_nexus(nexus_uuid)
    assert nexus.device_uri


@then("the nexus should be successfully unpublished")
def nexus_successfully_unpublished(find_nexus, nexus_uuid):
    nexus = find_nexus(nexus_uuid)
    assert not nexus.device_uri


@then("the nexus should be successfully published")
def nexus_successfully_published(find_nexus, nexus_uuid):
    nexus = find_nexus(nexus_uuid)
    assert nexus.device_uri
