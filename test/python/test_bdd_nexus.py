import pytest
from pytest_bdd import given, scenario, then, when, parsers

from collections import namedtuple
import subprocess

from common.mayastor import mayastor_mod
from common.volume import Volume

import grpc
import mayastor_pb2 as pb

BaseBdev = namedtuple("BaseBdev", "name uri")

LocalFile = namedtuple("LocalFile", "path uri")


def megabytes(n):
    return n * 1024 * 1024


def get_child_uris(nexus):
    return [child.uri for child in nexus.children]


def share_type(protocol):
    TYPES = {
        "nbd": pb.ShareProtocolNexus.NEXUS_NBD,
        "nvmf": pb.ShareProtocolNexus.NEXUS_NVMF,
        "iscsi": pb.ShareProtocolNexus.NEXUS_ISCSI,
    }
    return TYPES[protocol]


@scenario("features/nexus.feature", "creating a nexus")
def test_creating_a_nexus():
    "Creating a nexus."


@scenario("features/nexus.feature", "creating the same nexus")
def test_creating_the_same_nexus():
    "Creating the same nexus."


@scenario("features/nexus.feature", "creating a different nexus with the same children")
def test_fail_creating_a_different_nexus_with_the_same_children():
    "Creating a different nexus with the same children."


@scenario("features/nexus.feature", "creating a nexus without children")
def test_fail_creating_a_nexus_without_children():
    "Creating a nexus without children."


@scenario("features/nexus.feature", "creating a nexus with missing children")
def test_fail_creating_a_nexus_with_missing_children():
    "Creating a nexus with missing children."


@scenario(
    "features/nexus.feature", "creating a nexus from children with mixed block sizes"
)
def test_fail_creating_a_nexus_from_children_with_mixed_block_sizes():
    "Creating a nexus from children with mixed block sizes."


@scenario("features/nexus.feature", "creating a nexus that is larger than its children")
def test_fail_creating_a_nexus_larger_than_its_children():
    "Creating a nexus that is larger than its children."


@scenario("features/nexus.feature", "destroying a nexus")
def test_destroying_a_nexus():
    "Destroying a nexus."


@scenario("features/nexus.feature", "destroying a nexus without first unpublishing")
def test_destroying_a_nexus_without_first_unpublishing():
    "Destroying a nexus without first unpublishing."


@scenario("features/nexus.feature", "destroying a nexus that does not exist")
def test_destroying_a_nexus_that_does_not_exist():
    "Destroying a nexus that does not exist."


@scenario("features/nexus.feature", "listing nexuses")
def test_listing_nexuses():
    "Listing nexuses."


@scenario("features/nexus.feature", "removing a child from a nexus")
def test_removing_a_child_from_a_nexus():
    "Removing a child from a nexus."


@scenario("features/nexus.feature", "adding a child to a nexus")
def test_adding_a_child_to_a_nexus():
    "Adding a child to a nexus."


@scenario("features/nexus.feature", "publishing a nexus")
def test_publishing_a_nexus():
    "Publishing a nexus."


@scenario("features/nexus.feature", "unpublishing a nexus")
def test_unpublishing_a_nexus():
    "Unpublishing a nexus."


@scenario("features/nexus.feature", "unpublishing a nexus that is not published")
def test_unpublishing_a_nexus_that_is_not_published():
    "Unpublishing a nexus that is not published."


@scenario("features/nexus.feature", "republishing a nexus with a different protocol")
def test_fail_republishing_a_nexus_with_a_different_protocol():
    "Republishing a nexus with a different protocol."


@scenario("features/nexus.feature", "republishing a nexus with the same protocol")
def test_republishing_a_nexus_with_the_same_protocol():
    "Republishing a nexus with the same protocol."


@scenario("features/nexus.feature", "publishing a nexus with a crypto-key")
def test_publishing_a_nexus_with_a_cryptokey():
    "Publishing a nexus with a crypto-key."


@pytest.fixture(scope="module")
def remote_instance():
    yield "ms0"


@pytest.fixture(scope="module")
def nexus_instance():
    yield "ms1"


@pytest.fixture(scope="module")
def base_instances(remote_instance, nexus_instance):
    yield [remote_instance, nexus_instance]


@pytest.fixture(scope="module")
def nexus_uuid():
    yield "86050f0e-6914-4e9c-92b9-1237fd6d17a6"


@pytest.fixture(scope="module")
def local_bdev_uri():
    yield "bdev:///malloc0"


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
def find_nexus(mayastor_mod, nexus_instance):
    def find(uuid):
        for nexus in mayastor_mod[nexus_instance].ms.ListNexus(pb.Null()).nexus_list:
            if nexus.uuid == uuid:
                return nexus
        return None

    yield find


@pytest.fixture
def local_bdev_with_512_blocksize(mayastor_mod, nexus_instance):
    uri = "malloc:///malloc1?size_mb=64&blk_size=512"
    mayastor_mod[nexus_instance].bdev.Create(pb.BdevUri(uri=uri))
    yield uri
    mayastor_mod[nexus_instance].bdev.Destroy(pb.BdevUri(uri=uri))


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
def create_nexus(mayastor_mod, nexus_instance, created_nexuses):
    def create(uuid, size, children):
        nexus = mayastor_mod[nexus_instance].ms.CreateNexus(
            pb.CreateNexusRequest(uuid=uuid, size=size, children=children)
        )
        created_nexuses[uuid] = nexus
        return nexus

    yield create


@given("a list of child devices")
def get_child_devices(nexus_children):
    pass


@given("a local mayastor instance")
def local_mayastor_instance(nexus_instance):
    pass


@given("a nexus")
@given("an unpublished nexus")
def get_nexus(create_nexus, nexus_uuid, nexus_children):
    create_nexus(nexus_uuid, megabytes(64), nexus_children)


@given(
    parsers.parse('a nexus published via "{protocol}"'),
    target_fixture="get_published_nexus",
)
def get_published_nexus(
    create_nexus,
    mayastor_mod,
    nexus_instance,
    nexus_uuid,
    nexus_children,
    find_nexus,
    protocol,
):
    create_nexus(nexus_uuid, megabytes(64), nexus_children)
    mayastor_mod[nexus_instance].ms.PublishNexus(
        pb.PublishNexusRequest(uuid=nexus_uuid, key="", share=share_type(protocol))
    )
    nexus = find_nexus(nexus_uuid)
    assert nexus.device_uri


@given("a remote mayastor instance")
def remote_mayastor_instance(remote_instance):
    pass


@when("creating a nexus")
@when("creating an identical nexus")
def creating_a_nexus(create_nexus, nexus_uuid, nexus_children):
    create_nexus(nexus_uuid, megabytes(64), nexus_children)


@when("attempting to create a new nexus")
def attempt_to_create_new_nexus(create_nexus, nexus_children):
    with pytest.raises(grpc.RpcError) as error:
        create_nexus(
            "ed378e05-d704-4e7a-a5bc-7d2344b3fb83", megabytes(64), nexus_children
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("attempting to create a nexus with no children")
def attempt_to_create_nexus_with_no_children(create_nexus, nexus_uuid):
    with pytest.raises(grpc.RpcError) as error:
        create_nexus(nexus_uuid, megabytes(64), [])
    assert error.value.code() == grpc.StatusCode.INTERNAL


@when("attempting to create a nexus with a child URI that does not exist")
def attempt_to_create_nexus_with_child_uri_that_does_not_exist(
    create_nexus, nexus_uuid, nexus_children
):
    with pytest.raises(grpc.RpcError) as error:
        create_nexus(
            nexus_uuid,
            megabytes(64),
            nexus_children + ["nvmf://10.0.0.2:8420/nqn.2019-05.io.openebs:missing"],
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("attempting to create a nexus from child devices with mixed block sizes")
def attempt_to_create_nexus_from_child_devices_with_mixed_block_sizes(
    create_nexus, nexus_uuid, nexus_children, local_bdev_with_512_blocksize
):
    with pytest.raises(grpc.RpcError) as error:
        create_nexus(nexus_uuid, megabytes(64), ["bdev:///malloc0", "bdev:///malloc1"])
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when(
    "attempting to create a nexus with a size larger than that of any of its children"
)
def attempt_to_create_nexus_with_size_larger_than_size_of_children(
    create_nexus, nexus_uuid, nexus_children
):
    with pytest.raises(grpc.RpcError) as error:
        create_nexus(nexus_uuid, megabytes(128), nexus_children)
    assert error.value.code() == grpc.StatusCode.INTERNAL


@when("destroying the nexus")
@when("destroying the nexus without unpublishing it")
def destroying_the_nexus(mayastor_mod, nexus_instance, nexus_uuid, created_nexuses):
    mayastor_mod[nexus_instance].ms.DestroyNexus(
        pb.DestroyNexusRequest(uuid=nexus_uuid)
    )
    del created_nexuses[nexus_uuid]


@when("destroying a nexus that does not exist")
def destroying_a_nexus_that_does_not_exist(mayastor_mod, nexus_instance):
    mayastor_mod[nexus_instance].ms.DestroyNexus(
        pb.DestroyNexusRequest(uuid="e6629036-1376-494d-bbc2-0b6345ab10df")
    )


@when("listing all nexuses", target_fixture="list_nexuses")
def list_nexuses(mayastor_mod, nexus_instance):
    return mayastor_mod[nexus_instance].ms.ListNexus(pb.Null()).nexus_list


@when(
    parsers.parse('publishing a nexus via "{protocol}"'), target_fixture="publish_nexus"
)
def publish_nexus(mayastor_mod, nexus_instance, nexus_uuid, protocol):
    mayastor_mod[nexus_instance].ms.PublishNexus(
        pb.PublishNexusRequest(uuid=nexus_uuid, key="", share=share_type(protocol))
    )


@when("publishing the nexus with the same protocol")
def publishing_the_nexus_with_the_same_protocol(
    find_nexus, mayastor_mod, nexus_instance, nexus_uuid
):
    nexus = find_nexus(nexus_uuid)
    mayastor_mod[nexus_instance].ms.PublishNexus(
        pb.PublishNexusRequest(
            uuid=nexus_uuid, key="", share=pb.ShareProtocolNexus.NEXUS_NVMF
        )
    )


@when("attempting to publish the nexus with a different protocol")
def attempt_to_publish_nexus_with_different_protocol(
    find_nexus, mayastor_mod, nexus_instance, nexus_uuid
):
    nexus = find_nexus(nexus_uuid)
    with pytest.raises(grpc.RpcError) as error:
        mayastor_mod[nexus_instance].ms.PublishNexus(
            pb.PublishNexusRequest(
                uuid=nexus_uuid, key="", share=pb.ShareProtocolNexus.NEXUS_NBD
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("publishing the nexus using a crypto-key")
def publish_nexus_with_cryptokey(mayastor_mod, nexus_instance, nexus_uuid):
    mayastor_mod[nexus_instance].ms.PublishNexus(
        pb.PublishNexusRequest(
            uuid=nexus_uuid, key="0123456789123456", share=share_type("nvmf")
        )
    )


@when("unpublishing the nexus")
def unpublish_nexus(mayastor_mod, nexus_instance, nexus_uuid):
    mayastor_mod[nexus_instance].ms.UnpublishNexus(
        pb.UnpublishNexusRequest(uuid=nexus_uuid)
    )


@when("removing a child")
def remove_child(mayastor_mod, nexus_instance, nexus_uuid, nexus_children):
    mayastor_mod[nexus_instance].ms.RemoveChildNexus(
        pb.RemoveChildNexusRequest(uuid=nexus_uuid, uri=nexus_children[0])
    )


@when("adding the removed child")
def add_child(mayastor_mod, nexus_instance, nexus_uuid, nexus_children):
    child = mayastor_mod[nexus_instance].ms.AddChildNexus(
        pb.AddChildNexusRequest(uuid=nexus_uuid, uri=nexus_children[0])
    )
    assert child.state == pb.ChildState.CHILD_DEGRADED


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


@then("the nexus should appear in the output list")
def nexus_should_appear_in_output(nexus_uuid, list_nexuses):
    assert nexus_uuid in [nexus.uuid for nexus in list_nexuses]


@then("the nexus should be created")
def nexus_should_be_created(find_nexus, nexus_uuid, nexus_children):
    nexus = find_nexus(nexus_uuid)
    assert nexus != None
    assert sorted(get_child_uris(nexus)) == sorted(nexus_children)
    assert nexus.state == pb.NexusState.NEXUS_ONLINE
    for child in nexus.children:
        assert child.state == pb.ChildState.CHILD_ONLINE


@then("the nexus should not be created")
def nexus_should_not_be_created(find_nexus, nexus_uuid):
    assert find_nexus(nexus_uuid) == None


@then("the nexus should be destroyed")
def nexus_should_be_destroyed(find_nexus, nexus_uuid):
    assert find_nexus(nexus_uuid) == None


@then("the nexus should be successfully published")
def nexus_successfully_published(find_nexus, nexus_uuid):
    nexus = find_nexus(nexus_uuid)
    assert nexus.device_uri


@then("the nexus should be successfully unpublished")
def nexus_successfully_unpublished(find_nexus, nexus_uuid):
    nexus = find_nexus(nexus_uuid)
    assert not nexus.device_uri


@then("the operation should fail with existing URIs in use")
@then("the operation should fail")
@then("the request should fail")
def operation_should_fail():
    pass


@then("the operation should succeed")
@then("the request should succeed")
def operation_should_succeed():
    pass
