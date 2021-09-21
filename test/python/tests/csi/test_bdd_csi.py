import pytest
from pytest_bdd import given, scenario, then, when, parsers

import os
import subprocess
import time

from common.mayastor import container_mod
from common.hdl import MayastorHandle
from common.csi_hdl import CsiHandle

import grpc
import csi_pb2 as pb
import csi_pb2_grpc as rpc


class Nexus:
    def __init__(self, uuid, protocol, uri):
        self.uuid = uuid
        self.protocol = protocol
        self.uri = uri


class Volume:
    def __init__(self, uuid, protocol, uri, mode, staging_target_path, fs_type):
        self.uuid = uuid
        self.protocol = protocol
        self.uri = uri
        self.mode = mode
        self.staging_target_path = staging_target_path
        self.fs_type = fs_type


class PublishedVolume:
    def __init__(self, volume, read_only, target_path):
        self.volume = volume
        self.read_only = read_only
        self.target_path = target_path


def get_uuid(n):
    return "11111111-0000-0000-0000-%.12d" % (n)


def share_type(protocol):
    import mayastor_pb2

    TYPES = {
        "nbd": mayastor_pb2.ShareProtocolNexus.NEXUS_NBD,
        "nvmf": mayastor_pb2.ShareProtocolNexus.NEXUS_NVMF,
        "iscsi": mayastor_pb2.ShareProtocolNexus.NEXUS_ISCSI,
    }
    return TYPES[protocol]


def access_mode(name):
    MODES = {
        "SINGLE_NODE_WRITER": pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_WRITER,
        "SINGLE_NODE_READER_ONLY": pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_READER_ONLY,
        "MULTI_NODE_READER_ONLY": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_READER_ONLY,
        "MULTI_NODE_SINGLE_WRITER": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_SINGLE_WRITER,
        "MULTI_NODE_MULTI_WRITER": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_MULTI_WRITER,
    }
    return MODES[name]


def get_volume_capability(volume, read_only):
    if volume.fs_type == "raw":
        return pb.VolumeCapability(
            access_mode=pb.VolumeCapability.AccessMode(mode=access_mode(volume.mode)),
            block=pb.VolumeCapability.BlockVolume(),
        )

    mount_flags = ["ro"] if read_only else []

    return pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode(volume.mode)),
        mount=pb.VolumeCapability.MountVolume(
            fs_type=volume.fs_type, mount_flags=mount_flags
        ),
    )


@pytest.fixture(scope="module")
def mayastor_instance(container_mod):
    container = container_mod.get("ms0")
    yield MayastorHandle(
        container.get("NetworkSettings.Networks.mayastor_net.IPAddress")
    )


@pytest.fixture(scope="module")
def csi_container(container_mod):
    # wait until container has address
    container_mod.get("ms1").get("NetworkSettings.Networks.mayastor_net.IPAddress")
    yield


@pytest.fixture(scope="module")
def fix_socket_permissions(csi_container):
    subprocess.run(["sudo", "chmod", "go+rw", "/tmp/csi.sock"], check=True)
    yield


@pytest.fixture(scope="module")
def csi_instance(fix_socket_permissions):
    yield CsiHandle("unix:///tmp/csi.sock")


@pytest.fixture
def staging_target_path():
    yield "/tmp/staging/mount"


@pytest.fixture
def target_path():
    try:
        os.mkdir("/tmp/publish")
    except FileExistsError:
        pass
    yield "/tmp/publish/mount"


@pytest.fixture(scope="module")
def io_timeout():
    yield "30"


@pytest.fixture(scope="module")
def mayastor_base_bdevs(mayastor_instance):
    devices = {}
    for n in range(5):
        uuid = get_uuid(n)
        uri = f"malloc:///malloc{n}?size_mb=64&uuid={uuid}&blk_size=4096"
        bdev = mayastor_instance.bdev_create(uri)
        devices[bdev.name] = uri
        mayastor_instance.bdev_share(bdev.name)
    yield devices
    for name, uri in devices.items():
        mayastor_instance.bdev_unshare(name)
        mayastor_instance.bdev_destroy(uri)


@pytest.fixture(scope="module")
def mayastor_nexuses(mayastor_instance, mayastor_base_bdevs):
    nexuses = []
    for n in range(5):
        uuid = get_uuid(n)
        nexus = mayastor_instance.nexus_create(
            uuid, 64 * 1024 * 1024, children=[f"bdev:///malloc{n}"]
        )
        nexuses.append(nexus.uuid)
    yield nexuses
    for uuid in nexuses:
        mayastor_instance.nexus_destroy(uuid)


@scenario("features/csi.feature", "publish volume request")
def test_publish_volume_request():
    "Publish volume request."


@scenario(
    "features/csi.feature", "publish volume request without specified target_path"
)
def test_publish_volume_request_without_specified_target_path():
    "Publish volume request without specified target_path."


@scenario("features/csi.feature", "publishing a reader only block volume as readonly")
def test_publishing_a_reader_only_block_volume_as_readonly():
    "Publishing a reader only block volume as readonly."


@scenario("features/csi.feature", "publishing a reader only block volume as rw")
def test_publishing_a_reader_only_block_volume_as_rw():
    "Publishing a reader only block volume as rw."


@scenario("features/csi.feature", "publishing a reader only mount volume as readonly")
def test_publishing_a_reader_only_mount_volume_as_readonly():
    "Publishing a reader only mount volume as readonly."


@scenario("features/csi.feature", "publishing a reader only mount volume as rw")
def test_publishing_a_reader_only_mount_volume_as_rw():
    "Publishing a reader only mount volume as rw."


@scenario("features/csi.feature", "publishing a single writer block volume as readonly")
def test_publishing_a_single_writer_block_volume_as_readonly():
    "Publishing a single writer block volume as readonly."


@scenario("features/csi.feature", "publishing a single writer block volume as rw")
def test_publishing_a_single_writer_block_volume_as_rw():
    "Publishing a single writer block volume as rw."


@scenario("features/csi.feature", "publishing a single writer mount volume as readonly")
def test_publishing_a_single_writer_mount_volume_as_readonly():
    "Publishing a single writer mount volume as readonly."


@scenario("features/csi.feature", "publishing a single writer mount volume as rw")
def test_publishing_a_single_writer_mount_volume_as_rw():
    "Publishing a single writer mount volume as rw."


@scenario(
    "features/csi.feature", "publishing the same volumes with a different target_path"
)
def test_publishing_the_same_volumes_with_a_different_target_path():
    "Publishing the same volumes with a different target_path."


@scenario("features/csi.feature", "republishing a volume")
def test_republishing_a_volume():
    "Republishing a volume."


@scenario("features/csi.feature", "restaging a volume")
def test_restaging_a_volume():
    "Restaging a volume."


@scenario("features/csi.feature", "stage volume request with unsupported fs_type")
def test_stage_volume_request_with_unsupported_fs_type():
    "Stage volume request with unsupported fs_type."


@scenario("features/csi.feature", "stage volume request without specified access_mode")
def test_stage_volume_request_without_specified_access_mode():
    "Stage volume request without specified access_mode."


@scenario("features/csi.feature", "stage volume request without specified mount")
def test_stage_volume_request_without_specified_mount():
    "Stage volume request without specified mount."


@scenario(
    "features/csi.feature", "stage volume request without specified staging_target_path"
)
def test_stage_volume_request_without_specified_staging_target_path():
    "Stage volume request without specified staging_target_path."


@scenario(
    "features/csi.feature", "stage volume request without specified volume_capability"
)
def test_stage_volume_request_without_specified_volume_capability():
    "Stage volume request without specified volume_capability."


@scenario("features/csi.feature", "stage volume request without specified volume_id")
def test_stage_volume_request_without_specified_volume_id():
    "Stage volume request without specified volume_id."


@scenario("features/csi.feature", "staging a single writer volume")
def test_staging_a_single_writer_volume():
    "Staging a single writer volume."


@scenario(
    "features/csi.feature",
    "staging different volumes with the same staging_target_path",
)
def test_staging_different_volumes_with_the_same_staging_target_path():
    "Staging different volumes with the same staging_target_path."


@scenario(
    "features/csi.feature",
    "staging the same volumes with a different staging_target_path",
)
def test_staging_the_same_volumes_with_a_different_staging_target_path():
    "Staging the same volumes with a different staging_target_path."


@scenario("features/csi.feature", "unstaging a single writer volume")
def test_unstaging_a_single_writer_volume():
    "Unstaging a single writer volume."


@pytest.fixture
def published_nexuses(mayastor_instance, mayastor_nexuses):
    published = {}
    yield published
    for uuid in published.keys():
        mayastor_instance.nexus_unpublish(uuid)


@pytest.fixture
def publish_nexus(mayastor_instance, published_nexuses):
    def publish(uuid, protocol):
        uri = mayastor_instance.nexus_publish(uuid, share_type(protocol))
        nexus = Nexus(uuid, protocol, uri)
        published_nexuses[uuid] = nexus
        return nexus

    yield publish


@pytest.fixture
def staged_volumes(csi_instance):
    staged = {}
    yield staged
    for volume in staged.values():
        csi_instance.node.NodeUnstageVolume(
            pb.NodeUnstageVolumeRequest(
                volume_id=volume.uuid, staging_target_path=volume.staging_target_path
            )
        )


@pytest.fixture
def stage_volume(csi_instance, publish_nexus, staged_volumes, io_timeout):
    def stage(volume):
        csi_instance.node.NodeStageVolume(
            pb.NodeStageVolumeRequest(
                volume_id=volume.uuid,
                publish_context={"uri": volume.uri, "ioTimeout": io_timeout},
                staging_target_path=volume.staging_target_path,
                volume_capability=get_volume_capability(volume, False),
                secrets={},
                volume_context={},
            )
        )
        staged_volumes[volume.uuid] = volume

    yield stage


@pytest.fixture
def published_volumes(csi_instance):
    published = {}
    yield published
    for volume in published.values():
        csi_instance.node.NodeUnpublishVolume(
            pb.NodeUnpublishVolumeRequest(
                volume_id=volume.volume.uuid, target_path=volume.target_path
            )
        )


@pytest.fixture
def publish_volume(csi_instance, publish_nexus, published_volumes):
    def publish(volume, read_only, target_path):
        csi_instance.node.NodePublishVolume(
            pb.NodePublishVolumeRequest(
                volume_id=volume.uuid,
                publish_context={"uri": volume.uri},
                staging_target_path=volume.staging_target_path,
                target_path=target_path,
                volume_capability=get_volume_capability(volume, read_only),
                readonly=read_only,
                secrets={},
                volume_context={},
            )
        )
        published_volumes[volume.uuid] = PublishedVolume(volume, read_only, target_path)

    yield publish


@given("a mayastor instance")
def get_mayastor_instance(mayastor_instance):
    pass


@given("a mayastor-csi instance")
def get_mayastor_csi_instance(csi_instance):
    pass


@given(
    parsers.parse('a nexus published via "{protocol}"'),
    target_fixture="get_published_nexus",
)
def get_published_nexus(publish_nexus, protocol):
    uuid = get_uuid(0)
    return publish_nexus(uuid, protocol)


@given(
    parsers.parse('an "{fs_type}" volume staged as "{mode}"'),
    target_fixture="get_staged_volume",
)
def get_staged_volume(
    get_published_nexus, stage_volume, staging_target_path, fs_type, mode
):
    nexus = get_published_nexus
    volume = Volume(
        nexus.uuid, nexus.protocol, nexus.uri, mode, staging_target_path, fs_type
    )
    stage_volume(volume)
    return volume


@given(
    parsers.parse('a block volume staged as "{mode}"'),
    target_fixture="get_staged_block_volume",
)
def get_staged_block_volume(
    get_published_nexus, stage_volume, staging_target_path, mode
):
    nexus = get_published_nexus
    volume = Volume(
        nexus.uuid, nexus.protocol, nexus.uri, mode, staging_target_path, "raw"
    )
    stage_volume(volume)
    return volume


@given("a published volume", target_fixture="generic_published_volume")
def generic_published_volume(
    generic_staged_volume, publish_volume, published_volumes, target_path
):
    volume = generic_staged_volume
    publish_volume(volume, False, target_path)
    return published_volumes[volume.uuid]


@given("a staged volume", target_fixture="generic_staged_volume")
def generic_staged_volume(get_published_nexus, stage_volume, staging_target_path):
    nexus = get_published_nexus
    volume = Volume(
        nexus.uuid,
        nexus.protocol,
        nexus.uri,
        "MULTI_NODE_SINGLE_WRITER",
        staging_target_path,
        "ext4",
    )
    stage_volume(volume)
    return volume


@when("attempting to stage a different volume with the same staging_target_path")
def attempt_to_stage_different_volume_with_same_staging_target_path(
    publish_nexus, get_staged_volume, stage_volume
):
    volume = get_staged_volume
    uuid = get_uuid(1)
    nexus = publish_nexus(uuid, volume.protocol)
    volume = Volume(
        nexus.uuid,
        nexus.protocol,
        nexus.uri,
        volume.mode,
        volume.staging_target_path,
        volume.fs_type,
    )
    with pytest.raises(grpc.RpcError) as error:
        stage_volume(volume)


@when("staging a volume with a missing staging_target_path")
def attempt_to_stage_volume_with_missing_staging_target_path(
    get_published_nexus, csi_instance, io_timeout
):
    nexus = get_published_nexus
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeStageVolume(
            pb.NodeStageVolumeRequest(
                volume_id=nexus.uuid,
                publish_context={"uri": nexus.uri, "ioTimeout": io_timeout},
                volume_capability=pb.VolumeCapability(
                    access_mode=pb.VolumeCapability.AccessMode(
                        mode=pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_SINGLE_WRITER
                    ),
                    mount=pb.VolumeCapability.MountVolume(
                        fs_type="ext4", mount_flags=[]
                    ),
                ),
                secrets={},
                volume_context={},
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("staging a volume with a missing volume_capability")
def attempt_to_stage_volume_with_missing_volume_capability(
    get_published_nexus, csi_instance, staging_target_path, io_timeout
):
    nexus = get_published_nexus
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeStageVolume(
            pb.NodeStageVolumeRequest(
                volume_id=nexus.uuid,
                publish_context={"uri": nexus.uri, "ioTimeout": io_timeout},
                staging_target_path=staging_target_path,
                secrets={},
                volume_context={},
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("staging a volume with a missing volume_id")
def attempt_to_stage_volume_with_missing_volume_id(
    get_published_nexus, csi_instance, staging_target_path, io_timeout
):
    nexus = get_published_nexus
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeStageVolume(
            pb.NodeStageVolumeRequest(
                publish_context={"uri": nexus.uri, "ioTimeout": io_timeout},
                staging_target_path=staging_target_path,
                volume_capability=pb.VolumeCapability(
                    access_mode=pb.VolumeCapability.AccessMode(
                        mode=pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_SINGLE_WRITER
                    ),
                    mount=pb.VolumeCapability.MountVolume(
                        fs_type="ext4", mount_flags=[]
                    ),
                ),
                secrets={},
                volume_context={},
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("staging a volume with a volume_capability with a missing access_mode")
def attempt_to_stage_volume_with_missing_access_mode(
    get_published_nexus, csi_instance, staging_target_path, io_timeout
):
    nexus = get_published_nexus
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeStageVolume(
            pb.NodeStageVolumeRequest(
                volume_id=nexus.uuid,
                publish_context={"uri": nexus.uri, "ioTimeout": io_timeout},
                staging_target_path=staging_target_path,
                volume_capability=pb.VolumeCapability(
                    mount=pb.VolumeCapability.MountVolume(
                        fs_type="ext4", mount_flags=[]
                    )
                ),
                secrets={},
                volume_context={},
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("staging a volume with a volume_capability with a missing mount")
def attempt_to_stage_volume_with_missing_mount(
    get_published_nexus, csi_instance, staging_target_path, io_timeout
):
    nexus = get_published_nexus
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeStageVolume(
            pb.NodeStageVolumeRequest(
                volume_id=nexus.uuid,
                publish_context={"uri": nexus.uri, "ioTimeout": io_timeout},
                staging_target_path=staging_target_path,
                volume_capability=pb.VolumeCapability(
                    access_mode=pb.VolumeCapability.AccessMode(
                        mode=pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_SINGLE_WRITER
                    ),
                ),
                secrets={},
                volume_context={},
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when(
    "staging a volume with a volume_capability with a mount with an unsupported fs_type"
)
def attempt_to_stage_volume_with_unsupported_fs_type(
    get_published_nexus, csi_instance, staging_target_path, io_timeout
):
    nexus = get_published_nexus
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeStageVolume(
            pb.NodeStageVolumeRequest(
                volume_id=nexus.uuid,
                publish_context={"uri": nexus.uri, "ioTimeout": io_timeout},
                staging_target_path=staging_target_path,
                volume_capability=pb.VolumeCapability(
                    access_mode=pb.VolumeCapability.AccessMode(
                        mode=pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_SINGLE_WRITER
                    ),
                    mount=pb.VolumeCapability.MountVolume(
                        fs_type="ext3", mount_flags=[]
                    ),
                ),
                secrets={},
                volume_context={},
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when(parsers.parse('staging an "{fs_type}" volume as "{mode}"'))
def stage_new_volume(
    get_published_nexus, stage_volume, staging_target_path, fs_type, mode
):
    nexus = get_published_nexus
    volume = Volume(
        nexus.uuid, nexus.protocol, nexus.uri, mode, staging_target_path, fs_type
    )
    stage_volume(volume)


@when("staging the same volume")
def stage_same_volume(get_staged_volume, stage_volume):
    volume = get_staged_volume
    stage_volume(volume)


@when("attempting to stage a different volume with the same staging_target_path")
def attempt_to_stage_different_volume_with_same_staging_target_path(
    get_staged_volume, publish_nexus, stage_volume
):
    volume = get_staged_volume
    uuid = get_uuid(1)
    nexus = publish_nexus(uuid, volume.protocol)
    with pytest.raises(grpc.RpcError) as error:
        stage_volume(
            Volume(
                nexus.uuid,
                nexus.protocol,
                nexus.uri,
                volume.mode,
                volume.staging_target_path,
                "ext4",
            )
        )
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS


@when("staging the same volume but with a different staging_target_path")
def attempt_to_stage_same_volume_with_different_staging_target_path(
    get_staged_volume, stage_volume
):
    volume = get_staged_volume
    with pytest.raises(grpc.RpcError) as error:
        stage_volume(
            Volume(
                volume.uuid,
                volume.protocol,
                volume.uri,
                volume.mode,
                "/tmp/different/staging/mount",
                volume.fs_type,
            )
        )
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS


@when("unstaging the volume")
def unstaging_the_volume(csi_instance, get_staged_volume, staged_volumes):
    volume = get_staged_volume
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume.uuid, staging_target_path=volume.staging_target_path
        )
    )
    del staged_volumes[volume.uuid]


@when("publishing a volume")
def generic_published_volume(generic_staged_volume, publish_volume, target_path):
    volume = generic_staged_volume
    publish_volume(volume, False, target_path)


@when("publishing a volume with a missing target_path")
def attempt_to_publish_volume_with_missing_target_path(
    csi_instance, generic_staged_volume
):
    volume = generic_staged_volume
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodePublishVolume(
            pb.NodePublishVolumeRequest(
                volume_id=volume.uuid,
                publish_context={"uri": volume.uri},
                staging_target_path=volume.staging_target_path,
                volume_capability=get_volume_capability(volume, False),
                readonly=False,
                secrets={},
                volume_context={},
            )
        )
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when("publishing the same volume")
def publish_same_volume(generic_published_volume, publish_volume):
    volume = generic_published_volume
    publish_volume(volume.volume, volume.read_only, volume.target_path)


@when("publishing the same volume with a different target_path")
def attempt_to_publish_same_volume_with_different_target_path(
    generic_published_volume, publish_volume
):
    with pytest.raises(grpc.RpcError) as error:
        volume = generic_published_volume
        publish_volume(volume.volume, volume.read_only, "/tmp/different/publish/mount")
    assert error.value.code() == grpc.StatusCode.INTERNAL


@when(parsers.parse('publishing the volume as "{flags}" should {disposition}'))
def publish_volume_as_read_or_write(
    get_staged_volume, publish_volume, target_path, flags, disposition
):
    volume = get_staged_volume
    if disposition == "succeed":
        publish_volume(volume, flags == "ro", target_path)
    else:
        with pytest.raises(grpc.RpcError) as error:
            publish_volume(volume, flags == "ro", target_path)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@when(parsers.parse('publishing the block volume as "{flags}" should {disposition}'))
def publish_block_volume_as_read_or_write(
    get_staged_block_volume, publish_volume, target_path, flags, disposition
):
    volume = get_staged_block_volume
    if disposition == "succeed":
        publish_volume(volume, flags == "ro", target_path)
    else:
        with pytest.raises(grpc.RpcError) as error:
            publish_volume(volume, flags == "ro", target_path)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@then(parsers.parse("the request should {disposition}"))
def request_success_expected(disposition):
    return disposition == "succeed"
