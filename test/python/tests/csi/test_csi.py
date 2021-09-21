import os
import pytest
import subprocess
import time

from common.mayastor import container_mod
from common.hdl import MayastorHandle
from common.csi_hdl import CsiHandle

import grpc
import csi_pb2 as pb
import csi_pb2_grpc as rpc


def get_uuid(n):
    return "11111111-0000-0000-0000-%.12d" % (n)


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


def test_plugin_info(csi_instance):
    info = csi_instance.identity.GetPluginInfo(pb.GetPluginInfoRequest())
    assert info.name == "io.openebs.csi-mayastor"
    assert info.vendor_version == "0.2"


def test_plugin_capabilities(csi_instance):
    response = csi_instance.identity.GetPluginCapabilities(
        pb.GetPluginCapabilitiesRequest()
    )
    services = [cap.service.type for cap in response.capabilities]
    assert pb.PluginCapability.Service.Type.CONTROLLER_SERVICE in services
    assert pb.PluginCapability.Service.Type.VOLUME_ACCESSIBILITY_CONSTRAINTS in services


def test_probe(csi_instance):
    response = csi_instance.identity.Probe(pb.ProbeRequest())
    assert response.ready


def test_node_info(csi_instance):
    info = csi_instance.node.NodeGetInfo(pb.NodeGetInfoRequest())
    assert info.node_id == "mayastor://ms1-test"
    assert info.max_volumes_per_node == 0


def test_node_capabilities(csi_instance):
    response = csi_instance.node.NodeGetCapabilities(pb.NodeGetCapabilitiesRequest())
    assert pb.NodeServiceCapability.RPC.Type.STAGE_UNSTAGE_VOLUME in [
        cap.rpc.type for cap in response.capabilities
    ]


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


@pytest.fixture(scope="module")
def io_timeout():
    yield "33"


@pytest.fixture(params=["nvmf"])
def share_type(request):
    import mayastor_pb2

    TYPES = {
        "nbd": mayastor_pb2.ShareProtocolNexus.NEXUS_NBD,
        "nvmf": mayastor_pb2.ShareProtocolNexus.NEXUS_NVMF,
        "iscsi": mayastor_pb2.ShareProtocolNexus.NEXUS_ISCSI,
    }
    yield TYPES[request.param]


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


@pytest.fixture(params=["ext4"])
def fs_type(request):
    yield request.param


@pytest.fixture
def volume_id(fs_type):
    # use a different (volume) uuid for each filesystem type
    yield get_uuid(["ext3", "ext4", "xfs"].index(fs_type))


@pytest.fixture
def mayastor_published_nexus(
    mayastor_instance, mayastor_nexuses, share_type, volume_id
):
    uuid = volume_id
    yield mayastor_instance.nexus_publish(uuid, share_type)
    mayastor_instance.nexus_unpublish(uuid)


def test_get_volume_stats(
    csi_instance, mayastor_published_nexus, volume_id, target_path
):
    with pytest.raises(grpc.RpcError) as error:
        csi_instance.node.NodeGetVolumeStats(
            pb.NodeGetVolumeStatsRequest(volume_id=volume_id, volume_path=target_path)
        )
    assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED


@pytest.fixture(params=["multi-node-reader-only", "multi-node-single-writer"])
def access_mode(request):
    MODES = {
        "single-node-writer": pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_WRITER,
        "single-node-reader-only": pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_READER_ONLY,
        "multi-node-reader-only": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_READER_ONLY,
        "multi-node-single-writer": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_SINGLE_WRITER,
        "multi-node-multi-writer": pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_MULTI_WRITER,
    }
    yield MODES[request.param]


@pytest.fixture(params=["rw", "ro"])
def read_only(request):
    yield request.param == "ro"


@pytest.fixture
def compatible(access_mode, read_only):
    yield read_only or access_mode not in [
        pb.VolumeCapability.AccessMode.Mode.SINGLE_NODE_READER_ONLY,
        pb.VolumeCapability.AccessMode.Mode.MULTI_NODE_READER_ONLY,
    ]


@pytest.fixture
def publish_mount_flags(read_only):
    yield ["ro"] if read_only else []


@pytest.fixture
def stage_context(mayastor_published_nexus, io_timeout):
    yield {"uri": mayastor_published_nexus, "ioTimeout": io_timeout}


@pytest.fixture
def publish_context(mayastor_published_nexus, volume_id):
    yield {"uri": mayastor_published_nexus}


@pytest.fixture
def block_volume_capability(access_mode):
    yield pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode),
        block=pb.VolumeCapability.BlockVolume(),
    )


@pytest.fixture
def stage_mount_volume_capability(access_mode, fs_type):
    yield pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode),
        mount=pb.VolumeCapability.MountVolume(fs_type=fs_type, mount_flags=[]),
    )


@pytest.fixture
def publish_mount_volume_capability(access_mode, fs_type, publish_mount_flags):
    yield pb.VolumeCapability(
        access_mode=pb.VolumeCapability.AccessMode(mode=access_mode),
        mount=pb.VolumeCapability.MountVolume(
            fs_type=fs_type, mount_flags=publish_mount_flags
        ),
    )


@pytest.fixture
def staged_block_volume(
    csi_instance, volume_id, stage_context, staging_target_path, block_volume_capability
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=block_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    yield
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_stage_block_volume(
    csi_instance, volume_id, stage_context, staging_target_path, block_volume_capability
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=block_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    time.sleep(0.5)
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_publish_block_volume(
    csi_instance,
    volume_id,
    publish_context,
    staging_target_path,
    target_path,
    block_volume_capability,
    read_only,
    staged_block_volume,
    compatible,
):
    if compatible:
        csi_instance.node.NodePublishVolume(
            pb.NodePublishVolumeRequest(
                volume_id=volume_id,
                publish_context=publish_context,
                staging_target_path=staging_target_path,
                target_path=target_path,
                volume_capability=block_volume_capability,
                readonly=read_only,
                secrets={},
                volume_context={},
            )
        )
        time.sleep(0.5)
        csi_instance.node.NodeUnpublishVolume(
            pb.NodeUnpublishVolumeRequest(volume_id=volume_id, target_path=target_path)
        )
    else:
        with pytest.raises(grpc.RpcError) as error:
            csi_instance.node.NodePublishVolume(
                pb.NodePublishVolumeRequest(
                    volume_id=volume_id,
                    publish_context=publish_context,
                    staging_target_path=staging_target_path,
                    target_path=target_path,
                    volume_capability=block_volume_capability,
                    readonly=read_only,
                    secrets={},
                    volume_context={},
                )
            )
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@pytest.fixture
def staged_mount_volume(
    csi_instance,
    volume_id,
    stage_context,
    staging_target_path,
    stage_mount_volume_capability,
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=stage_mount_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    yield
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_stage_mount_volume(
    csi_instance,
    volume_id,
    stage_context,
    staging_target_path,
    stage_mount_volume_capability,
):
    csi_instance.node.NodeStageVolume(
        pb.NodeStageVolumeRequest(
            volume_id=volume_id,
            publish_context=stage_context,
            staging_target_path=staging_target_path,
            volume_capability=stage_mount_volume_capability,
            secrets={},
            volume_context={},
        )
    )
    time.sleep(0.5)
    csi_instance.node.NodeUnstageVolume(
        pb.NodeUnstageVolumeRequest(
            volume_id=volume_id, staging_target_path=staging_target_path
        )
    )


def test_publish_mount_volume(
    csi_instance,
    volume_id,
    publish_context,
    staging_target_path,
    target_path,
    publish_mount_volume_capability,
    read_only,
    staged_mount_volume,
    compatible,
):
    if compatible:
        csi_instance.node.NodePublishVolume(
            pb.NodePublishVolumeRequest(
                volume_id=volume_id,
                publish_context=publish_context,
                staging_target_path=staging_target_path,
                target_path=target_path,
                volume_capability=publish_mount_volume_capability,
                readonly=read_only,
                secrets={},
                volume_context={},
            )
        )
        time.sleep(0.5)
        csi_instance.node.NodeUnpublishVolume(
            pb.NodeUnpublishVolumeRequest(volume_id=volume_id, target_path=target_path)
        )
    else:
        with pytest.raises(grpc.RpcError) as error:
            csi_instance.node.NodePublishVolume(
                pb.NodePublishVolumeRequest(
                    volume_id=volume_id,
                    publish_context=publish_context,
                    staging_target_path=staging_target_path,
                    target_path=target_path,
                    volume_capability=publish_mount_volume_capability,
                    readonly=read_only,
                    secrets={},
                    volume_context={},
                )
            )
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
