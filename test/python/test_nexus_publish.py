import pytest

from collections import namedtuple
import subprocess

from common.mayastor import mayastor_mod

import grpc
import mayastor_pb2 as pb

BaseBdev = namedtuple("BaseBdev", "name uri")

LocalFile = namedtuple("LocalFile", "path uri")


def megabytes(n):
    return n * 1024 * 1024


def get_uuid(n):
    return "11111111-0000-0000-0000-%.12d" % (n)


def get_child_uris(nexus):
    return [child.uri for child in nexus.children]


def share_type(protocol):
    TYPES = {
        "nbd": pb.ShareProtocolNexus.NEXUS_NBD,
        "nvmf": pb.ShareProtocolNexus.NEXUS_NVMF,
        "iscsi": pb.ShareProtocolNexus.NEXUS_ISCSI,
    }
    return TYPES[protocol]


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
def nexus_count(mayastor_mod, nexus_instance):
    def count():
        return len(mayastor_mod[nexus_instance].ms.ListNexus(pb.Null()).nexus_list)

    yield count


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


@pytest.fixture()
def publish_nexus(mayastor_mod, nexus_instance):
    def publish(uuid, protocol):
        mayastor_mod[nexus_instance].ms.PublishNexus(
            pb.PublishNexusRequest(uuid=uuid, key="", share=share_type(protocol))
        )

    yield publish


@pytest.fixture()
def unpublish_nexus(mayastor_mod, nexus_instance):
    def unpublish(uuid):
        mayastor_mod[nexus_instance].ms.UnpublishNexus(
            pb.UnpublishNexusRequest(uuid=uuid)
        )

    yield unpublish


@pytest.fixture()
def destroy_nexus(mayastor_mod, nexus_instance, created_nexuses):
    def destroy(uuid):
        mayastor_mod[nexus_instance].ms.DestroyNexus(pb.DestroyNexusRequest(uuid=uuid))
        del created_nexuses[uuid]

    yield destroy


@pytest.fixture(params=["iscsi", "nvmf"])
def share_protocol(request):
    yield request.param


@pytest.mark.parametrize("n", range(5))
def test_create_destroy(create_nexus, destroy_nexus, nexus_count, nexus_children, n):
    uuid = get_uuid(n)
    create_nexus(uuid, megabytes(64), nexus_children)
    destroy_nexus(uuid)
    assert nexus_count() == 0


@pytest.mark.parametrize("n", range(5))
def test_create_publish_unpublish_destroy(
    create_nexus,
    publish_nexus,
    unpublish_nexus,
    destroy_nexus,
    nexus_count,
    nexus_children,
    share_protocol,
    n,
):
    uuid = get_uuid(n)
    create_nexus(uuid, megabytes(64), nexus_children)
    publish_nexus(uuid, share_protocol)
    unpublish_nexus(uuid)
    destroy_nexus(uuid)
    assert nexus_count() == 0


@pytest.mark.parametrize("n", range(5))
def test_create_publish_destroy(
    create_nexus,
    publish_nexus,
    destroy_nexus,
    nexus_count,
    nexus_children,
    share_protocol,
    n,
):
    uuid = get_uuid(n)
    create_nexus(uuid, megabytes(64), nexus_children)
    publish_nexus(uuid, share_protocol)
    destroy_nexus(uuid)
    assert nexus_count() == 0
