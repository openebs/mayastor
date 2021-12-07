import time

from common.mayastor import container_mod, mayastor_mod as mayastors
import uuid as guid
import pytest

VOLUME_COUNT = 1


def ensure_zero_devices(mayastors):
    """
    Assert all nodes have no bdevs left.
    """
    nodes = ["ms0", "ms1"]

    for node in nodes:
        bdevs = mayastors[node].bdev_list()
    assert len(bdevs) == 0


def create_publish(node, children):
    """
    Create a nexus with the given children. The nexus is created,
    shared, wait, and unshared before finally being destroyed
    """
    nexus_uuids = list()
    for i in range(VOLUME_COUNT):
        # Create the nexus
        new_uuid = guid.uuid4()
        nexus_uuids.append(new_uuid)
        nexus = node.nexus_create(new_uuid, 20 * 1024 * 1024, list(children[i]))

        # Publish the nexus
        node.nexus_publish(nexus.uuid)

    time.sleep(2)

    for i in range(VOLUME_COUNT):
        uuid = nexus_uuids[i]
        node.nexus_destroy(str(uuid))


def delete_all_bdevs(node):
    for dev in node.bdev_list():
        node.bdev_unshare(dev.name)
        node.bdev_destroy(f"malloc:///{dev.name}?size_mb=50")


@pytest.mark.parametrize("times", range(10))
def test_remote_only(mayastors, times):
    print("Run ", times)
    """
        Test nexus with a remote bdev
        """
    remotes = ["ms1"]
    local = "ms0"

    children = list(list())
    for i in range(VOLUME_COUNT):
        children.append(list())

    ensure_zero_devices(mayastors)

    device = "malloc:///malloc{index}?size_mb=50"
    device_name = "malloc{index}"
    for remote in remotes:
        for i in range(VOLUME_COUNT):
            mayastors[remote].bdev_create(device.format(index=i))
            uri = mayastors[remote].bdev_share(device_name.format(index=i))
            children[i].append(uri)

    create_publish(mayastors[local], children)

    for remote in remotes:
        delete_all_bdevs(mayastors[remote])

    ensure_zero_devices(mayastors)
