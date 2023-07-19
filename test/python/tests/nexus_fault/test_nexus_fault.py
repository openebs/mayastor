"""Faulted nexus I/O management feature tests."""

import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)

import os
import subprocess
import time

from retrying import retry

from common.command import run_cmd
from common.fio import Fio
from common.mayastor import container_mod, mayastor_mod
from common.nvme import nvme_connect, nvme_disconnect

import grpc
import nexus_pb2 as pb


def megabytes(n):
    return n * 1024 * 1024


@scenario(
    "features/nexus_fault.feature",
    "a temporarily faulted nexus should not cause initiator filesystem to shutdown",
)
def test_a_temporarily_faulted_nexus_should_not_cause_initiator_filesystem_to_shutdown():
    """a temporarily faulted nexus should not cause initiator filesystem to shutdown."""


@given("a filesystem is placed on top of the connected device")
def _(connect_nexus_1):
    """a filesystem is placed on top of the connected device."""
    device = connect_nexus_1
    print(device)
    run_cmd(f"sudo mkfs.xfs {device}")


@given(
    "a fio workload is started on top of the mounted filesystem", target_fixture="fio"
)
def _(mounted_nexus):
    """a fio workload is started on top of the mounted filesystem."""
    fio_cmd = Fio(
        f"job-raw", "randwrite", f"{mounted_nexus}/fio.io", size="200M"
    ).build()
    print(fio_cmd)
    yield subprocess.Popen(fio_cmd, shell=True)


@given("a local mayastor instance")
def _(remote_instance):
    """a local mayastor instance."""


@given("a remote mayastor instance")
def _(local_instance):
    """a remote mayastor instance."""


@given("a single replica (remote) nexus is published via nvmf")
def _(create_nexus):
    """a single replica (remote) nexus is published via nvmf."""
    nexus = create_nexus
    print(nexus)


@given("the filesystem is mounted", target_fixture="mounted_nexus")
def _(connect_nexus_1):
    """the filesystem is mounted."""
    dev = connect_nexus_1
    path = f"/mnt{dev}"
    run_cmd(f"sudo mkdir -p {path}")
    run_cmd(f"sudo mount {dev} {path}")
    yield path
    run_cmd(f"sudo umount {path}")


@given("the nexus is connected to a kernel initiator", target_fixture="connect_nexus_1")
def _(publish_nexus):
    """the nexus is connected to a kernel initiator."""
    yield nvme_connect(publish_nexus)
    nvme_disconnect(publish_nexus)


@when("the remote mayastor instance is restarted")
def _(container_mod, mayastor_mod, remote_instance, nexus_uuid, find_nexus):
    """the remote mayastor instance is restarted."""
    container_mod.get(remote_instance).restart()
    nexus = find_nexus(nexus_uuid)
    while nexus.state != pb.NexusState.NEXUS_FAULTED:
        nexus = find_nexus(nexus_uuid)
        print(nexus)

    remote_ready(mayastor_mod, remote_instance)


@when("the faulted nexus is recreated")
def _(recreate_pool, republish_nexus):
    """the faulted nexus is recreated."""


@then("the fio workload should complete gracefully")
def _(fio):
    """the fio workload should complete gracefully."""
    try:
        code = fio.wait(timeout=60)
    except subprocess.TimeoutExpired:
        assert False, "FIO timed out"
    assert code == 0, "FIO failed, exit code: %d" % code


@then("the initiator filesystem should not be shutdown")
def _(mounted_nexus):
    """the initiator filesystem should not be shutdown."""
    try:
        # xfs_info should still be working as the fs is not shutdown
        run_cmd(f"sudo xfs_info {mounted_nexus}")
    except Exception as e:
        pytest.fail(f"Filesystem on {mounted_nexus} should not be shutdown: {e}")


@pytest.fixture(scope="module")
def remote_instance():
    yield "ms0"


@pytest.fixture(scope="module")
def local_instance():
    yield "ms1"


@pytest.fixture
def create_nexus(mayastor_mod, nexus_uuid, create_replica, local_instance):
    nexus = mayastor_mod[local_instance].nexus_create(
        uuid=nexus_uuid,
        size=megabytes(303),
        children=list([create_replica.uri]),
    )
    yield nexus


@pytest.fixture
def publish_nexus(mayastor_mod, nexus_uuid, create_nexus, local_instance):
    yield mayastor_mod[local_instance].nexus_publish(nexus_uuid)


@pytest.fixture
def republish_nexus(mayastor_mod, nexus_uuid, local_instance, recreate_replica):
    mayastor_mod[local_instance].nexus_destroy(nexus_uuid)
    mayastor_mod[local_instance].nexus_create(
        uuid=nexus_uuid,
        size=megabytes(303),
        children=list([recreate_replica.uri]),
    )
    yield mayastor_mod[local_instance].nexus_publish(nexus_uuid)


@pytest.fixture(scope="module")
def local_files():
    files = [f"/tmp/disk-{base}.img" for base in ["remote"]]
    for path in files:
        subprocess.run(
            ["sudo", "sh", "-c", f"rm -f '{path}' && truncate -s 400M '{path}'"],
            check=True,
        )
    yield files
    for path in files:
        subprocess.run(["sudo", "rm", "-f", path], check=True)


@pytest.fixture
def create_pool(mayastor_mod, pool_remote, remote_instance, local_files):
    pool = mayastor_mod[remote_instance].pool_create(pool_remote, local_files[0])
    print(pool)
    yield pool


@pytest.fixture
def recreate_pool(mayastor_mod, pool_remote, remote_instance, local_files):
    pool = mayastor_mod[remote_instance].pool_create(pool_remote, local_files[0])
    print(pool)
    yield pool


@pytest.fixture
def create_replica(mayastor_mod, pool_remote, remote_instance, create_pool):
    replica = mayastor_mod[remote_instance].replica_create(
        pool_remote, "c64f5e67-e979-4933-8952-741600d3792a", megabytes(333)
    )
    print(replica)
    yield replica


@pytest.fixture
def recreate_replica(mayastor_mod, pool_remote, remote_instance, recreate_pool):
    replica = mayastor_mod[remote_instance].replica_create(
        pool_remote, "c64f5e67-e979-4933-8952-741600d3792a", megabytes(333)
    )
    print(replica)
    yield replica


@pytest.fixture(scope="module")
def pool_remote():
    yield "pool-remote"


@pytest.fixture(scope="module")
def nexus_uuid():
    yield "2c58c9f0-da89-4cb9-8097-dc67fa132493"


@pytest.fixture(scope="module")
def mayastor_instance(mayastor_mod):
    yield mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def find_nexus(mayastor_mod, local_instance):
    def find(uuid):
        for nexus in mayastor_mod[local_instance].nexus_list():
            if nexus.uuid == uuid:
                return nexus
        return None

    yield find


@retry(wait_fixed=200, stop_max_attempt_number=30)
def remote_ready(mayastor_mod, remote_instance):
    mayastor_mod[remote_instance].pool_list()
