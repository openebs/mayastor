"""Rebuild history feature tests."""

""""

The tests here use three replicas for a nexus, named 'source', 'target' and 'newchild'.
Full rebuild test - removes 'target' and adds 'newchild'.
Partial rebuild test(not implemented yet) - removes 'target', add 'target' back.

"""

import pytest
from pytest_bdd import (
    given,
    scenario,
    then,
    when,
    parsers,
)

import os
import subprocess
import time

from v1.mayastor import container_mod, mayastor_mod
from v1.volume import Volume

import grpc
import nexus_pb2 as pb


def megabytes(n):
    return n * 1024 * 1024


@pytest.fixture
def nexus_state(mayastor_nexus, find_nexus, nexus_uuid):
    yield find_nexus(nexus_uuid)


@pytest.fixture(scope="module")
def local_files():
    files = [
        f"/tmp/disk-rebuild-{base}.img" for base in ["source", "target", "newchild"]
    ]
    for path in files:
        subprocess.run(
            ["sudo", "sh", "-c", f"rm -f '{path}' && truncate -s 64M '{path}'"],
            check=True,
        )
    yield
    for path in files:
        subprocess.run(["sudo", "rm", "-f", path], check=True)


@pytest.fixture(scope="module")
def source_uri(local_files):
    yield "aio:///tmp/disk-rebuild-source.img?blk_size=4096"


@pytest.fixture(scope="module")
def target_uri(local_files):
    yield "aio:///tmp/disk-rebuild-target.img?blk_size=4096"


@pytest.fixture(scope="module")
def new_child_uri(local_files):
    yield "aio:///tmp/disk-rebuild-newchild.img?blk_size=4096"


@pytest.fixture(scope="module")
def nexus_uuid():
    yield "2c58c9f0-da89-4cb9-8097-dc67fa132493"


@pytest.fixture(scope="module")
def nexus_name():
    yield "nexus-1"


@pytest.fixture(scope="module")
def mayastor_instance(mayastor_mod):
    yield mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def find_nexus(mayastor_instance):
    def find(uuid):
        for nexus in mayastor_instance.nexus_rpc.ListNexus(
            pb.ListNexusOptions()
        ).nexus_list:
            if nexus.uuid == uuid:
                return nexus
        return None

    yield find


@pytest.fixture
def mayastor_nexus(mayastor_instance, nexus_name, nexus_uuid, source_uri, target_uri):
    nexus = mayastor_instance.nexus_rpc.CreateNexus(
        pb.CreateNexusRequest(
            name=nexus_name,
            uuid=nexus_uuid,
            size=megabytes(64),
            children=[source_uri, target_uri],
            minCntlId=50,
            maxCntlId=59,
            resvKey=0xABCDEF0012345678,
            preemptKey=0,
        )
    )
    yield nexus
    mayastor_instance.nexus_rpc.DestroyNexus(pb.DestroyNexusRequest(uuid=nexus_uuid))


@scenario(
    "features/rebuild_history.feature", "Record Full rebuild of a faulted replica"
)
def test_record_full_rebuild_of_a_faulted_replica():
    """Record Full rebuild of a faulted replica."""


@scenario(
    "features/rebuild_history.feature", "Record Partial rebuild of a faulted replica"
)
def test_record_partial_rebuild_of_a_faulted_replica():
    """Record Partial rebuild of a faulted replica."""


@given("a volume with multiple replicas, undergoing IO")
def get_nexus(mayastor_nexus):
    pass


@given("the io-engine is running and a volume is created with more than one replicas")
def nexus_ready(mayastor_nexus):
    pass


@when("as a result, a new replica is hosted on a different pool")
def add_replica(mayastor_instance, nexus_uuid, new_child_uri):
    """add new_child_uri that has a different uri than originally removed replica"""
    mayastor_instance.nexus_rpc.AddChildNexus(
        pb.AddChildNexusRequest(uuid=nexus_uuid, uri=new_child_uri, norebuild=True)
    )


@when("one of the replica becomes faulted")
def remove_replica(mayastor_instance, nexus_uuid, target_uri):
    """ "remove target_uri from the nexus"""
    mayastor_instance.nexus_rpc.RemoveChildNexus(
        pb.RemoveChildNexusRequest(uuid=nexus_uuid, uri=target_uri)
    )


@when("the replica comes back online on the same pool")
def _():
    """the replica comes back online on the same pool."""
    raise NotImplementedError


@then("a full rebuild of the new replica starts")
def start_rebuild(mayastor_instance, nexus_uuid, new_child_uri):
    """a full rebuild of the new replica starts."""
    mayastor_instance.nexus_rpc.StartRebuild(
        pb.StartRebuildRequest(nexus_uuid=nexus_uuid, uri=new_child_uri)
    )
    time.sleep(2)


@then("a partial rebuild of the replica starts")
def _():
    """a partial rebuild of the replica starts."""
    raise NotImplementedError


@then("the replica rebuild event is recorded upon rebuild completion")
def rebuild_recorded(mayastor_instance, nexus_uuid, new_child_uri):
    response = retrieve_rebuild_history(mayastor_instance, nexus_uuid)
    history = response.records
    assert len(history) == 1


@then("this event history is retrievable")
def retrieve_rebuild_history(mayastor_instance, nexus_uuid):
    """this full replica rebuild event is recorded and is retrievable."""
    response = mayastor_instance.nexus_rpc.GetRebuildHistory(
        pb.RebuildHistoryRequest(uuid=nexus_uuid)
    )
    return response
