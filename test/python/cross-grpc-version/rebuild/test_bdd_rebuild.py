import pytest
from pytest_bdd import given, scenario, then, when, parsers
from retrying import retry

import os
import subprocess
import time

from common.mayastor import container_mod, mayastor_mod
from common.volume import Volume
from v1.mayastor import container_mod as v1_container_mod
from v1.mayastor import mayastor_mod as v1_mayastor_mod

from v1.volume import Volume

import grpc
import mayastor_pb2 as pb
import nexus_pb2 as nexus_pb


def megabytes(n):
    return n * 1024 * 1024


@pytest.fixture(scope="module")
def local_files():
    files = [f"/tmp/disk-rebuild-{base}.img" for base in ["source", "target"]]
    for path in files:
        subprocess.run(
            ["sudo", "sh", "-c", f"rm -f '{path}' && truncate -s 64M '{path}'"],
            check=True,
        )
    yield
    for path in files:
        subprocess.run(["sudo", "rm", "-f", path], check=True)


@pytest.fixture(scope="module")
def nexus_uuid():
    yield "2c58c9f0-da89-4cb9-8097-dc67fa132493"


@pytest.fixture(scope="module")
def source_uri(local_files):
    yield "aio:///tmp/disk-rebuild-source.img?blk_size=4096"


@pytest.fixture(scope="module")
def target_uri(local_files):
    yield "aio:///tmp/disk-rebuild-target.img?blk_size=4096"


def find_child(nexus, uri):
    for child in nexus.children:
        if child.uri == uri:
            return child
    return None


def convert_nexus_state(state):
    STATES = {
        "UNKNOWN": nexus_pb.NexusState.NEXUS_UNKNOWN,
        "ONLINE": nexus_pb.NexusState.NEXUS_ONLINE,
        "DEGRADED": nexus_pb.NexusState.NEXUS_DEGRADED,
        "FAULTED": nexus_pb.NexusState.NEXUS_FAULTED,
    }
    return STATES[state]


def convert_child_state(state):
    STATES = {
        "UNKNOWN": pb.ChildState.CHILD_UNKNOWN,
        "ONLINE": pb.ChildState.CHILD_ONLINE,
        "DEGRADED": pb.ChildState.CHILD_DEGRADED,
        "FAULTED": pb.ChildState.CHILD_FAULTED,
    }
    return STATES[state]


@pytest.fixture(scope="module")
def v0_mayastor_instance(mayastor_mod):
    yield mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def v1_mayastor_instance(v1_mayastor_mod):
    yield v1_mayastor_mod["ms0"]


def convert_child_action(state):
    ACTIONS = {
        "OFFLINE": nexus_pb.ChildAction.Offline,
        "ONLINE": nexus_pb.ChildAction.Online,
    }
    return ACTIONS[state]


def lookup_nexus(v1_mayastor_instance, nexus_uuid):
    for nexus in v1_mayastor_instance.nexus_rpc.ListNexus(pb.Null()).nexus_list:
        if nexus.uuid == nexus_uuid:
            return nexus
    return None


def lookup_nexus_child(v1_mayastor_instance, nexus_uuid, child_uri):
    nexus = lookup_nexus(v1_mayastor_instance, nexus_uuid)
    if nexus is None:
        return None
    for child in nexus.children:
        if child.uri == child_uri:
            return child
    return None


@retry(wait_fixed=100, stop_max_attempt_number=5)
def wait_child_state(v1_mayastor_instance, nexus_uuid, child_uri, state):
    child = lookup_nexus_child(v1_mayastor_instance, nexus_uuid, child_uri)
    assert child is not None and child.state == convert_child_state(state)


@scenario("features/rebuild.feature", "running rebuild")
def test_running_rebuild():
    """Running rebuild."""


@scenario("features/rebuild.feature", "stopping rebuild")
def test_stopping_rebuild():
    """Stopping rebuild."""


@scenario("features/rebuild.feature", "pausing rebuild")
def test_pausing_rebuild():
    """Pausing rebuild."""


@scenario("features/rebuild.feature", "resuming rebuild")
def test_resuming_rebuild():
    """Resuming rebuild."""


@scenario("features/rebuild.feature", "setting a child ONLINE")
def test_setting_a_child_online():
    """Setting a child ONLINE."""


@scenario("features/rebuild.feature", "setting a child OFFLINE")
def test_setting_a_child_offline():
    """Setting a child OFFLINE."""


@given("a mayastor instance")
@given(parsers.parse('a mayastor instance "{name}"'))
def get_instance():
    pass


@given("a v0 version nexus")
@given("a v0 version nexus with a source child device")
def get_nexus(mayastor_nexus):
    pass


@pytest.fixture
def mayastor_nexus(v0_mayastor_instance, nexus_uuid, source_uri):
    nexus = v0_mayastor_instance.ms.CreateNexus(
        pb.CreateNexusRequest(
            uuid=nexus_uuid, size=megabytes(64), children=[source_uri]
        )
    )
    yield nexus
    v0_mayastor_instance.ms.DestroyNexus(pb.DestroyNexusRequest(uuid=nexus_uuid))


@pytest.fixture(scope="module")
def find_nexus(v0_mayastor_instance):
    def find(uuid):
        for nexus in v0_mayastor_instance.ms.ListNexus(pb.Null()).nexus_list:
            if nexus.uuid == uuid:
                return nexus
        return None

    yield find


@pytest.fixture
def nexus_state(mayastor_nexus, find_nexus, nexus_uuid):
    yield find_nexus(nexus_uuid)


@pytest.fixture
def rebuild_state(v0_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    try:
        yield v0_mayastor_instance.ms.GetRebuildState(
            pb.RebuildStateRequest(uuid=nexus_uuid, uri=target_uri)
        ).state
    except:
        yield None


@when("a target child is added to the nexus")
def add_child(v1_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    v1_mayastor_instance.nexus_rpc.AddChildNexus(
        nexus_pb.AddChildNexusRequest(uuid=nexus_uuid, uri=target_uri, norebuild=True)
    )


@when("the rebuild operation is started")
def start_rebuild(v1_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    v1_mayastor_instance.nexus_rpc.StartRebuild(
        nexus_pb.StartRebuildRequest(nexus_uuid=nexus_uuid, uri=target_uri)
    )


@when("the rebuild operation is then stopped")
def stop_rebuild(v1_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    v1_mayastor_instance.nexus_rpc.StopRebuild(
        nexus_pb.StopRebuildRequest(nexus_uuid=nexus_uuid, uri=target_uri)
    )
    time.sleep(0.5)


@when("the rebuild operation is then paused")
def pause_rebuild(v1_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    v1_mayastor_instance.nexus_rpc.PauseRebuild(
        nexus_pb.PauseRebuildRequest(nexus_uuid=nexus_uuid, uri=target_uri)
    )
    time.sleep(0.5)


@when(parsers.parse("the target child is set {state}"), target_fixture="set_child")
@when(parsers.parse("the target child is then set {state}"), target_fixture="set_child")
def set_child(v1_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri, state):
    v1_mayastor_instance.nexus_rpc.ChildOperation(
        nexus_pb.ChildOperationRequest(
            nexus_uuid=nexus_uuid, uri=target_uri, action=convert_child_action(state)
        )
    )
    # After offlining a child, it may take some time to close the device
    # and reach DEGRADED state.
    if state == "OFFLINE":
        wait_child_state(v1_mayastor_instance, nexus_uuid, target_uri, "DEGRADED")


@pytest.fixture
def rebuild_v0_statistics(v0_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    return v0_mayastor_instance.ms.GetRebuildStats(
        pb.RebuildStatsRequest(uuid=nexus_uuid, uri=target_uri)
    )


@when("the rebuild statistics are requested", target_fixture="v1_rebuild_statistics")
def v1_rebuild_statistics(v1_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    return v1_mayastor_instance.nexus_rpc.GetRebuildStats(
        nexus_pb.RebuildStatsRequest(nexus_uuid=nexus_uuid, uri=target_uri)
    )


@when("the rebuild operation is then resumed")
def resume_rebuild(v1_mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    v1_mayastor_instance.nexus_rpc.ResumeRebuild(
        nexus_pb.ResumeRebuildRequest(nexus_uuid=nexus_uuid, uri=target_uri)
    )


@then(parsers.parse("the nexus state is {expected}"))
def check_nexus_state(nexus_state, expected):
    assert nexus_state.state == convert_nexus_state(expected)


@then(parsers.parse("the source child state is {expected}"))
def check_source_child_state(nexus_state, source_uri, expected):
    child = find_child(nexus_state, source_uri)
    assert child.state == convert_child_state(expected)


@then(parsers.parse("the target child state is {expected}"))
def check_target_child_state(nexus_state, target_uri, expected):
    child = find_child(nexus_state, target_uri)
    assert child.state == convert_child_state(expected)


@then(parsers.parse('the rebuild state is "{expected}"'))
def check_rebuild_state(rebuild_state, expected):
    assert rebuild_state == expected


@then(parsers.parse("the rebuild count is {expected:d}"))
def check_rebuild_count(nexus_state, expected):
    assert nexus_state.rebuilds == expected


@then("the rebuild state is undefined")
def rebuild_state_is_undefined(rebuild_state):
    assert rebuild_state is None


@then(parsers.parse('the rebuild statistics counter "{name}" is {expected}'))
def check_rebuild_statistics_counter(rebuild_v0_statistics, name, expected):
    assert (getattr(rebuild_v0_statistics, name) == 0) == (expected == "zero")
