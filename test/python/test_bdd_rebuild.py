import pytest
from pytest_bdd import given, scenario, then, when, parsers

import os
import subprocess
import time

from common.mayastor import mayastor_mod
from common.volume import Volume

import grpc
import mayastor_pb2 as pb


def megabytes(n):
    return n * 1024 * 1024


def find_child(nexus, uri):
    for child in nexus.children:
        if child.uri == uri:
            return child
    return None


def convert_nexus_state(state):
    STATES = {
        "UNKNOWN": pb.NexusState.NEXUS_UNKNOWN,
        "ONLINE": pb.NexusState.NEXUS_ONLINE,
        "DEGRADED": pb.NexusState.NEXUS_DEGRADED,
        "FAULTED": pb.NexusState.NEXUS_FAULTED,
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


def convert_child_action(state):
    ACTIONS = {
        "OFFLINE": pb.ChildAction.offline,
        "ONLINE": pb.ChildAction.online,
    }
    return ACTIONS[state]


@scenario("features/rebuild.feature", "running rebuild")
def test_running_rebuild():
    "Running rebuild."


@scenario("features/rebuild.feature", "stopping rebuild")
def test_stopping_rebuild():
    "Stopping rebuild."


@scenario("features/rebuild.feature", "pausing rebuild")
def test_pausing_rebuild():
    "Pausing rebuild."


@scenario("features/rebuild.feature", "resuming rebuild")
def test_resuming_rebuild():
    "Resuming rebuild."


@scenario("features/rebuild.feature", "setting a child ONLINE")
def test_setting_a_child_online():
    "Setting a child ONLINE."


@scenario("features/rebuild.feature", "setting a child OFFLINE")
def test_setting_a_child_offline():
    "Setting a child OFFLINE."


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
def source_uri(local_files):
    yield "aio:///tmp/disk-rebuild-source.img?blk_size=4096"


@pytest.fixture(scope="module")
def target_uri(local_files):
    yield "aio:///tmp/disk-rebuild-target.img?blk_size=4096"


@pytest.fixture(scope="module")
def nexus_uuid():
    yield "2c58c9f0-da89-4cb9-8097-dc67fa132493"


@pytest.fixture(scope="module")
def mayastor_instance(mayastor_mod):
    yield mayastor_mod["ms0"]


@pytest.fixture(scope="module")
def find_nexus(mayastor_instance):
    def find(uuid):
        for nexus in mayastor_instance.ms.ListNexus(pb.Null()).nexus_list:
            if nexus.uuid == uuid:
                return nexus
        return None

    yield find


@pytest.fixture
def mayastor_nexus(mayastor_instance, nexus_uuid, source_uri):
    nexus = mayastor_instance.ms.CreateNexus(
        pb.CreateNexusRequest(
            uuid=nexus_uuid, size=megabytes(64), children=[source_uri]
        )
    )
    yield nexus
    mayastor_instance.ms.DestroyNexus(pb.DestroyNexusRequest(uuid=nexus_uuid))


@pytest.fixture
def nexus_state(mayastor_nexus, find_nexus, nexus_uuid):
    yield find_nexus(nexus_uuid)


@pytest.fixture
def rebuild_state(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    try:
        yield mayastor_instance.ms.GetRebuildState(
            pb.RebuildStateRequest(uuid=nexus_uuid, uri=target_uri)
        ).state
    except:
        yield None


@given("a mayastor instance")
@given(parsers.parse('a mayastor instance "{name}"'))
def get_instance(mayastor_instance):
    pass


@given("a nexus")
@given("a nexus with a source child device")
def get_nexus(mayastor_nexus):
    pass


@when("a target child is added to the nexus")
def add_child(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    mayastor_instance.ms.AddChildNexus(
        pb.AddChildNexusRequest(uuid=nexus_uuid, uri=target_uri, norebuild=True)
    )


@when("the rebuild operation is started")
def start_rebuild(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    mayastor_instance.ms.StartRebuild(
        pb.StartRebuildRequest(uuid=nexus_uuid, uri=target_uri)
    )


@when("the rebuild operation is then paused")
def pause_rebuild(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    mayastor_instance.ms.PauseRebuild(
        pb.PauseRebuildRequest(uuid=nexus_uuid, uri=target_uri)
    )
    time.sleep(0.5)


@when("the rebuild operation is then resumed")
def resume_rebuild(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    mayastor_instance.ms.ResumeRebuild(
        pb.ResumeRebuildRequest(uuid=nexus_uuid, uri=target_uri)
    )


@when("the rebuild operation is then stopped")
def stop_rebuild(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    mayastor_instance.ms.StopRebuild(
        pb.StopRebuildRequest(uuid=nexus_uuid, uri=target_uri)
    )
    time.sleep(0.5)


@when("the rebuild statistics are requested", target_fixture="rebuild_statistics")
def rebuild_statistics(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri):
    return mayastor_instance.ms.GetRebuildStats(
        pb.RebuildStatsRequest(uuid=nexus_uuid, uri=target_uri)
    )


@when(parsers.parse("the target child is set {state}"), target_fixture="set_child")
@when(parsers.parse("the target child is then set {state}"), target_fixture="set_child")
def set_child(mayastor_instance, mayastor_nexus, nexus_uuid, target_uri, state):
    mayastor_instance.ms.ChildOperation(
        pb.ChildNexusRequest(
            uuid=nexus_uuid, uri=target_uri, action=convert_child_action(state)
        )
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


@then(parsers.parse("the rebuild count is {expected:d}"))
def check_rebuild_count(nexus_state, expected):
    assert nexus_state.rebuilds == expected


@then(parsers.parse('the rebuild state is "{expected}"'))
def check_rebuild_state(rebuild_state, expected):
    assert rebuild_state == expected


@then("the rebuild state is undefined")
def rebuild_state_is_undefined(rebuild_state):
    assert rebuild_state is None


@then(parsers.parse('the rebuild statistics counter "{name}" is {expected}'))
def check_rebuild_statistics_counter(rebuild_statistics, name, expected):
    assert (getattr(rebuild_statistics, name) == 0) == (expected == "zero")
