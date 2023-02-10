"""Rebuild history feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)


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
def _():
    """a volume with multiple replicas, undergoing IO."""
    raise NotImplementedError  # to be implemented


@given("the io-engine is running and a volume is created with more than one replicas")
def _():
    """the io-engine is running and a volume is created with more than one replicas."""
    raise NotImplementedError  # to be implemented


@when("as a result, a new replica is hosted on a different pool")
def _():
    """as a result, a new replica is hosted on a different pool."""
    raise NotImplementedError  # to be implemented


@when("one of the replica becomes faulted")
def _():
    """one of the replica becomes faulted."""
    raise NotImplementedError  # to be implemented


@when("the replica comes back online on the same pool")
def _():
    """the replica comes back online on the same pool."""
    raise NotImplementedError  # to be implemented


@then("a full rebuild of the new replica happens")
def _():
    """a full rebuild of the new replica happens."""
    raise NotImplementedError  # to be implemented


@then("a partial rebuild of the replica happens")
def _():
    """a partial rebuild of the replica happens."""
    raise NotImplementedError


@then("this full replica rebuild event is recorded and is retrievable")
def _():
    """this full replica rebuild event is recorded and is retrievable."""
    raise NotImplementedError


@then("this partial replica rebuild event will be recorded and is retrievable")
def _():
    """this partial replica rebuild event will be recorded and is retrievable."""
    raise NotImplementedError
