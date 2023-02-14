Feature: Rebuild history

    Background:
        Given the io-engine is running and a volume is created with more than one replicas

    Scenario: Record Full rebuild of a faulted replica
        Given a volume with multiple replicas, undergoing IO
        When one of the replica becomes faulted
        And as a result, a new replica is hosted on a different pool
        Then a full rebuild of the new replica starts
        And the replica rebuild event is recorded upon rebuild completion
        And this event history is retrievable

    Scenario: Record Partial rebuild of a faulted replica
        Given a volume with multiple replicas, undergoing IO
        When one of the replica becomes faulted
        And the replica comes back online on the same pool
        Then a partial rebuild of the replica starts
        And the replica rebuild event is recorded upon rebuild completion
        And this event history is retrievable