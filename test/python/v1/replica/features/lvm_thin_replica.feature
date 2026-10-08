Feature: LVM thin pool, replica, snapshot and clone support

  Background:
    Given a mayastor instance "ms0"
    And an lvm pool with a thin pool

  Scenario: creating a pool with a thin pool option
    Then a thin pool logical volume exists in the volume group
    And the pool disks report the thinpool option verbatim

  Scenario: recreating a pool with the same thinpool option
    When the user creates the same pool again
    Then a thin pool logical volume exists in the volume group
    And the pool disks report the thinpool option verbatim

  Scenario: recreating a pool with different thinpool options
    When the user creates the same pool with different thinpool options
    Then the pool creation fails with an invalid argument error
    And the pool disks report the thinpool option verbatim
    And the thin pool keeps its original size

  Scenario: creating a pool with a thin pool larger than the disk
    Given a disk without an lvm pool
    When the user creates a pool with an oversized thin pool
    Then the pool creation fails for lack of space
    And no volume group is left on the disk
    And no physical volume is left on the disk

  Scenario: creating a thin replica on a thin pool
    When the user creates a thin replica
    Then the replica is reported as thin provisioned

  Scenario: creating a thin replica without a thin pool
    Given an lvm pool without a thin pool
    When the user creates a thin replica on the plain pool
    Then the thin replica creation fails with a precondition error

  Scenario: snapshotting a thin replica
    Given a thin replica
    When the user creates a snapshot of the replica
    Then the snapshot is listed with valid parameters
    And destroying the replica fails while the snapshot exists

  Scenario: snapshotting a thick replica
    Given a thick replica on the thin pool
    When the user creates a snapshot of the thick replica
    Then the snapshot creation fails with a precondition error

  Scenario: creating the same snapshot twice
    Given a thin replica with a snapshot
    When the user creates the same snapshot again
    Then the second creation fails as already existing

  Scenario: cloning a snapshot
    Given a thin replica with a snapshot
    When the user creates a clone from the snapshot
    Then the clone is listed as a thin clone of the snapshot
    And destroying the snapshot fails while the clone exists
    And the snapshot can be destroyed once the clone is gone

  Scenario: creating the same clone twice
    Given a thin replica with a snapshot
    When the user creates a clone from the snapshot
    And the user creates the same clone again
    Then the second creation fails as already existing
