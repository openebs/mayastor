Feature: Mayastor pool management

  Background:
    Given a mayastor instance "ms0"

  Scenario: creating a pool using disk with invalid block size
    When the user attempts to create a pool specifying a disk with an invalid block size
    Then the pool creation should fail

  Scenario: creating a pool using invalid uuid
    When the user attempts to create a pool specifying an invalid uuid
    Then the pool creation should fail

  Scenario: creating a pool with multiple disks
    When the user attempts to create a pool specifying multiple disks
    Then the pool creation should fail

  Scenario: creating a pool using valid uuid
    When the user attempts to create a pool specifying a valid uuid
    Then the pool should be created with same uuid

  Scenario: creating a pool with an AIO disk
    When the user creates a pool specifying a URI representing an aio disk
    Then the pool should be created

  Scenario: creating a pool with a name that already exists
    Given a pool "p0" on "disk0"
    When the user creates a pool with the name of an existing pool
    Then the pool create command should fail

  Scenario: listing pools
    Given a pool "p0" on "disk0"
    Given a pool "p1" on "disk1"
    When the user lists the current pools
    Then the pool should appear in the output list

  Scenario: listing pool by name
    Given a pool "p0" on "disk0"
    Given a pool "p1" on "disk1"
    When the user lists the pool with name filter as p0
    Then only the pool p0 should appear in the output list

  Scenario: listing pool by name which does not exists
    Given a pool "p0" on "disk0"
    When the user lists the pool with name filter as p1
    Then no pool should not appear in the output list

  Scenario: exporting a pool removes the pool from the system
    Given a pool "p0" on "disk0"
    When the user exports the pool
    Then the pool should be exported

  Scenario: destroying a pool
    Given a pool "p0" on "disk0"
    When the user destroys the pool
    Then the pool should be destroyed

  Scenario: destroying a pool that does not exist
    When the user destroys a pool that does not exist
    Then the pool destroy command should fail

  Scenario: importing a pool
    Given an aio pool
    When the user exports the pool
    And then imports the pool
    Then the pool should be imported

  Scenario: importing a pool with invalid uuid
    Given an aio pool
    When the user exports the pool
    And then imports the pool with incorrect uuid
    Then the pool should be not imported

  Scenario: importing a pool with uuid
    Given an aio pool with uuid
    When the user exports the pool
    And then imports the pool
    Then the pool with same uuid should be imported
