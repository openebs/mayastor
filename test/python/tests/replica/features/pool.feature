Feature: Mayastor pool management

  Background:
    Given a mayastor instance "ms0"

  Scenario: creating a pool using disk with invalid block size
    When the user attempts to create a pool specifying a disk with an invalid block size
    Then the pool creation should fail

  Scenario: creating a pool with multiple disks
    When the user attempts to create a pool specifying multiple disks
    Then the pool creation should fail

  Scenario: creating a pool with an AIO disk
    When the user creates a pool specifying a URI representing an aio disk
    Then the pool should be created

  Scenario: creating a pool with a name that already exists
    Given a pool "p0"
    When the user creates a pool with the name of an existing pool
    Then the pool create command should succeed

  Scenario: listing pools
    Given a pool "p0"
    When the user lists the current pools
    Then the pool should appear in the output list

  Scenario: destroying a pool
    Given a pool "p0"
    When the user destroys the pool
    Then the pool should be destroyed

  Scenario: destroying a pool that does not exist
    When the user destroys a pool that does not exist
    Then the pool destroy command should succeed
