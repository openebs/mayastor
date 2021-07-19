Feature: Mayastor replica management

  Background:
    Given a mayastor instance "ms0"
    And a pool "p0"

  Scenario: creating a replica
    When the user creates an unshared replica
    Then the replica is created
    And the share state is unshared

  Scenario: creating a replica shared over "iscsi"
    When the user attempts to create a replica shared over "iscsi"
    Then the create replica command should fail

  Scenario: creating a replica with a name that already exists
    Given a replica
    When the user creates a replica that already exists
    Then the create replica command should succeed

  Scenario: listing replicas
    Given a replica
    When the user lists the current replicas
    Then the replica should appear in the output list

  Scenario: sharing a replica over "nvmf"
    Given a replica that is unshared
    When the user shares the replica over "nvmf"
    Then the share state should change to "nvmf"

  Scenario: sharing a replica over "iscsi"
    Given a replica that is unshared
    When the user attempts to share the replica over "iscsi"
    Then the share replica command should fail

  Scenario: sharing a replica that is already shared with the same protocol
    Given a replica shared over "nvmf"
    When the user shares the replica with the same protocol
    Then the share replica command should succeed

  Scenario: sharing a replica that is already shared with a different protocol
    Given a replica shared over "nvmf"
    When the user attempts to share the replica with a different protocol
    Then the share replica command should fail

  Scenario: unsharing a replica
    Given a replica shared over "nvmf"
    When the user unshares the replica
    Then the share state should change to unshared

  Scenario: destroying a replica
    Given a replica
    When the user destroys the replica
    Then the replica should be destroyed

  Scenario: destroying a replica that does not exist
    When the user destroys a replica that does not exist
    Then the replica destroy command should succeed

  Scenario: listing replica stats
    Given a replica
    When the user gets replica stats
    Then the stats for the replica should be listed

  Scenario: creating a replica shared over "nvmf"
    When the user creates a replica shared over "nvmf"
    Then the replica is created
    And the share state is "nvmf"

  Scenario: listing replicas
    Given a replica
    When the user lists the current replicas
    Then the replica should appear in the output list

  Scenario: writing to a shared replica
    Given a replica shared over "nvmf"
    When the user writes to the replica
    Then the write operation should succeed

  Scenario: reading from a shared replica
    Given a replica shared over "nvmf"
    When the user reads from the replica
    Then the read operation should succeed

  Scenario: recreating a replica
    Given a replica
    When the user attempts to recreate the existing replica
    Then the old data should have been reset
