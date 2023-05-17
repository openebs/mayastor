Feature: Mayastor replica management

  Background:
    Given a mayastor instance "ms0"
    And an lvs backed pool "lvs-pool0"

  Scenario: creating a replica
    When the user creates an unshared replica
    Then the replica is created
    And the share state is unshared

  Scenario: creating a replica shared over "iscsi"
    When the user attempts to create a replica shared over "iscsi"
    Then the create replica command should return error
    Then replica should not be created

  Scenario: creating a replica shared over "nvmf"
    When the user creates a replica shared over "nvmf"
    Then the replica is created
    And the share state is "nvmf"

  Scenario: creating a replica with a name that already exists
    Given a replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user creates a replica with a name that already exists
    Then the create replica command should fail
    And replica should be present

  Scenario: creating a replica with a uuid that already exists
    Given a replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user creates a replica with a uuid that already exists
    Then the create replica command should fail
    And replica should be present

  Scenario: listing replicas
    Given a replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    And a replica "replica-2" with uuid "11ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replicas
    Then "replica-1" should appear in the output list
    And "replica-2" should appear in the output list

  Scenario: listing replicas by replica name
    Given a replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    And a replica "replica-2" with uuid "11ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replica by replica name "replica-1"
    Then "replica-1" should appear in the output list based on replica name
    And only 1 replicas should be present in the output list based on replica name

  Scenario: listing replicas by pool name on a pool with replicas
    Given a replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replicas by pool name "lvs-pool0"
    Then "replica-1" should appear in the output list based on pool name
    And only 1 replicas should be present in the output list based on pool name

  Scenario: listing replicas by pool name with a non-existent pool name
    Given a replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replicas by pool name "lvs-pool1"
    Then only 0 replicas should be present in the output list based on pool name

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

  Scenario: unsharing a shared replica
    Given a replica shared over "nvmf"
    When the user unshares the replica
    Then the share state should change to unshared

  Scenario: unsharing a replica that is not shared
    Given a replica that is unshared
    When the user unshares the replica
    Then the share state should remain in unshared

  Scenario: destroying a replica
    Given a replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user destroys the replica
    Then the replica should be destroyed

  Scenario: destroying a replica that does not exist
    When the user destroys a replica that does not exist
    Then the replica destroy command should fail

  Scenario: writing to a shared replica
    Given a replica shared over "nvmf"
    When the user writes to the replica
    Then the write operation should succeed

  Scenario: reading from a shared replica
    Given a replica shared over "nvmf"
    When the user reads from the replica
    Then the read operation should succeed
