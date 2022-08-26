Feature: Mayastor replica management

  Background:
    Given a mayastor instance "ms0"
    And a pool "p0"

  Scenario: creating a v1 version replica with a v0 version name that already exists
    Given a v0 version replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user creates a v1 version replica with a name that already exists
    Then the create replica command should fail
    And replica should be present

  Scenario: creating a v1 version replica with a v0 version uuid that already exists
    Given a v0 version replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user creates a v1 version replica with a uuid that already exists
    Then the create replica command should fail
    And replica should be present

  Scenario: listing replicas
    Given a v0 version replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replicas with v1 gRPC call
    Then "replica-1" should appear in the output list

  Scenario: listing replicas by pool name on a pool with v0 version replicas
    Given a v0 version replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replicas by pool name "p0"
    Then "replica-1" should appear in the output list based on pool name

  Scenario: listing v0 version replicas by replica name using v1 version gRPC call
    Given a v0 version replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    And a v0 version replica "replica-2" with uuid "11ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replica by replica name "replica-1"
    Then "replica-1" should appear in the output list based on replica name
    And only 1 replicas should be present in the output list based on replica name

  Scenario: listing replicas by pool name with a non-existent pool name
    Given a v0 version replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user lists the replicas by pool name "p1"
    Then only 0 replicas should be present in the output list based on pool name

  Scenario: sharing a v0 version replica over "nvmf" using v1 gRPC call
    Given a replica that is unshared
    When the user shares the replica over "nvmf"
    Then the share state should change to "nvmf"

  Scenario: sharing a v0 version replica over "iscsi" using v1 gRPC call
    Given a replica that is unshared
    When the user attempts to share the replica over "iscsi"
    Then the share replica command should fail

  Scenario: sharing a v0 version replica that is already shared with the same protocol using v1 gRPC call
    Given a replica shared over "nvmf"
    When the user shares the replica with the same protocol
    Then the share replica command should succeed

  Scenario: sharing a v0 version replica that is already shared with a different protocol using v1 gRPC call
    Given a replica shared over "nvmf"
    When the user attempts to share the replica with a different protocol
    Then the share replica command should fail

  Scenario: unsharing a v0 version replica using v1 gRPC call
    Given a replica shared over "nvmf"
    When the user unshares the replica
    Then the share state should change to unshared

  Scenario: unsharing a v0 version replica that is not shared using v1 gRPC call
    Given a replica that is unshared
    When the user unshares the replica
    Then the share state should remain in unshared

  Scenario: destroying a v0 version replica using v1 gRPC call
    Given a v0 version replica "replica-1" with uuid "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    When the user destroys the replica
    Then the replica should be destroyed
