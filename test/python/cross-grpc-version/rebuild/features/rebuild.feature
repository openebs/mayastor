Feature: Nexus rebuild functionality

  Background:
    Given a mayastor instance
    And a v0 version nexus with a source child device

  Scenario: running rebuild
    When a target child is added to the nexus
    And the rebuild operation is started
    Then the nexus state is DEGRADED
    And the source child state is ONLINE
    And the target child state is DEGRADED
    And the rebuild state is "running"
    And the rebuild count is 1

  Scenario: stopping rebuild
    When a target child is added to the nexus
    And the rebuild operation is started
    And the rebuild operation is then stopped
    Then the nexus state is DEGRADED
    And the source child state is ONLINE
    And the target child state is DEGRADED
    And the rebuild state is undefined
    And the rebuild count is 0

  Scenario: pausing rebuild
    When a target child is added to the nexus
    And the rebuild operation is started
    And the rebuild operation is then paused
    And the rebuild statistics are requested
    Then the nexus state is DEGRADED
    And the source child state is ONLINE
    And the target child state is DEGRADED
    And the rebuild state is "paused"
    And the rebuild statistics counter "blocks_total" is non-zero
    And the rebuild statistics counter "blocks_recovered" is non-zero
    And the rebuild statistics counter "progress" is non-zero
    And the rebuild statistics counter "tasks_total" is non-zero
    And the rebuild statistics counter "tasks_active" is zero

  Scenario: resuming rebuild
    When a target child is added to the nexus
    And the rebuild operation is started
    And the rebuild operation is then paused
    And the rebuild operation is then resumed
    Then the nexus state is DEGRADED
    And the source child state is ONLINE
    And the target child state is DEGRADED
    And the rebuild state is "running"
    And the rebuild count is 1

  Scenario: setting a child ONLINE
    When a target child is added to the nexus
    And the target child is set OFFLINE
    And the target child is then set ONLINE
    Then the nexus state is DEGRADED
    And the source child state is ONLINE
    And the target child state is DEGRADED
    And the rebuild state is "running"
    And the rebuild count is 1

  Scenario: setting a child OFFLINE
    When a target child is added to the nexus
    And the rebuild operation is started
    And the target child is set OFFLINE
    Then the nexus state is DEGRADED
    And the source child state is ONLINE
    And the target child state is DEGRADED
    And the rebuild state is undefined
    And the rebuild count is 0