Feature: Mayastor nexus multipath management

  Background:
    Given a local mayastor instance
    And a remote mayastor instance

  Scenario: running IO against an ANA NVMe controller
    Given a client connected to two controllers to the same namespace
    And both controllers are online
    When I start Fio
    Then I should be able to see IO flowing to one path only

  Scenario: replace failed I/O path on demand for NVMe controller
    Given a client connected to one nexus via single I/O path
    And fio client is running against target nexus
    When the only I/O path degrades
    Then it should be possible to create a second nexus and connect it as the second path
    And it should be possible to remove the first failed I/O path
    And fio client should successfully complete with the replaced I/O path
