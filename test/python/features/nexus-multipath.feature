Feature: Mayastor nexus multipath management

  Background:
    Given a local mayastor instance
    And a remote mayastor instance

  Scenario: running IO against an ANA NVMe controller
    Given a client connected to two controllers to the same namespace
    And both controllers are online
    When I start Fio
    Then I should be able to see IO flowing to one path only
