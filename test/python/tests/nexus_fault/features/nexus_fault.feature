Feature: Faulted nexus I/O management

  Background:
    Given a local mayastor instance
    And a remote mayastor instance

  Scenario: a temporarily faulted nexus should not cause initiator filesystem to shutdown
    Given a single replica (remote) nexus is published via nvmf
    And the nexus is connected to a kernel initiator
    And a filesystem is placed on top of the connected device
    And the filesystem is mounted
    And a fio workload is started on top of the mounted filesystem
    When the remote mayastor instance is restarted
    And the faulted nexus is recreated
    Then the fio workload should complete gracefully
    And the initiator filesystem should not be shutdown
