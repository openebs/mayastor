Feature: Mayastor nexus shutdown feature

Background:
    Given a published and connected nexus

Scenario: shutting down a nexus with active fio client
    Given fio client is running against target nexus
    When nexus is shutdown
    Then fio should not experience I/O errors and wait on I/O

Scenario: idempotency of nexus shutdown
    Given a shutdown nexus
    When nexus is shutdown again
    Then operation should succeed
