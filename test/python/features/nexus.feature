Feature: Mayastor nexus management

  Background:
    Given a local mayastor instance
    And a remote mayastor instance

  Scenario: creating a nexus
    Given a list of child devices
    When creating a nexus
    Then the nexus should be created

  Scenario: creating the same nexus
    Given a nexus
    When creating an identical nexus
    Then the operation should succeed

  Scenario: creating a different nexus with the same children
    Given a nexus
    When attempting to create a new nexus
    Then the operation should fail with existing URIs in use

  Scenario: creating a nexus without children
    When attempting to create a nexus with no children
    Then the operation should fail

  Scenario: creating a nexus with missing children
    When attempting to create a nexus with a child URI that does not exist
    Then the operation should fail

  Scenario: creating a nexus from children with mixed block sizes
    When attempting to create a nexus from child devices with mixed block sizes
    Then the nexus should not be created

  Scenario: creating a nexus that is larger than its children
    Given a list of child devices
    When attempting to create a nexus with a size larger than that of any of its children
    Then the nexus should not be created

  Scenario: destroying a nexus
    Given a nexus
    When destroying the nexus
    Then the nexus should be destroyed

  Scenario: destroying a nexus without first unpublishing
    Given a nexus published via "nvmf"
    When destroying the nexus without unpublishing it
    Then the nexus should be destroyed

  Scenario: destroying a nexus that does not exist
    When destroying a nexus that does not exist
    Then the operation should succeed

  Scenario: listing nexuses
    Given a nexus
    When listing all nexuses
    Then the nexus should appear in the output list

  Scenario: removing a child from a nexus
    Given a nexus
    When removing a child
    Then the child should be successfully removed

  Scenario: adding a child to a nexus
    Given a nexus
    When removing a child
    And adding the removed child
    Then the child should be successfully added

  Scenario: publishing a nexus
    Given a nexus
    When publishing a nexus via "nvmf"
    Then the nexus should be successfully published

  Scenario: unpublishing a nexus
    Given a nexus published via "nvmf"
    When unpublishing the nexus
    Then the nexus should be successfully unpublished

  Scenario: unpublishing a nexus that is not published
    Given an unpublished nexus
    When unpublishing the nexus
    Then the request should succeed

  Scenario: republishing a nexus with the same protocol
    Given a nexus published via "nvmf"
    When publishing the nexus with the same protocol
    Then the nexus should be successfully published

  Scenario: republishing a nexus with a different protocol
    Given a nexus published via "nvmf"
    When attempting to publish the nexus with a different protocol
    Then the request should fail

  Scenario: publishing a nexus with a crypto-key
    Given an unpublished nexus
    When publishing the nexus using a crypto-key
    Then the request should succeed
