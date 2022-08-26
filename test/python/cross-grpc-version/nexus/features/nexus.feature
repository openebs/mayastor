Feature: Mayastor nexus management

  Background:
    Given a local mayastor instance
    And a remote mayastor instance

  Scenario: creating the same nexus with v1 gRPC call
    Given a v0 version nexus
    When creating an identical v1 version nexus
    Then the operation should fail

  Scenario: destroying a v0 version nexus with v1 gRPC call
    Given a v0 version nexus
    When destroying the nexus
    Then the nexus should be destroyed

  Scenario: destroying a v0 version nexus without first unpublishing
    Given a nexus published via "nvmf"
    When destroying the nexus without unpublishing it
    Then the nexus should be destroyed

  Scenario: destroying a v0 version nexus that does not exist
    When destroying a nexus that does not exist
    Then the operation should fail

  Scenario: listing nexuses created with v0 version using v1 gRPC call
    Given a v0 version nexus
    When listing all nexuses
    Then the nexus should appear in the output list

  Scenario: listing v0 nexus by name which does not exists
    Given a v0 version nexus "86050f0e-6914-4e9c-92b9-1237fd6d17a6"
    When the user lists the nexus with name filter as "22ca10d3-4f2b-4b95-9814-9181c025cc1a"
    Then no nexus should appear in the output list

  Scenario: removing a child from a v0 version nexus using v1 gRPC call
    Given a v0 version nexus
    When removing a child
    Then the child should be successfully removed

  Scenario: adding a child to a v0 version nexus using v1 gRPC call
    Given a v0 version nexus
    When removing a child
    And adding the removed child
    Then the child should be successfully added

  Scenario: publishing a v0 version nexus using v1 gRPC call
    Given a v0 version nexus
    When publishing a nexus via "nvmf"
    Then the nexus should be successfully published

  Scenario: unpublishing a v0 version nexus using v1 gRPC call
    Given a v0 version nexus published via "nvmf"
    When unpublishing the nexus
    Then the nexus should be successfully unpublished

  Scenario: unpublishing a v0 version nexus using v1 gRPC call that is not published
    Given an unpublished v0 version nexus
    When unpublishing the nexus
    Then the request should succeed

  Scenario: republishing a v0 version nexus with the same protocol using v1 gRPC call
    Given a v0 version nexus published via "nvmf"
    When publishing the nexus with the same protocol
    Then the nexus should be successfully published

  Scenario: republishing a v0 version nexus with a different protocol using v1 gRPC call
    Given a v0 version nexus published via "nvmf"
    When attempting to publish the nexus with a different protocol
    Then the request should fail

  Scenario: publishing a v0 version nexus with a crypto-key using v1 gRPC call
    Given an unpublished v0 version nexus
    When publishing the nexus using a crypto-key
    Then the request should succeed

