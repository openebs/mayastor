Feature: Explicit UUID and name for replicas

    Background:
      Given a mayastor instance
      And a data pool created on mayastor instance

    Scenario: create a replica with explicit UUID and name
      When a new replica is successfully created using UUID and name
      Then replica block device has the given name and UUID
      And replica name is returned in the response object to the caller

    Scenario: list replicas created with explicit UUIDs and names
      When a new replica is successfully created using UUID and name
      Then both UUID and name should be provided upon replicas enumeration

    Scenario: list replicas created using v1 api
      When a new replica is successfully created using v1 api
      Then replica should be successfully enumerated via new replica enumeration api
