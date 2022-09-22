Feature: Mayastor pool management

  Background:
    Given a mayastor instance "ms0"

  Scenario: creating a v1 version pool with a v0 version pool name that already exists
    Given a v0 version pool "p0" on "disk0"
    When the user creates a v1 version pool with the name of an existing pool
    Then the pool create command should fail

  Scenario: creating a v1 version pool with same name and different disk of existing v0 version pool
    Given a v0 version pool "p2" on "disk3"
    When the user creates a v1 version pool with the same name and different disk of an existing pool
    Then the pool create request should fail

  Scenario: listing pools created with v0 version using v1 grpc call
    Given a v0 version pool "p0" on "disk0"
    Given a v0 version pool "p1" on "disk1"
    When the user lists the current pools
    Then the pool should appear in the output list

  Scenario: listing pool by name created with v0 version using v1 grpc call
    Given a v0 version pool "p0" on "disk0"
    Given a v0 version pool "p1" on "disk1"
    When the user lists the pool with name filter as p0
    Then only the pool p0 should appear in the output list

  Scenario: listing v0 pool by name which does not exists using v1 grpc call
    Given a v0 version pool "p0" on "disk0"
    When the user lists the pool with name filter as p1
    Then no pool should appear in the output list

  Scenario: destroying a pool created with v0 version using v1 grpc call
    Given a v0 version pool "p0" on "disk0"
    When the user destroys the pool
    Then the pool should be destroyed