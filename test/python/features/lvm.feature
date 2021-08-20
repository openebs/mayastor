Feature: LVM pool and replica support

  Background:
    Given a mayastor instance "ms0"

  Scenario: Determine if the mayastor instance supports LVM pools
    When the user calls the gRPC mayastor info request
    Then the instance shall report if it supports the LVM feature

  Scenario: creating a pool with an losetup disk
    When the user creates a pool specifying a URI representing an loop disk
    Then the lvm pool should be created

  Scenario: creating a lvs pool with an AIO disk
    When the user creates a lvs pool specifying a URI representing an aio disk
    Then the lvs pool should be created

  Scenario: Listing LVM pools and LVS pools
    When a user calls the listPool() gRPC method
    Then the pool should be listed reporting the correct pooltype field

  Scenario: destroying a lvm pool with an losetup disk
    When the user destroys a pool specifying type as lvm
    Then the lvm pool should be removed

  Scenario: destroying a lvs pool with an AIO disk
    When the user destroys a pool specifying type as lvs
    Then the lvs pool should be removed
