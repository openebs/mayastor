Feature: LVM replica support

  Background:
    Given a mayastor instance "ms0"
    And an LVM VG backed pool called "lvmpool" 

  Scenario: Creating an lvm volume on an imported lvm volume group
    When a user calls the createreplica on pool "lvmpool"
    Then a vol should be created on that pool with the given uuid as its name

  Scenario: Destroying a replica backed by lvm pool
    Given an LVM backed replica
    When a user calls destroy replica
    Then the replica gets destroyed

  Scenario: Listing replicas from either an LVS or LVM pool
    Given an LVS pool with a replica
    And an LVM backed replica
    When a user calls list replicas
    Then all replicas should be listed