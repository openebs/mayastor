Feature: LVM replica support

  Background:
    Given a mayastor instance "ms0"
    And an LVM VG backed pool called "lvmpool"

  Scenario: Creating an lvm volume on an imported lvm volume group
    When a user calls the createreplica on pool "lvmpool"
    Then an lv should be created on the lvmpool

  Scenario: Creating an lvm volume on a pool identified by name
    When a user calls the createreplica with the pool name instead of its uuid
    Then an lv should be created on the lvmpool

  Scenario: Getting io stats of an lvm replica
    Given an LVM backed replica with a name of its own
    When a user calls get replica io stats
    Then the lvm replica is listed with its own name and pool

  Scenario: Getting pool io stats while an lvm pool exists
    Given an LVS pool
    And an LVM backed replica
    And the lvm replica has been written to over nvmf
    When a user calls get pool io stats
    Then the lvm pool and the lvs pool are both reported
    And the lvm pool stats are the total of its replica stats

  Scenario: Destroying a replica backed by lvm pool
    Given an LVM backed replica
    When a user calls destroy replica
    Then the replica gets destroyed

  Scenario: Listing replicas from either an LVS or LVM pool
    Given an LVS pool with a replica
    And an LVM backed replica
    When a user calls list replicas
    Then all replicas should be listed

  Scenario: Importing an lvm pool whose replica is inactive
    Given an LVM backed replica
    When the pool is exported and its volumes are deactivated
    And the user imports the pool again
    Then the replica is active and listed again
