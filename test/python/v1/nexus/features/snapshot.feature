Feature: Mayastor nexus snapshot management

Scenario: creating a snapshot for published healthy one replica nexus with active I/O
  Given a published one replica nexus
  Given fio client is performing I/O on the published nexus
  When a nexus snapshot is created
  Then successful status for replicas should be returned to the caller
  And snapshot should be created on all nexus replicas
  And fio should complete without I/O errors
