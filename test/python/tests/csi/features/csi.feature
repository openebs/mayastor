Feature: Mayastor CSI plugin pool management

  Background:
    Given a mayastor instance
    And a mayastor-csi instance
    And a nexus published via "nvmf"

  Scenario: stage volume request without specified volume_id
    When staging a volume with a missing volume_id
    Then the request should fail

  Scenario: stage volume request without specified staging_target_path
    When staging a volume with a missing staging_target_path
    Then the request should fail

  Scenario: stage volume request without specified volume_capability
    When staging a volume with a missing volume_capability
    Then the request should fail

  Scenario: stage volume request without specified access_mode
    When staging a volume with a volume_capability with a missing access_mode
    Then the request should fail

  Scenario: stage volume request without specified mount
    When staging a volume with a volume_capability with a missing mount
    Then the request should fail

  Scenario: stage volume request with unsupported fs_type
    When staging a volume with a volume_capability with a mount with an unsupported fs_type
    Then the request should fail

  Scenario: staging a single writer volume
    When staging an "ext4" volume as "MULTI_NODE_SINGLE_WRITER"
    Then the request should succeed

  Scenario: restaging a volume
    Given an "ext4" volume staged as "MULTI_NODE_SINGLE_WRITER"
    When staging the same volume
    Then the request should succeed

  Scenario: staging different volumes with the same staging_target_path
    Given an "ext4" volume staged as "MULTI_NODE_SINGLE_WRITER"
    When attempting to stage a different volume with the same staging_target_path
    Then the request should fail

  Scenario: staging the same volumes with a different staging_target_path
    Given an "ext4" volume staged as "MULTI_NODE_SINGLE_WRITER"
    When staging the same volume but with a different staging_target_path
    Then the request should fail

  Scenario: unstaging a single writer volume
    Given an "ext4" volume staged as "MULTI_NODE_SINGLE_WRITER"
    When unstaging the volume
    Then the request should succeed

  Scenario: publish volume request without specified target_path
    Given a staged volume
    When publishing a volume with a missing target_path
    Then the request should fail

  Scenario: publish volume request
    Given a staged volume
    When publishing a volume
    Then the request should succeed

  Scenario: republishing a volume
    Given a staged volume
    And a published volume
    When publishing the same volume
    Then the request should succeed

  Scenario: publishing the same volumes with a different target_path
    Given a staged volume
    And a published volume
    When publishing the same volume with a different target_path
    Then the request should fail

  Scenario: publishing a single writer mount volume as readonly
    Given an "ext4" volume staged as "MULTI_NODE_SINGLE_WRITER"
    When publishing the volume as "ro" should succeed

  Scenario: publishing a single writer mount volume as rw
    Given an "ext4" volume staged as "MULTI_NODE_SINGLE_WRITER"
    When publishing the volume as "rw" should succeed

  Scenario: publishing a reader only mount volume as readonly
    Given an "ext4" volume staged as "MULTI_NODE_READER_ONLY"
    When publishing the volume as "ro" should succeed

  Scenario: publishing a reader only mount volume as rw
    Given an "ext4" volume staged as "MULTI_NODE_READER_ONLY"
    When publishing the volume as "rw" should fail

  Scenario: publishing a single writer block volume as readonly
    Given a block volume staged as "MULTI_NODE_SINGLE_WRITER"
    When publishing the block volume as "ro" should succeed

  Scenario: publishing a single writer block volume as rw
    Given a block volume staged as "MULTI_NODE_SINGLE_WRITER"
    When publishing the block volume as "rw" should succeed

  Scenario: publishing a reader only block volume as readonly
    Given a block volume staged as "MULTI_NODE_READER_ONLY"
    When publishing the block volume as "ro" should succeed

  Scenario: publishing a reader only block volume as rw
    Given a block volume staged as "MULTI_NODE_READER_ONLY"
    When publishing the block volume as "rw" should fail
