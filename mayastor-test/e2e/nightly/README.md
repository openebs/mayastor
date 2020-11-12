## About
Long running stress e2e tests for mayastor

To run the tests use the `test.sh` file.

When adding a test make sure to bump the timeout value suitably.

## Tests
### pvc_stress
```
Do {

Scenario: A Mayastor deployment should respond correctly to new Mayastor PVC declarations
  Given: Mayastor is deployed on a Kubernetes cluster
    And: A StorageClass resource is defined for the Mayastor CSI plugin provisioner
  When: A new, valid PVC resource which references that StorageClass is declared via a k8s client
  Then: A corresponding PV should be dynamically provisioned
    And: The reported status of the PVC and PV resources should become ‘Bound’
    And: A corresponding MayastorVoume CR should be created
    And: The reported status of the MayastorVolume should become 'healthy'

Scenario: A Mayastor deployment should respond correctly to the deletion of PVC resources

Given: A Mayastor deployment with PVs which have been dynamically provisioned by the Mayastor CSI plugin
When: An existing PVC resource is deleted via a k8s client
  And: The PVC is not mounted by a pod
  And: The PVC references a StorageClass which is provisioned by Mayastor
Then: The PVC and its corresponding PV should be removed
  And: x The MayastorVolume CR should be removed

} While (<100 cycles)
```

