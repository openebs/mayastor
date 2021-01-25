## About
Basic volume IO tests

## Tests
### E2E: Volume IO Test (iSCSI)

Feature: Export of Mayastor Volume over iSCSI

Scenario: An application pod mounts a Mayastor Volume

Given: Mayastor is deployed on a k8s cluster
  And: One or more Storage Pools are configured and all have the status 'Healthy'
￼When: A PVC for a Mayastor-provisioned StorageClass is declared via a k8s client
  And: The StorageClass parameters section has a key named protocol with the value iscsi
  And: A Pod resource which consumes that PVC is declared via a k8s client
Then: The Pod is able to mount the volume
  And: The Pod enters the 'Running' state
  And: It can be verified that an application in the Pod can perform R/W I/O on the Mayastor Volume 

### E2E: Volume IO Test (NVMeF-TCP)

Feature: Export of Mayastor Volume over NVMeF-TCP

Scenario: An application pod can mount and use a Mayastor Volume exported over NVMF

Given: Mayastor is deployed on a k8s cluster
  And: One or more Storage Pools are configured and all have the status ‘Healthy'
￼When: A PVC for a Mayastor-provisioned StorageClass is declared via a k8s client
  And: The StorageClass parameters section has a key named protocol with the value nvmf
  And: A Pod resource which consumes the PVC is declared via a k8s client
Then: The Pod is able to mount the volume
  And: The Pod enters the 'Running' state
  And: An application in the Pod can perform R/W I/O on the Mayastor Volume 