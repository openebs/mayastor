## Pre-requisites for this test

* A Kubernetes cluster with at least 3 nodes, with mayastor installed.

## Overview
The tests in this folder are for testing the behaviour of the replicas in a Kubernetes environment.

## Test Descriptions

### replica test
The purpose of this test is to ensure that a replica can be correctly added to an unpublished nexus.

To run:
```bash
go test replica_test.go
```

To run with verbose output:
```bash
go test -v replica_test.go
```