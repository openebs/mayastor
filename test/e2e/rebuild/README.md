## Pre-requisites for this test

* A Kubernetes cluster with at least 3 nodes, with mayastor installed.

## Overview
The tests in this folder are for testing the behaviour of the rebuild feature in a Kubernetes environment.

## Test Descriptions

### Basic rebuild test
The purpose of this test is to ensure that a rebuild starts and completes successfully when the replica count in the MSV is incremented.

To run:
```bash
go test basic_rebuild_test.go
```

To run with verbose output:
```bash
go test -v basic_rebuild_test.go
```