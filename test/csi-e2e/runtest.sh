#!/usr/bin/env bash
GINKGO_FLAGS="-ginkgo.v -ginkgo.progress"
go test -v -timeout=0 . ${GINKGO_FLAGS}

# Required until CAS-566
# "Mayastor volumes not destroyed when PV is destroyed if storage class reclaim policy is Retain"
# is fixed.
kubectl -n mayastor delete msv --all
