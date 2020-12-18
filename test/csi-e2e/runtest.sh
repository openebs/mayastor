#!/usr/bin/env bash
set -eux
cd "$(dirname "$(realpath "$0")")"
go test -v . -ginkgo.v -ginkgo.progress -timeout 0

# Required until CAS-566
# "Mayastor volumes not destroyed when PV is destroyed if storage class reclaim policy is Retain"
# is fixed.
kubectl -n mayastor delete msv --all
