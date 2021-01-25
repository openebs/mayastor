#!/usr/bin/env bash

set -e

# typical output for kubectl get pods -n mayastor is,
# collect the restart values
#NAME                    READY   STATUS    RESTARTS   AGE
#mayastor-4xg7x          1/1     Running   0          124m
#mayastor-csi-6746c      2/2     Running   0          124m
#mayastor-csi-pdwjp      2/2     Running   0          124m
#mayastor-lzr5n          1/1     Running   0          124m
restarts=$(kubectl get pods -n mayastor | grep -e mayastor -e moac | awk '{print $4}')
for num in $restarts
do
    if [ "$num" -ne "0" ]; then
        # Useful to dump what failure was seen
        kubectl get pods -n mayastor
        exit 255
    fi
done
exit 0
