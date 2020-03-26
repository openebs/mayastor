#!/usr/bin/env bash
kubectl exec -it fio -- fio --name=benchtest --size=50m --filename=/volume/test --direct=1 --rw=randrw --ioengine=libaio --bs=4k --iodepth=16 --numjobs=1 --time_based --runtime=60
