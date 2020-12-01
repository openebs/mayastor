#!/usr/bin/env bash

# The default go test timeout of 10 minutes may be insufficient.

# We start with a timeout value of 60 seconds and bump up the value
# adding a number of seconds for each test.
timeout=60

#pvc_stress run duration is around 7 minutes for 100 iterations,
# add 8 minutes to handle variations in timing.
timeout=$(( timeout + 480 ))

#pvc_stress_fio run duration is around 11 minutes for 10 iterations,
# with fio duration set to 5 seconds.
# add 12 minutes to handle variations in timing.
timeout=$(( timeout + 720 ))

# FIXME: we want to pvc_stress before pvc_stress_fio.
go test ./... --timeout "${timeout}s"
