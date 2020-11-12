#!/usr/bin/env bash

# For stress tests the default go test timeout of 10 minutes may be
# insufficient.
# We start with a timeout value of 0 and bump up the value by addsing
# the number of seconds for each test.
timeout=0
#pvc_stress run duration is around 7 minutes, add 10 minutes to handle
#unexpected delays.
timeout=$(( timeout + 600 ))

go test ./... --timeout "${timeout}s"
