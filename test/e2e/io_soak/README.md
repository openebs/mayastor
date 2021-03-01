# IO Soak test
JIRA: MQ-25
## Abstract
Runs fio with varying duty cycles concurrently on a number of volumes for an extended duration.

## Parameters
* `e2e_io_soak_load_factor` : Number of volumes per Mayastor node, type integer
* `e2e_io_soak_replicas`    : Number of replicas for each volume, type integer
* `e2e_io_soak_duration`    : Duration of fio runs, type string 
* `e2e_io_soak_protocols`   : Share protocols to run tests with, comma separated list

`e2e_io_soak_duration` is parsed using `golangs` library function `time.ParseDuration`.
So `e2e_io_soak_duration` string is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m".
Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h". 