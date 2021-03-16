# Mayastor E2E fio test pod
## Introduction
Derived from `dmonakhov/alpine-fio`

Arguments
 * sleep <sleep seconds>
 * segfault-after <delay seconds>
 * exitv <exit value>
 * -- <fio argument list> 

 1. fio is only run if fio arguments are specified.
 2. fio is always run as a forked process.
 3. the segfault directive takes priority over the sleep directive
 4. exitv <v> override exit value - this is to simulate failure.
 5. argument parsing is simple, invalid specifications are skipped over for example `"sleep --"` => `sleep` is skipped over, parsing resumes from `--`. Execution does not fail. 

## building
Run `./build.sh`

This builds the image `mayadata/e2e-fio` 


