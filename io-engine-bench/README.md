# Benchmarks

This is where we keep benchmarks to various io-engine operations, example: how long does it take to create a nexus.

## Prerequisites
It's recommended to run the benchmarks from this folder because:
1. we have a cargo runner to run it as root (required by the in-binary benchmark)

2. we have code in the cargo runner to chown the results to $USER and copy them back and forth
from the `io-engine-bench/results/criterion` location and the cargo target location.

## Examples
### To run the benchmark in release:
> RUST_LOG=disable cargo bench -p io-engine-bench

### To run the benchmark in debug:
> RUST_LOG=disable $RUST_NIGHTLY_PATH/bin/cargo bench -p io-engine-bench --profile=dev

## Results
The criterion results are placed in the cargo target folder. You may direct your browser to this location to inspect the results.

For "persistence", if the path `io-engine-bench/results/` exists, the benchmarks are moved to that location.
