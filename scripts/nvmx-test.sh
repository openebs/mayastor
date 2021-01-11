#!/bin/env bash
cd mayastor || exit
cargo test --lib -- --nocapture --test-threads=1
cargo test --test block_device_nvmf -- --nocapture --test-threads=1
