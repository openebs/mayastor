source_root=/nix/store/jr704zplcr7mgvyjcvlhq2g2400y495j-source/


sudo gdb --directory ${source_root}module/bdev/error/ -ex "set environment RUST_TEST_THREADS=1" --args target/debug/deps/error_count-69526f859c36e89f
