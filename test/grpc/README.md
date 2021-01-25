## Setup Your Environment

To get started running these tests, first setup your environment the same as you would to build Mayastor:

```sh
modprobe {nbd,xfs,nvme_tcp}
echo 4096 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
cd MayaStor
nix-shell
```

The within the Nix shell you opened above:
```sh
cargo build --all
```

## Running All Tests

```sh
./scripts/grpc-test.sh
```

## Running Individual Tests

Test use mocha. To set this up:
```sh
cd test/grpc
npm install
```

Then to run the nexus tests:
```sh
./node_modules/mocha/bin/mocha test_nexus.js
```

## Common Problems

### Missing shared library

```
error: failed to run custom build command for `blkid-sys v0.1.4 (https://github.com/openebs/blkid?branch=blkid-sys#e153bea2)`

Caused by:
  process didn't exit successfully: `/home/tom/work/Mayastor/target/debug/build/blkid-sys-0010525277b1b8bb/build-script-build` (exit code: 101)
--- stderr
thread 'main' panicked at 'Unable to find libclang: "couldn\'t find any valid shared libraries matching: [\'libclang.so\', \'libclang-*.so\', \'libclang.so.*\'], set the `LIBCLANG_PATH` environment variable to a path where one of these files can be found (invalid: [])"', src/libcore/result.rs:1165:5
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace.
```

Failures like that above are generally because you're building outside of a Nix shell.

```sh
nix-shell
```

### Problems executing mkfs during test_replica.rs

If you see output like:
```
thread 'main' panicked at 'Failed to execute mkfs command: Os { code: 2, kind: NotFound, message: "No such file or directory" }', src/libcore/result.rs:1188:5
stack backtrace:
<lines removed>
  14: core::result::Result<T,E>::expect
             at /rustc/19bd93467617a447c22ec32cc1cf14d40cb84ccf/src/libcore/result.rs:983
  15: mayastor_csi::mount::probe_defaults
             at csi/src/mount.rs:178
  16: mayastor_csi::mount::probe_filesystems
             at csi/src/mount.rs:141
  17: mayastor_csi::main::{{closure}}
             at csi/src/server.rs:228

```

Then you should consider commenting out the line beginning `Defaults       secure_path=\"...` from your sudoers file.

The sudoers file can be edited with:
```sh
sudo visudo
```

### Panic when starting NBD disks

```
[2020-02-25T13:34:42.382283899Z DEBUG nexus_bdev.rs:289] nexus Transitioned state from Init to Online
[2020-02-25T13:34:42.382313930Z DEBUG nexus_share.rs:69] creating share handle for nexus
[2020-02-25T13:34:42.452238468Z INFO nexus_nbd.rs:215] Started nbd disk /dev/nbd0 for nexus
thread '<unnamed>' panicked at 'assertion failed: `(left == right)`
  left: `127`,
 right: `0`', mayastor/tests/common/mod.rs:222:5
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace.
^C[2020-02-25T13:35:07.424970457Z WARN env.rs:291] Received SIGNO: 2
[2020-02-25T13:35:07.425050412Z WARN env.rs:240] Mayastor stopped non-zero: 2
[2020-02-25T13:35:07.425106684Z DEBUG nvmf.rs:591] Destroying nvmf target 127.0.0.1:8420
```

There may be another Mayastor process still running. These processes can be found with `ps -aux | grep Mayastor`.

### Can't find NBD devices

If the Mayastor log contains errors such as:
```
... ERROR nbd.c:1050] open("/dev/nbd0") failed: No such file or directory
```

It's likely that you haven't enabled NBD:
```sh
sudo modprobe nbd
```

### Failed to create rbuf

```
  1) replica
       nvmf
         should write to nvmf replica:
     Error: Command /home/tom/work/fork-Mayastor/target/debug/initiator exited with code 101. Error output: [2020-02-25T18:12:21.492162280Z INFO env.rs:464] EAL arguments ["initiator", "-c 0x1", "--no-shconf", "-m 128", "--base-virtaddr=0x200000000000", "--file-prefix=mayastor_pid13357", "--huge-unlink", "--log-level=lib.eal:6", "--log-level=lib.cryptodev:0", "--log-level=user1:6", "--match-allocations"]
EAL: No available hugepages reported in hugepages-1048576kB
[2020-02-25T18:12:21.658523351Z WARN env.rs:568] rlimit configuration not implemented
[2020-02-25T18:12:21.658914386Z INFO env.rs:571] Total number of cores available: 1
[2020-02-25T18:12:21.779002302Z ERROR bdev.c:1174] create rbuf large pool failed
[2020-02-25T18:12:21.779092809Z ERROR subsystem.c:129] Init subsystem bdev failed
thread 'main' panicked at 'Failed to initialize subsystems: -1', mayastor/src/core/env.rs:546:13
```

**NB** This failure is characterised by `create rbuf large pool failed`, not by `EAL: No available hugepages reported in hugepages-1048576kB`.

You probably need to enable hugepages:
```sh
modprobe xfs
echo 4096 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
```

To make this change persist across boots:
```sh
echo "vm.nr_hugepages = 4096" >> /etc/sysctl.d/20-hugepages.conf
```

Alternatively, you may have [leftover hugepages](#leftover-hugepages).

### Blocked by filesystems leftover from previous runs

If you see failures in `test_csi.js` which look like:
```
mayastor-csi exited with code=null and signal=SIGTERM:
-----------------------------------------------------
[2020-02-25T18:24:43Z INFO  mayastor_csi] Removed stale CSI socket /tmp/mayastor_csi_test.sock
[2020-02-25T18:24:43Z WARN  mayastor_csi::mount] Filesystem xfs will not be available: Failed to mkfs xfs fs: mkfs.xfs: /tmp/fs.img appears to contain an existing filesystem (ext4).
    mkfs.xfs: Use the -f option to force overwrite.

[2020-02-25T18:24:43Z WARN  mayastor_csi::mount] Filesystem ext4 will not be available: Failed to mkfs ext4 fs: mke2fs 1.45.4 (23-Sep-2019)
    /tmp/fs.img is mounted; will not make a filesystem here!

[2020-02-25T18:24:43Z ERROR mayastor_csi::mount] Failed to cleanup default mount options files: rmdir: failed to remove '/tmp/fs_default': Device or resource busy
```

Deleting those files should resolve this problem. As sudo:
```sh
rm /tmp/fs.img
umount /tmp/fs_default
rmdir /tmp/fs_default
```

### Leftover hugepages

Sometimes hugepages can be leftover, even when all Mayastor process have been killed. These can be found in /dev/hugepages:
```sh
root@host:/# ls -l /dev/hugepages
-rw------- 1 root root 2097152 Feb 21 14:29 spdk_pid24919map_0
```

To deallocate these, simply delete the files:
```sh
root@host:/# rm /dev/hugepages/spdk_pid24919map_0
```
