# Testing Mayastor

In order to test Mayastor, you'll need to be able to [**run Mayastor**][doc-run],
follow that guide for persistent hugepages & kernel module setup.

Or, for ad-hoc:

- Ensure at least 512 2MB hugepages.

  ```bash
  echo 512 | sudo tee  /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
  ```

- Ensure several kernel modules are installed:

  ```bash
  modprobe nbd xfs btrfs nvmet nvme_fabrics nvmet_rdma nvme_tcp nvme_rdma nvme_loop
  ```

## Running the test suite

Mayastor's unit tests, integration tests, and documentation tests via the conventional `cargo test`.

> **An important note**: Mayastor tests need to run on the host with [`SYS_ADMIN` capabilities][sys-admin-capabilities].
>
> You can see in `mayastor/.cargo/config` we override the test runner to execute as root, take this capability,
> then drop privileges.

Mayastor uses [spdk][spdk] which is quite senistive to threading. This means tests need to run one at a time:

```bash
cd io-engine
cargo test -- --test-threads 1 --nocapture
```

## Using your own SPDK version

In order to use your own SPDK version, your SPDK tree must rebase the commit of the latest `vYY.mm.x-mayastor`
branch from the https://github.com/openebs/spdk repo.
Build SPDK with these instructions inside of your nix shell:

```bash
cd spdk-rs
git clone https://github.com/openebs/spdk
cd spdk
git checkout vYY.mm.x-mayastor
# Rebase your branch
git submodule update --init
cd -
./build_spdk.sh
```

Before you run the cargo tests again, make sure spdk-rs is rebuild:

```bash
cd ../io-engine
cargo clean -p spdk-rs
cargo test -- --test-threads 1 --nocapture
```

## Running the end-to-end test suite

Mayastor does more complete, end-to-end testing testing with [`mocha`][mocha]. It requires some extra setup.

> **TODO:** We're still writing this! Sorry! Let us know if you want us to prioritize this!

## Running the gRPC test suite

There is a bit of extra setup to the gRPC tests, you need to set up the node modules.

To prepare:

```bash
cd test/grpc/
npm install
```

Then, to run the tests:

```bash
./node_modules/mocha/bin/mocha test_csi.js
```

## Using PCIe NVMe devices in cargo tests while developing

When developing new features, testing those with real PCIe devices in the process might come in handy.
In order to do so, the PCIe device first needs to be bound to the vfio driver:

```bash
sudo PCI_ALLOWED="<PCI-ADDRESS>" ./spdk-rs/spdk/scripts/setup.sh
```

The bdev name in the cargo test case can then follow the PCIe URI pattern:

```rust
static BDEVNAME1: &str = "pcie:///<PCI-ADDRESS>";
```

After testing the device may be rebound to the NVMe driver:

```bash
sudo PCI_ALLOWED="<PCI-ADDRESS>" ./spdk-rs/spdk/scripts/setup.sh reset
```

Please do not submit pull requests with cargo test cases that require PCIe devices to be present.

[spdk]: https://spdk.io/
[doc-run]: ./run.md
[mocha]: https://mochajs.org/
[sys-admin-capabilities]: https://man7.org/linux/man-pages/man7/capabilities.7.html
