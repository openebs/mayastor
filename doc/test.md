# Testing Mayastor I/O Engine

In order to test Mayastor, you'll need to be able to [**run Mayastor**][doc-run],
follow that guide for persistent hugepages & kernel module setup.

Or, for ad-hoc:

- Ensure at least 3584 2 MiB hugepages.

  ```bash
  echo 3584 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
  ```

- Ensure several kernel modules are installed:

  ```bash
  modprobe nbd xfs nvmet nvme_fabrics nvmet_rdma nvme_tcp nvme_rdma nvme_loop
  ```

## Running the test suite

Mayastor's unit tests, integration tests, and documentation tests via the conventional `cargo test`.

> **An important note**: Mayastor tests need to run on the host with [`SYS_ADMIN` capabilities][sys-admin-capabilities].
>
> You can see in `io-engine/.cargo/config` we override the test runner to execute as root, take this capability,
> then drop privileges.

> **Remember to enter the nix-shell before running any of the commands herein**

Mayastor uses [spdk][spdk] which is quite sensitive to threading. This means tests need to run one at a time:

```bash
cd io-engine
RUST_LOG=TRACE cargo test --features=io-engine-testing -- --test-threads 1 --nocapture
```

> _**NOTE**_:
> The flag --features=io-engine-testing ensures you run tests with features enabled only for testing purposes

Recent linux kernel versions no longer allow for mixing of nvme hostid's with different nvme hostnqn's, so you should
ensure your system is configured with default values.
Typically, this happens as long as you install nvme-cli on your host, but you can also ensure this by using our script:

```bash
sudo ./scripts/nvme-conf.sh --check --apply
 NVME SYSCONFDIR   : /etc/nvme
|           exists : true
|= hostid          : /etc/nvme/hostid
|           exists : true
|          content : 7b562a4c-61e8-ef10-a994-047c16834e32
|= hostnqn         : /etc/nvme/hostnqn
|           exists : true
|          content : nqn.2014-08.org.nvmexpress:uuid:7b562a4c-61e8-ef10-a994-047c16834e32


INFO: /etc/nvme is valid
```

## Testing your own SPDK version

To test your custom SPDK version please refer to
the [spdk-rs documentation](https://github.com/openebs/spdk-rs/blob/develop/README.md#custom-spdk)

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

Please do not submit pull requests with active cargo test cases that require PCIe devices to be present.

## Running the end-to-end test suite

Mayastor does more complete, end-to-end testing. It requires some extra setup.

> **TODO:** We're still writing this! Sorry! Let us know if you want us to prioritize this!

## Running the gRPC [`mocha`][mocha] test suite

There is a bit of extra setup to the [`mocha`][mocha] tests, you need to set up the node modules.

To prepare:

```bash
cd test/grpc/
npm install
```

Then, to run the tests:

```bash
./node_modules/mocha/bin/mocha test_csi.js
```

## Running the BDD test suite

> **TODO:** We're still writing this! Sorry! Let us know if you want us to prioritize this!

There is a bit of extra setup to the gRPC tests, you need to set up the python venv.

To prepare:

```bash
./test/python/setup.sh
```

Then, to run the tests:

```bash
./scripts/pytest-tests.sh
```

[spdk]: https://spdk.io/

[doc-run]: ./run.md

[mocha]: https://mochajs.org/

[sys-admin-capabilities]: https://man7.org/linux/man-pages/man7/capabilities.7.html
