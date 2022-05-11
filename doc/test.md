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
  modprobe nbd xfs nvmet nvme_fabrics nvmet_rdma nvme_tcp nvme_rdma nvme_loop
  ```

## Running the test suite

Mayastor's unit tests, integration tests, and documentation tests via the conventional `cargo test`.

> **An important note**: Mayastor tests need to run on the host with [`SYS_ADMIN` capabilities][sys-admin-capabilities].
>
> You can see in `mayastor/.cargo/config` we override the test runner to execute as root, take this capability,
> then drop privileges.

Mayastor uses [spdk][spdk] which is quite senistive to threading. This means tests need to run one at a time:

```bash
cargo test -- --test-threads 1
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

[spdk]: https://spdk.io/
[doc-run]: ./run.md
[mocha]: https://mochajs.org/
[sys-admin-capabilities]: https://man7.org/linux/man-pages/man7/capabilities.7.html
