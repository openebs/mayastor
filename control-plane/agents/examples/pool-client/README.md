# Overview

The pool-client is an example of how to interact with the Pool service over a
message bus.

It performs the following operations:

1. Creates a pool
2. Lists the pools
3. Destroys the previously created pool
4. Lists the pools again

## Running the example

The pool-client example requires the following to be started inside the nix shell:

1. nats server
```bash
nats-server
```

2. node service
```bash
cargo run --bin node
```

3. mayastor - specifying the message bus endpoint and node name as "mayastor-node"
```bash
sudo ./target/debug/mayastor -n 127.0.0.1:4222 -N mayastor-node
```

4. pool service
```bash
cargo run --bin pool
```

5. pool-client
```bash
cargo run --example pool-client
```