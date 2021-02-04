# Control Plane Deployer

Deploying all the `control plane agents` with all the trimmings is not an entirely straightforward exercise as there
are many parts to it, including the additional configuration steps to be able to run multiple `mayastor` instances
alongside each other.

The `deployer` tool facilitates this by creating a composable docker `"cluster"` which allows us to run any number of
`mayastor` instances, the `control plane agents` and any other pluggable components.

## Examples

**Using the help**
```textmate
[nix-shell:~/git/Mayastor]$ cargo run --bin deployer -- --help
  deployer --help
  agents 0.1.0
  Deployment actions

  USAGE:
      deployer <SUBCOMMAND>

  FLAGS:
      -h, --help       Prints help information
      -V, --version    Prints version information

  SUBCOMMANDS:
      help     Prints this message or the help of the given subcommand(s)
      list     List all running components
      start    Create and start all components
      stop     Stop and delete all components
```
The help can also be used on each subcommand.

**Deploying the cluster with default components**

```textmate
[nix-shell:~/git/Mayastor]$ cargo run --bin deployer -- start -m 2
    Finished dev [unoptimized + debuginfo] target(s) in 0.13s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer start`
Using options: CliArgs { action: Start(StartOptions { agents: [Node(Node), Pool(Pool), Volume(Volume)], base_image: None, jaeger: false, no_rest: false, mayastors: 2, jaeger_image: None, build: false, dns: false, show_info: false, cluster_name: "cluster" }) }
```

Notice the options which are printed out. They can be overridden - more on this later.

We could also use the `deploy` tool to inspect the components:
```textmate
[nix-shell:~/git/Mayastor]$ cargo run --bin deployer -- list
   Compiling agents v0.1.0 (/home/tiago/git/Mayastor/agents)
    Finished dev [unoptimized + debuginfo] target(s) in 5.50s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer list`
Using options: CliArgs { action: List(ListOptions { no_docker: false, format: None }) }
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                              NAMES
e775b6272009        <no image>          "/home/tiago/git/May…"   56 minutes ago      Up 56 minutes       0.0.0.0:8080-8081->8080-8081/tcp   rest
888da76b62ed        <no image>          "/home/tiago/git/May…"   56 minutes ago      Up 56 minutes                                          volume
95e8e0c45755        <no image>          "/home/tiago/git/May…"   56 minutes ago      Up 56 minutes                                          pool
bd1504c962fe        <no image>          "/home/tiago/git/May…"   56 minutes ago      Up 56 minutes                                          node
e8927a7a1cec        <no image>          "/home/tiago/git/May…"   56 minutes ago      Up 56 minutes                                          mayastor-2
9788961605c1        <no image>          "/home/tiago/git/May…"   56 minutes ago      Up 56 minutes                                          mayastor-1
ff94b234f2b9        <no image>          "/nix/store/pbd1hbhx…"   56 minutes ago      Up 56 minutes       0.0.0.0:4222->4222/tcp             nats
```

As the previous logs shows, the `rest` server ports are mapped to your host on 8080/8081.
So, we for example list existing `nodes` (aka `mayastor` instances) as such:
```textmate
[nix-shell:~/git/Mayastor]$ curl -k https://localhost:8080/v0/nodes | jq
[
  {
    "id": "mayastor-1",
    "grpcEndpoint": "10.1.0.3:10124",
    "state": "Online"
  },
  {
    "id": "mayastor-2",
    "grpcEndpoint": "10.1.0.4:10124",
    "state": "Online"
  }
]
```

To tear-down the cluster, just run the stop command:
```textmate
[nix-shell:~/git/Mayastor]$ cargo run --bin deployer -- stop
    Finished dev [unoptimized + debuginfo] target(s) in 0.13s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer stop`
Using options: CliArgs { action: Stop(StopOptions { cluster_name: "cluster" }) }
```

For more information, please refer to the help argument on every command/subcommand.

### Debugging a Service

For example, to debug the rest server, we'd create a `cluster` without the rest server:
```textmate
[nix-shell:~/git/Mayastor]$ cargo run --bin deployer -- start --no-rest --show-info
   Compiling agents v0.1.0 (/home/tiago/git/Mayastor/agents)
    Finished dev [unoptimized + debuginfo] target(s) in 5.86s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/deployer start --no-rest --show-info`
Using options: CliArgs { action: Start(StartOptions { agents: [Node(Node), Pool(Pool), Volume(Volume)], base_image: None, jaeger: false, no_rest: true, mayastors: 1, jaeger_image: None, build: false, dns: false, show_info: true, cluster_name: "cluster" }) }
[20994b0098d6] [/volume] /home/tiago/git/Mayastor/target/debug/volume -n nats.cluster:4222
[f4884e343756] [/pool] /home/tiago/git/Mayastor/target/debug/pool -n nats.cluster:4222
[fb6e78a0b6ef] [/node] /home/tiago/git/Mayastor/target/debug/node -n nats.cluster:4222
[992df686ec8a] [/mayastor] /home/tiago/git/Mayastor/target/debug/mayastor -N mayastor -g 10.1.0.3:10124 -n nats.cluster:4222
[0a5016d6c81f] [/nats] /nix/store/pbd1hbhxm17xy29mg1gibdbvbmr7gnz2-nats-server-2.1.9/bin/nats-server -DV
```

As you can see, there is no `rest` server started - go ahead and start your own!
This way you can make changes to this specific server and test them without destroying the state of the cluster.

```textmate
[nix-shell:~/git/Mayastor]$ cargo run --bin rest
    Finished dev [unoptimized + debuginfo] target(s) in 0.13s
     Running `sh /home/tiago/git/myconfigs/maya/test_as_sudo.sh target/debug/rest`
Jan 27 12:23:44.993  INFO mbus_api::mbus_nats: Connecting to the nats server nats://0.0.0.0:4222...
Jan 27 12:23:45.007  INFO mbus_api::mbus_nats: Successfully connected to the nats server nats://0.0.0.0:4222
Jan 27 12:23:45.008  INFO actix_server::builder: Starting 16 workers
Jan 27 12:23:45.008  INFO actix_server::builder: Starting "actix-web-service-0.0.0.0:8080" service on 0.0.0.0:8080
....
[nix-shell:~/git/Mayastor]$ curl -k https://localhost:8080/v0/nodes | jq
[
  {
    "id": "bob's your uncle",
    "grpcEndpoint": "10.1.0.3:10124",
    "state": "Unknown"
  }
]
```
