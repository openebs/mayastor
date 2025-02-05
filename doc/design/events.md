# Event Bus

As part of mayastor we wanted to have event driven capabilities which can allow us to respond to certain events and perform specific actions. \
A message bus ([NATS]) had been initially used in early versions of mayastor, though since its initial use was mostly p2p, we ended up temporarily moving away from it in favour of [gRPC]. \
As a result of it, we ended up with high coupling between components, such as the io-engine and the core-agent.

With that out of the way, we still believe a message bus is a good solution for many use cases within mayastor:

1. Event driven reconcilers
2. Event accruing for metrics
3. Fault diagnostics system
4. etc

> **NOTE**: What's a message bus after all? It's a messaging system that allows applications to communicate with each other by sending and receiving messages. It acts as a broker that routes messages between senders and receivers which are loosely coupled.

## Enter NATS Jetstream

We've compared several options and ended up selecting [NATS] (again!) as the message bus for our eventing system.

"NATS has a built-in persistence engine called [Jetstream] which enables messages to be stored and replayed at a later time. Unlike NATS Core which requires you to have an active subscription to process messages as they happen, JetStream allows the NATS server to capture messages and replay them to consumers as needed. This functionality enables a different quality of service for your NATS messages, and enables fault-tolerant and high-availability configurations."

### Pros of NATS

- Always on and available (Highly Available)
- Low CPU-consuming
- Fast: A high-velocity communication bus
- High scalability
- Light-weight
- Supports wildcard-based subjects subscription

### Cons of NATS

- Fire and forget in the case of Core NATS but with JetStream it provides ‘at least once’ and ‘exactly once’ delivery guarantees
- No persistence in the Core NATS but it is possible with JetStream

---

We don't currently have a requirement for a messaging queue where order is important, nor do we rely on this information to be persistent. \
However, for optimum functionality we prefer a highly available deployment ensuring smooth operation of the event consumers.

We deploy a highly available Nats with Jetstream enabled, but with an in-memory storage configuration.
Here's how we configure via its helm chart:

```yaml
nats:
  jetstream:
    enabled: true
    memStorage:
      enabled: true
      size: "5Mi"
    fileStorage:
      enabled: false
cluster:
  enabled: true
  replicas: 3
```

## Events

Here we list the events which we're currently publishing on the event bus.

### Volume Events

| Category | Action | Source        | Description                                      |
|----------|--------|---------------|--------------------------------------------------|
| Volume   | Create | Control plane | Generated when a volume is successfully created  |
| Volume   | Delete | Control plane | Generated when a volume is successfully deleted  |

### Replica Events

| Category | Action       | Source     | Description                                      |
|----------|--------------|------------|--------------------------------------------------|
| Replica  | Create       | Data plane | Generated when a replica is successfully created |
| Replica  | Delete       | Data plane | Generated when a replica is successfully deleted |
| Replica  | StateChange  | Data plane | Created upon a change in replica state           |

### Pool Events

| Category | Action | Source     | Description                                    |
|----------|--------|------------|------------------------------------------------|
| Pool     | Create | Data plane | Generated when a pool is successfully created  |
| Pool     | Delete | Data plane | Generated when a pool is successfully deleted  |

### Nexus Events

| Category          | Action            | Source     | Description                                          |
|-------------------|-------------------|------------|------------------------------------------------------|
| Nexus             | Create            | Data plane | Created when a nexus is successfully created         |
| Nexus             | Delete            | Data plane | Created when a nexus is successfully deleted         |
| Nexus             | StateChange       | Data plane | Created upon a change in nexus state                 |
| Nexus             | RebuildBegun      | Data plane | Created when a nexus child rebuild operation begins  |
| Nexus             | RebuildEnd        | Data plane | Created when a nexus child rebuild operation ends    |
| Nexus             | AddChild          | Data plane | Created when a child is added to nexus               |
| Nexus             | RemoveChild       | Data plane | Created when a child is removed from nexus           |
| Nexus             | OnlineChild       | Data plane | Created when a nexus child becomes online            |
| Nexus             | SubsystemPause    | Data plane | Created when an I/O subsystem is paused              |
| Nexus             | SubsystemResume   | Data plane | Created when an I/O subsystem is resumed             |
| Nexus             | Init              | Data plane | Created when nexus enters into init state            |
| Nexus             | Reconfiguring     | Data plane | Created when nexus enters into reconfiguring state   |
| Nexus             | Shutdown          | Data plane | Created when a nexus is shutdown                     |

### Node Events

| Category  | Action      | Source        | Description                                  |
|-----------|-------------|---------------|----------------------------------------------|
| Node      | StateChange | Control plane | Created upon a change in node state          |

### High Availability Events

| Category           | Action      | Source        | Description                                                            |
|--------------------|-------------|---------------|------------------------------------------------------------------------|
| HighAvailability   | SwitchOver  | Control plane | Created when a volume switch over operation starts, fails or completes |

### Nvme Path Events

| Category   | Action          | Source        | Description                                             |
|------------|-----------------|---------------|---------------------------------------------------------|
| NvmePath   | NvmePathSuspect | Control plane | Created when an NVMe path enters into suspect state     |
| NvmePath   | NvmePathFail    | Control plane | Created when an NVMe path transitions into failed state |
| NvmePath   | NvmePathFix     | Control plane | Created when an NVMe controller reconnects to a nexus   |

### Host Initiator Events

| Category       | Action                | Source     | Description                                              |
|----------------|-----------------------|------------|----------------------------------------------------------|
| HostInitiator  | NvmeConnect           | Data plane | Created upon a host connection to a nexus                |
| HostInitiator  | NvmeDisconnect        | Data plane | Created upon a host disconnection to a nexus             |
| HostInitiator  | NvmeKeepAliveTimeout  | Data plane | Created upon a host keep alive timeout (KATO) on a nexus |

### IO-Engine Events

| Category          | Action          | Source     | Description                                        |
|-------------------|-----------------|------------|----------------------------------------------------|
| IoEngineCategory  | Start           | Data plane | Created when io-engine initializes                 |
| IoEngineCategory  | Shutdown        | Data plane | Created when io-engine shutdown starts             |
| IoEngineCategory  | Stop            | Data plane | Created when an io-engine is stopped               |
| IoEngineCategory  | ReactorUnfreeze | Data plane | Created when an io-engine reactor is healthy again |
| IoEngineCategory  | ReactorFreeze   | Data plane | Created when an io-engine reactor is frozen        |

### Snapshot and Clone Events

| Category  | Action | Source     | Description                                     |
|-----------|--------|------------|-------------------------------------------------|
| Snapshot  | Create | Data plane | Created when a snapshot is successfully created |
| Clone     | Create | Data plane | Created when a clone is successfully created    |

## Consumers

- [x] call-home
- [x] e2e testing
- [ ] support dump (kubectl-plugin)

[NATS]: https://nats.io/
[Jetstream]: https://docs.nats.io/nats-concepts/jetstream
[gRPC]: https://grpc.io/
