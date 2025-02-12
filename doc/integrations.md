# Integrations with other projects

| Technology             | Integration                                      | Description                                                           |
|:-----------------------|:------------------------------------------------:|:---------------------------------------------------------------------:|
| [SPDK]                 | [spdk-rs] <br> [io-engine]                       | Mayastor uses SPDK to build a high-speed low-latency storage backend  |
| [gRPC]                 | [Inter Service Communication]                    | Used as internal service communication                                |
| [etcd]                 | [Persistent Store] <br> [PStor client]           | Used as persistent configuration (not volume data)                    |
| [NATS]                 | [Event bus]                                      | Used as event bus                                                     |
| [OpenTelemetry]        | [Tracing]                                        | Tracing system for observability                                      |
| [Helm]                 | [Helm Install Guide]                             | Installs/upgrades on K8s cluster                                      |
| [Grafana]              | [Grafana Dashboards]                             | Install grafana custom dashboards with OpenEBS exported metrics       |
| [Grafana/Loki]         | [Loki Support logs]                              | Collect support logs                                                  |
| [Prometheus]           | [Monitoring]                                     | Export stats                                                          |
| [Kubernetes]           | [Install Guide]                                  | Runs on K8s                                                           |

[//]: <>  (Technology Links)
[Grafana]: https://grafana.com/
[Grafana/Loki]: https://grafana.com/oss/loki/
[SPDK]: https://spdk.io/
[gRPC]: https://grpc.io/
[etcd]: https://etcd.io/
[NATS]: https://nats.io/
[OpenTelemetry]: https://opentelemetry.io/
[Helm]: https://helm.sh/
[Prometheus]: https://prometheus.io/
[Kubernetes]: https://kubernetes.io/

[//]: <>  (Integrations Links)
[Grafana Dashboards]: https://openebs.io/docs/main/user-guides/observability#install-the-helm-chart
[Loki Support logs]: https://openebs.io/docs/user-guides/replicated-storage-user-guide/replicated-pv-mayastor/advanced-operations/supportability
[spdk-rs]: https://github.com/openebs/spdk-rs
[io-engine]: https://github.com/openebs/mayastor/blob/HEAD/doc/design/mayastor.md
[Inter Service Communication]: https://github.com/openebs/mayastor/blob/HEAD/doc/design/control-plane.md#internal-communication
[Persistent Store]: https://github.com/openebs/mayastor/blob/HEAD/doc/design/control-plane.md#persistent-store-kvstore-for-configuration-data
[PStor client]: https://github.com/openebs/mayastor-control-plane/blob/HEAD/utils/pstor/src/etcd.rs
[Event bus]: https://github.com/openebs/mayastor/blob/HEAD/doc/design/events.md
[Tracing]: https://github.com/openebs/mayastor/blob/HEAD/doc/design/control-plane.md#tracing-and-telemetry
[Helm Install Guide]: https://openebs.io/docs/quickstart-guide/installation#installation-via-helm
[Monitoring]: https://openebs.io/docs/user-guides/replicated-storage-user-guide/replicated-pv-mayastor/advanced-operations/monitoring
[Install Guide]: https://openebs.io/docs/quickstart-guide/installation
