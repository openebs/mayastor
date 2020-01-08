# Monitoring extension for MayaStor

Currently it shows two graphs: iops and bandwidth for arbitrary replica.
The monitoring stack consists of:

* telegraf: gathering stats from mayastor REST API endpoint
* influxdb: database for the stats
* grafana: graphical frontend

Note that this is just a proof of concept for showing "something" at
events like KubeCon. Monitoring for MayaStor will need to be designed
from scratch at some point in future when requirements are clear.

Metrics in influxDB don't reside on persistent volume so when the pod
is restarted, all saved measurements are gone.

# Deployment

We assume that mayastor (including moac) has been already deployed to
`mayastor` namespace.

1.  Create configmap holding configuration files for grafana:
    ```bash
    kubectl -n mayastor create configmap grafana-config \
        --from-file=datasources.yaml=grafana/datasources.yaml \
        --from-file=dashboards.yaml=grafana/dashboards.yaml \
        --from-file=mayastor-dashboard.json=grafana/mayastor-dashboard.json
    ```

2.  Create configmap holding configuration of telegraf:
    ```bash
    kubectl create -f telegraf-config.yaml
    ```

3.  Deploy all three components: telegraf, influxdb and grafana:
    ```bash
    kubectl create -f monitor-deployment.yaml
    ```

4.  Get port of grafana to be used for external access (in this case 30333):
    ```bash
    kubectl -n mayastor get svc
    ```
    ```
    NAME      TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)        AGE
    grafana   NodePort    10.0.0.88    <none>        80:30333/TCP   3m10s
    moac      ClusterIP   10.0.0.184   <none>        4000/TCP       113m
    ```

5.  Put URL in following form to your web browser:
    `http://<cluster-node-ip>:<external-port>/` (user/password is "admin").
    Choose mayastor dashboard.
