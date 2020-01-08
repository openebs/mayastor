# Monitoring extension for MayaStor

It consists of monitor script written in NodeJS, which continuously
retrieves stats data from moac REST API server and saves them to
influxDB. From there the data can be taken and displayed by Grafana.

Note that this is just a proof of concept for showing nice pictures to
folks at KubeCons. Monitoring for MayaStor will need to be designed
from scratch at some point in future when requirements are clear.

Metrics in influxDB don't reside on persistent volume so when the pod
is restarted, the history of metrics is gone.

# Deployment

We assume that mayastor (including moac) has been already deployed to
`mayastor` namespace.

1.  Create grafana credentials used to login to web UI.
    ```bash
    kubectl -n mayastor create secret generic grafana-creds \
      --from-literal=GF_SECURITY_ADMIN_USER=admin \
      --from-literal=GF_SECURITY_ADMIN_PASSWORD=admin
    ```

2.  Create config map holding configuration files for grafana:
    ```bash
    kubectl -n mayastor create configmap grafana-config \
        --from-file=datasources.yaml=grafana/datasources.yaml \
        --from-file=dashboards.yaml=grafana/dashboards.yaml \
        --from-file=mayastor-dashboard.json=grafana/mayastor-dashboard.json
    ```

3.  Deploy monitor script, influx and grafana:
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
