# Helm chart for Mayastor

Helm chart isn't published yet and is used mostly internally to generate yamls in `deploy/` directory and for end2end test. But chart should be deployable from this repo with helm anyway. Command below expects that:

  * you have k8s cluster up and running with [mayastor requirements](https://mayastor.gitbook.io/introduction/quickstart/preparing-the-cluster) fulfilled (take a look at [mayastor-terraform-playground](https://github.com/mayadata-io/mayastor-terraform-playground/) (WARNING - super-pre-alpha)
  * kubectl is able to access your cluster without any arguments (i.e. you have cluster configured in config as default or your environment variable KUBECONFIG points to working kubeconfig)

```
cd /path/to/openebs/Mayastor
helm install mayastor ./chart --namespace=mayastor --create-namespace
```

To uninstall:

```
helm uninstall mayastor -n mayastor
kubectl delete namespace mayastor
```

# TODO

[ ] publish :-)

## templating

[ ] templatize namespace properly - mayastor namespace is hardcoded in yaml templates
  - use Release.Namespace
  - use Release.Name
[ ] allow pulling image from authenticated repository
[ ] allow changing image versions separately

