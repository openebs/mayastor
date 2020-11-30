Pre-requisites for this test
----------------------------

	* A 3-node cluster with nodes k8s-1 k8s-2 and k8s-3 located at
	  172.18.8.101-3 respectively
	* k8s-1 is the master node, does NOT have the label openebs.io/engine
	  to avoid having to disconnect the master node and is labelled
	  openebs.io/podrefuge=true
	* moac is deployed with the following selector to keep it on k8s-1:
	  nodeSelector:
		openebs.io/podrefuge: "true"
	  see ../common/moac-deployment-refuge.yaml
	* k8s-2 and k8s-3 are labelled openebs.io/engine=mayastor, as usual
	* the cluster is deployed using vagrant via bringup_cluster.sh and
	  KUBESPRAY_REPO is correctly defined in ../common/io_connect_node.sh
	* mayastor is installed on the cluster, with mayastor instances on
	  k8s-2 and k8s-3 only (due to the node labels)
	* the storage classes defined in ../common/storage-class-2-repl.yaml
	  have been applied (replica count of 2).
