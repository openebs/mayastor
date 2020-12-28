package node_disconnect_lib

import (
	"e2e-basic/common"
	"fmt"
	"sort"

	. "github.com/onsi/gomega"
)

const mayastorRegexp = "^mayastor-.....$"
const moacRegexp = "^moac-..........-.....$"
const namespace = "mayastor"
const timeoutSeconds = 100

// DisconnectSetup
// Set up for disconnection tests. Ensure moac is on the refuge node but
// no mayastor instances are
func DisconnectSetup() {
	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())
	Expect(len(nodeList)).To(BeNumerically(">=", 3))

	// sort the nodes - that also means k8s-1 is the refuge on local clusters
	sort.Slice(nodeList, func(i, j int) bool { return nodeList[i].NodeName < nodeList[j].NodeName })
	refugeIndex := 0

	// Select one node to be the refuge, remove the engine=mayastor label so mayastor does not run there
	refugeNode := ""
	for i, node := range nodeList {
		if i == refugeIndex {
			refugeNode = node.NodeName
			common.UnlabelNode(refugeNode, "openebs.io/engine")
			common.LabelNode(refugeNode, "openebs.io/podrefuge=true")
		}
	}
	Expect(refugeNode).NotTo(Equal(""))

	moacOnRefugeNode := common.PodPresentOnNode(moacRegexp, namespace, refugeNode)

	// Update moac to ensure it stays on the refuge node (even if it currently is)
	fmt.Printf("apply moac node selector for node \"%s\"\n", refugeNode)
	common.ApplyNodeSelectorToDeployment("moac", namespace, "openebs.io/podrefuge", "true")

	// if not already on the refuge node
	if moacOnRefugeNode == false {
		fmt.Printf("moving moac to node \"%s\"\n", refugeNode)
		// reduce the number of moac instances to be zero
		// this seems to be needed to guarantee that moac moves to the refuge node
		var repl int32 = 0
		common.SetDeploymentReplication("moac", namespace, &repl)

		// wait for moac to disappear from the cluster
		for _, node := range nodeList {
			fmt.Printf("waiting for moac absence from %s\n", node.NodeName)
			err = common.WaitForPodAbsentFromNode(moacRegexp, namespace, node.NodeName, timeoutSeconds)
			Expect(err).ToNot(HaveOccurred())
		}

		// bring the number of moac instances back to 1
		repl = 1
		common.SetDeploymentReplication("moac", namespace, &repl)

		// wait for moac to be running on the refuge node
		fmt.Printf("waiting for moac presence on %s\n", refugeNode)
		err = common.WaitForPodRunningOnNode(moacRegexp, namespace, refugeNode, timeoutSeconds)
		Expect(err).ToNot(HaveOccurred())
	}

	// wait until all mayastor pods are in state "Running" and only on the non-refuge nodes
	fmt.Printf("waiting for mayastor absence from %s\n", refugeNode)
	err = common.WaitForPodAbsentFromNode(mayastorRegexp, namespace, refugeNode, timeoutSeconds)
	Expect(err).ToNot(HaveOccurred())

	for _, node := range nodeList {
		if node.NodeName != refugeNode {
			fmt.Printf("waiting for mayastor presence on %s\n", node.NodeName)
			err = common.WaitForPodRunningOnNode(mayastorRegexp, namespace, node.NodeName, timeoutSeconds)
			Expect(err).ToNot(HaveOccurred())
		}
	}
}

// DisconnectTeardown
// Remove the node selector modifications done in DisconnectSetup
func DisconnectTeardown() {
	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())
	Expect(len(nodeList)).To(BeNumerically(">=", 3))

	// apply/remove the labels whether present or not
	// An error will not occur if the label is already present/absent
	for _, node := range nodeList {
		common.LabelNode(node.NodeName, "openebs.io/engine=mayastor")
		common.UnlabelNode(node.NodeName, "openebs.io/podrefuge")
	}

	fmt.Printf("remove moac node affinity\n")
	common.RemoveAllNodeSelectorsFromDeployment("moac", namespace)

	// wait until all nodes have mayastor pods in state "Running"
	for _, node := range nodeList {
		fmt.Printf("waiting for mayastor presence on %s\n", node.NodeName)
		err = common.WaitForPodRunningOnNode(mayastorRegexp, namespace, node.NodeName, timeoutSeconds)
		Expect(err).ToNot(HaveOccurred())
	}
}
