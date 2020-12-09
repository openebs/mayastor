package node_disconnect_setup_test

import (
	"e2e-basic/common"
	"fmt"
	"sort"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const mayastor_regexp = "^mayastor-.....$"
const moac_regexp = "^moac-..........-.....$"
const namespace = "mayastor"
const timeoutSeconds = 100

// Set up for disconnection tests. Ensure moac is on the refuge node but
// no mayastor instances are
func disconnectSetupTest() {
	// ensure we are using 2 replicas
	common.MkStorageClass("mayastor-iscsi-2", 2, "iscsi", "io.openebs.csi-mayastor")
	common.MkStorageClass("mayastor-nvmf-2", 2, "nvmf", "io.openebs.csi-mayastor")

	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())
	Expect(len(nodeList) >= 3)

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
	Expect(refugeNode != "")

	moacOnRefugeNode := common.PodPresentOnNode(moac_regexp, namespace, refugeNode)

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
			err = common.WaitForPodAbsentFromNode(moac_regexp, namespace, node.NodeName, timeoutSeconds)
			Expect(err).ToNot(HaveOccurred())
		}

		// bring the number of moac instances back to 1
		repl = 1
		common.SetDeploymentReplication("moac", namespace, &repl)

		// wait for moac to be running on the refuge node
		fmt.Printf("waiting for moac presence on %s\n", refugeNode)
		err = common.WaitForPodRunningOnNode(moac_regexp, namespace, refugeNode, timeoutSeconds)
		Expect(err).ToNot(HaveOccurred())
	}

	// wait until all mayastor pods are in state "Running" and only on the non-refuge nodes
	fmt.Printf("waiting for mayastor absence from %s\n", refugeNode)
	err = common.WaitForPodAbsentFromNode(mayastor_regexp, namespace, refugeNode, timeoutSeconds)
	Expect(err).ToNot(HaveOccurred())

	for _, node := range nodeList {
		if node.NodeName != refugeNode {
			fmt.Printf("waiting for mayastor presence on %s\n", node.NodeName)
			err = common.WaitForPodRunningOnNode(mayastor_regexp, namespace, node.NodeName, timeoutSeconds)
			Expect(err).ToNot(HaveOccurred())
		}
	}
}

func TestNodeLossSetup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Node Loss Test Setup")
}

var _ = Describe("Mayastor disconnect setup", func() {
	It("should correctly set up the cluster for disconnection testing", func() {
		disconnectSetupTest()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.LoggerTo(GinkgoWriter, true))

	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")

	common.TeardownTestEnv()
})
