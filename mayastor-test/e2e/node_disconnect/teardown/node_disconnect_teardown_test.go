package node_disconnect_teardown_test

import (
	"e2e-basic/common"
	"fmt"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const mayastor_regexp = "^mayastor-.....$"
const namespace = "mayastor"
const timeoutSeconds = 100

func disconnectTeardownTest() {
	common.RmStorageClass("mayastor-iscsi-2")
	common.RmStorageClass("mayastor-nvmf-2")

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
		err = common.WaitForPodRunningOnNode(mayastor_regexp, namespace, node.NodeName, timeoutSeconds)
		Expect(err).ToNot(HaveOccurred())
	}
}

func TestNodeLossTeardown(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Node Loss Test Teardown")
}

var _ = Describe("Mayastor disconnect setup", func() {
	It("should correctly tear down the cluster after disconnection testing", func() {
		disconnectTeardownTest()
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
