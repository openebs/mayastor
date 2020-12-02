package node_disconnect_nvmf_test

// TODO factor out remaining code duplicated with node_disconnect_iscsi_test

import (
	"e2e-basic/common"
	"fmt"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	defTimeoutSecs = "90s"
	g_nodeToKill   = ""
	g_nexusNode    = ""
	g_uuid         = ""
)

func disconnectNode(vmname string, nexusNode string) {
	cmd := exec.Command("bash", "../../common/io_connect_node.sh", vmname, nexusNode, "DISCONNECT")
	cmd.Dir = "./"
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

func reconnectNode(vmname string, nexusNode string, checkError bool) {
	cmd := exec.Command("bash", "../../common/io_connect_node.sh", vmname, nexusNode, "RECONNECT")
	cmd.Dir = "./"
	_, err := cmd.CombinedOutput()
	if checkError {
		Expect(err).ToNot(HaveOccurred())
	}
}

func lossTest() {
	g_nexusNode = common.GetMsvNode(g_uuid)
	fmt.Printf("nexus node is \"%s\"\n", g_nexusNode)

	if g_nexusNode == "k8s-2" {
		g_nodeToKill = "k8s-3"
	} else if g_nexusNode == "k8s-3" {
		g_nodeToKill = "k8s-2"
	} else {
		fmt.Printf("Unexpected nexus node name\n")
		Expect(false)
	}
	fmt.Printf("node to kill is \"%s\"\n", g_nodeToKill)

	fmt.Printf("running spawned fio\n")
	go common.RunFio("fio", 20)

	time.Sleep(5 * time.Second)
	fmt.Printf("disconnecting \"%s\"\n", g_nodeToKill)
	disconnectNode(g_nodeToKill, g_nexusNode)
	disconnectNode(g_nodeToKill, "k8s-1")

	fmt.Printf("waiting 60s for disconnection to affect the nexus\n")
	time.Sleep(60 * time.Second)

	fmt.Printf("running fio while node is disconnected\n")
	common.RunFio("fio", 20)

	//volumeState = getMsvState(g_uuid)
	//fmt.Printf("Volume state is \"%s\"\n", volumeState) ///// FIXME - this reports an incorrect value

	fmt.Printf("reconnecting \"%s\"\n", g_nodeToKill)
	reconnectNode(g_nodeToKill, g_nexusNode, true)
	reconnectNode(g_nodeToKill, "k8s-1", true)

	fmt.Printf("running fio when node is reconnected\n")
	common.RunFio("fio", 20)
}

func TestNodeLoss(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Node Loss Test Suite")
}

var _ = Describe("Mayastor node loss test", func() {
	It("should verify behaviour when a node becomes inaccessible", func() {
		lossTest()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.LoggerTo(GinkgoWriter, true))

	common.SetupTestEnv()

	g_uuid = common.MkPVC(fmt.Sprintf("loss-test-pvc-nvmf"), "mayastor-nvmf")

	common.ApplyDeployYaml("../deploy/fio_nvmf.yaml")

	fmt.Printf("waiting for fio\n")
	Eventually(func() bool {
		return common.FioReadyPod()
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")

	// ensure node is reconnected in the event of a test failure
	fmt.Printf("reconnecting %s\n", g_nodeToKill)
	reconnectNode(g_nodeToKill, g_nexusNode, false)
	reconnectNode(g_nodeToKill, "k8s-1", false)

	fmt.Printf("removing fio pod\n")
	common.DeleteDeployYaml("../deploy/fio_nvmf.yaml")

	fmt.Printf("removing pvc\n")
	common.RmPVC(fmt.Sprintf("loss-test-pvc-nvmf"), "mayastor-nvmf")

	common.TeardownTestEnv()
})
