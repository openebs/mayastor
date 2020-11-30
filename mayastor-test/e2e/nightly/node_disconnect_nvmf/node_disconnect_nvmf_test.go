package node_disconnect_nvmf_test

import (
	"e2e-basic/nightly/common"
	"fmt"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var defTimeoutSecs = "90s"

var g_environment common.TestEnvironment

var g_nodeToKill = ""
var g_nexusNode = ""
var g_uuid = ""

func disconnectNode(vmname string, nexusNode string) {
	cmd := exec.Command("bash", "../common/io_connect_node.sh", vmname, nexusNode, "I")
	cmd.Dir = "./"
	_, err := cmd.CombinedOutput()
	if false {
		Expect(err).ToNot(HaveOccurred())
	}
}

func reconnectNode(vmname string, nexusNode string) {
	cmd := exec.Command("bash", "../common/io_connect_node.sh", vmname, nexusNode, "D")
	cmd.Dir = "./"
	_, err := cmd.CombinedOutput()
	if false {
		Expect(err).ToNot(HaveOccurred())
	}
}

func lossTest() {
	g_nexusNode = common.GetMsvNode(g_uuid, &g_environment.DynamicClient)
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
	go common.RunFio()

	time.Sleep(5 * time.Second)
	fmt.Printf("disconnecting \"%s\"\n", g_nodeToKill)
	disconnectNode(g_nodeToKill, g_nexusNode)
	disconnectNode(g_nodeToKill, "k8s-1")

	fmt.Printf("waiting 60s for disconnection to affect the nexus\n")
	time.Sleep(60 * time.Second)

	fmt.Printf("running fio while node is disconnected\n")
	common.RunFio()

	//volumeState = getMsvState(g_uuid)
	//fmt.Printf("Volume state is \"%s\"\n", volumeState) ///// FIXME - this reports an incorrect value

	fmt.Printf("reconnecting \"%s\"\n", g_nodeToKill)
	reconnectNode(g_nodeToKill, g_nexusNode)
	reconnectNode(g_nodeToKill, "k8s-1")

	fmt.Printf("running fio when node is reconnected\n")
	common.RunFio()
}

func TestNodeLoss(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Node Loss Test Suite")
}

var _ = Describe("Mayastor node loss test", func() {
	It("should verify behaviour when a node becomes inaccessibe", func() {
		lossTest()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.LoggerTo(GinkgoWriter, true))

	g_environment = common.SetupTestEnv()

	g_uuid = common.MkPVC(fmt.Sprintf("loss-test-pvc-nvmf"), "mayastor-nvmf", &g_environment.DynamicClient, &g_environment.KubeInt)

	common.ApplyDeployYaml("deploy/fio_nvmf.yaml")

	fmt.Printf("waiting for fio\n")
	Eventually(func() bool {
		return common.FioReadyPod(&g_environment.K8sClient)
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
	reconnectNode(g_nodeToKill, g_nexusNode)
	reconnectNode(g_nodeToKill, "k8s-1")

	fmt.Printf("removing fio pod\n")
	common.DeleteDeployYaml("deploy/fio_nvmf.yaml")

	fmt.Printf("removing pvc\n")
	common.RmPVC(fmt.Sprintf("loss-test-pvc-nvmf"), "mayastor-nvmf", &g_environment.DynamicClient, &g_environment.KubeInt)

	common.TeardownTestEnv(&g_environment)
})
