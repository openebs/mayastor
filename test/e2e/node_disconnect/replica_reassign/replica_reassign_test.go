package replica_reassignment_test

import (
	"e2e-basic/common"
	disconnect_lib "e2e-basic/node_disconnect/lib"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var gStorageClass string

var env disconnect_lib.DisconnectEnv

const reject = "REJECT"

func TestReplicaReassign(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replica reassignment test")
}

var _ = Describe("Mayastor replica reassignment test", func() {

	It("should create a refuge node and wait for the pods to re-deploy", func() {
		disconnect_lib.DisconnectSetup()
	})

	It("should define the storage class to use", func() {
		common.MkStorageClass("mayastor-nvmf-2", 2, "nvmf", "io.openebs.csi-mayastor")
		gStorageClass = "mayastor-nvmf-2"
	})

	It("should verify nvmf nexus repair of volume when a node becomes inaccessible", func() {
		env = disconnect_lib.Setup("loss-test-pvc-nvmf", "mayastor-nvmf-2", "fio", reject)
		env.ReplicaReassignTest()
		env.Teardown()
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

	// ensure node is reconnected in the event of a test failure
	env.ReconnectNode(false)
	env.Teardown()

	if gStorageClass != "" {
		common.RmStorageClass(gStorageClass)
	}
	disconnect_lib.DisconnectTeardown()
	common.TeardownTestEnv()
})
