package replica_disconnection_test

import (
	"e2e-basic/common"
	disconnect_lib "e2e-basic/node_disconnect/lib"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var gStorageClasses []string

var env disconnect_lib.DisconnectEnv

const reject = "REJECT"
const drop = "DROP"
const run_drop = false

func TestNodeLoss(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replica disconnection tests")
}

var _ = Describe("Mayastor replica disconnection test", func() {

	It("should create a refuge node and wait for the pods to re-deploy", func() {
		disconnect_lib.DisconnectSetup()
	})

	It("should define the storage classes to use", func() {
		common.MkStorageClass("mayastor-iscsi-2", 2, "iscsi", "io.openebs.csi-mayastor")
		gStorageClasses = append(gStorageClasses, "mayastor-iscsi-2")
		common.MkStorageClass("mayastor-nvmf-2", 2, "nvmf", "io.openebs.csi-mayastor")
		gStorageClasses = append(gStorageClasses, "mayastor-nvmf-2")
	})

	It("should verify nvmf nexus behaviour when a node becomes inaccessible (iptables REJECT)", func() {
		env = disconnect_lib.Setup("loss-test-pvc-nvmf", "mayastor-nvmf-2", "fio", reject)
		env.LossTest()
		env.Teardown()
	})

	It("should verify iscsi nexus behaviour when a node becomes inaccessible (iptables REJECT)", func() {
		env = disconnect_lib.Setup("loss-test-pvc-iscsi", "mayastor-iscsi-2", "fio", reject)
		env.LossTest()
		env.Teardown()
	})

	if run_drop {
		It("should verify nvmf nexus behaviour when a node becomes inaccessible (iptables DROP)", func() {
			env = disconnect_lib.Setup("loss-test-pvc-nvmf", "mayastor-nvmf-2", "fio", drop)
			env.LossTest()
			env.Teardown()
		})

		It("should verify iscsi nexus behaviour when a node becomes inaccessible (iptables DROP)", func() {
			env = disconnect_lib.Setup("loss-test-pvc-iscsi", "mayastor-iscsi-2", "fio", drop)
			env.LossTest()
			env.Teardown()
		})
	}

	It("should verify nvmf nexus behaviour when a node becomes inaccessible when no IO is received (iptables REJECT)", func() {
		env = disconnect_lib.Setup("loss-test-pvc-nvmf", "mayastor-nvmf-2", "fio", reject)
		env.LossWhenIdleTest()
		env.Teardown()
	})

	It("should verify iscsi nexus behaviour when a node becomes inaccessible when no IO is received (iptables REJECT)", func() {
		env = disconnect_lib.Setup("loss-test-pvc-iscsi", "mayastor-iscsi-2", "fio", reject)
		env.LossWhenIdleTest()
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

	for _, sc := range gStorageClasses {
		common.RmStorageClass(sc)
	}
	disconnect_lib.DisconnectTeardown()
	common.TeardownTestEnv()
})
