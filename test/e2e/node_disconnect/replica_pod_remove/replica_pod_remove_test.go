package replica_pod_remove_test

import (
	"e2e-basic/common"
	"e2e-basic/common/junit"

	disconnect_lib "e2e-basic/node_disconnect/lib"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

var env disconnect_lib.DisconnectEnv

const gStorageClass = "mayastor-nvmf-pod-remove-test-sc"

func TestMayastorPodLoss(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(junit.ConstructJunitFileName("replica-pod-remove-junit.xml"))

	RunSpecsWithDefaultAndCustomReporters(t, "Replica pod removal tests",
		[]Reporter{junitReporter})
}

var _ = Describe("Mayastor replica pod removal test", func() {
	AfterEach(func() {
		logf.Log.Info("AfterEach")
		env.Teardown() // removes fio pod and volume
		common.RmStorageClass(gStorageClass)

		// Check resource leakage.
		err := common.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should verify nvmf nexus behaviour when a mayastor pod is removed", func() {
		common.MkStorageClass(gStorageClass, 2, "nvmf", "io.openebs.csi-mayastor")
		env = disconnect_lib.Setup("loss-test-pvc-nvmf", gStorageClass, "fio-pod-remove-test")
		env.PodLossTest()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))
	common.SetupTestEnv()
	close(done)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	common.TeardownTestEnv()
})
