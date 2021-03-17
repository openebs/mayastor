package replica_pod_remove_test

import (
	"e2e-basic/common"

	disruption_lib "e2e-basic/ms_pod_disruption/lib"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var env disruption_lib.DisruptionEnv

const gStorageClass = "mayastor-nvmf-pod-remove-test-sc"

func TestMayastorPodLoss(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Replica pod removal tests", "replica-pod-remove")
}

var _ = Describe("Mayastor replica pod removal test", func() {
	AfterEach(func() {
		logf.Log.Info("AfterEach")
		err := common.RmStorageClass(gStorageClass)
		Expect(err).ToNot(HaveOccurred())

		// Check resource leakage.
		err = common.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should verify nvmf nexus behaviour when a mayastor pod is removed", func() {
		err := common.MkStorageClass(gStorageClass, 2, common.ShareProtoNvmf, common.NSDefault)
		Expect(err).ToNot(HaveOccurred())
		env = disruption_lib.Setup("loss-test-pvc-nvmf", gStorageClass, "fio-pod-remove-test")
		env.PodLossTest()
		env.Teardown() // removes fio pod and volume
	})
})

var _ = BeforeSuite(func(done Done) {
	common.SetupTestEnv()
	close(done)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	common.TeardownTestEnv()
})
