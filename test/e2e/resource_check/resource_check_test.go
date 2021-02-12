package basic_test

import (
	"e2e-basic/common"
	rep "e2e-basic/common/reporter"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// Check that there are no artefacts left over from
// the previous 3rd party test.
func resourceCheck() {

	found, err := common.CheckForTestPods()
	if err != nil {
		logf.Log.Error(err, "Failed to check for test pods.")
	} else {
		Expect(found).To(BeFalse())
	}

	found, err = common.CheckForPVCs()
	if err != nil {
		logf.Log.Error(err, "Failed to check for PVCs")
	}
	Expect(found).To(BeFalse())

	found, err = common.CheckForPVs()
	if err != nil {
		logf.Log.Error(err, "Failed to check PVs")
	}
	Expect(found).To(BeFalse())

	found, err = common.CheckForMSVs()
	if err != nil {
		logf.Log.Error(err, "Failed to check MSVs")
	}
	Expect(found).To(BeFalse())
}

func TestResourceCheck(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "Resource Check Suite", rep.GetReporters("resource_check"))
}

var _ = Describe("Mayastor resource check", func() {
	It("should have no resources allocated", func() {
		resourceCheck()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))
	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")
	common.TeardownTestEnv()
})
