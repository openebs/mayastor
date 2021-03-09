package basic_test

import (
	"e2e-basic/common"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Check that there are no artefacts left over from
// the previous 3rd party test.
func resourceCheck() {

	found, err := common.CheckForTestPods()
	if err != nil {
		logf.Log.Info("Failed to check for test pods.", "error", err)
	} else {
		Expect(found).To(BeFalse())
	}

	found, err = common.CheckForPVCs()
	if err != nil {
		logf.Log.Info("Failed to check for PVCs", err)
	}
	Expect(found).To(BeFalse())

	found, err = common.CheckForPVs()
	if err != nil {
		logf.Log.Info("Failed to check PVs", "error", err)
	}
	Expect(found).To(BeFalse())

	found, err = common.CheckForMSVs()
	if err != nil {
		logf.Log.Info("Failed to check MSVs", "error", err)
	}
	Expect(found).To(BeFalse())
}

func TestResourceCheck(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Resource Check Suite", "resource_check")
}

var _ = Describe("Mayastor resource check", func() {
	It("should have no resources allocated", func() {
		resourceCheck()
	})
})

var _ = BeforeSuite(func(done Done) {
	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")
	common.TeardownTestEnv()
})
