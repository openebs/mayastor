package basic_rebuild_test

import (
	"testing"

	"e2e-basic/common"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	podName      = "rebuild-test-fio"
	pvcName      = "rebuild-test-pvc"
	storageClass = "rebuild-test-nvmf"
)

func basicRebuildTest() {
	err := common.MkStorageClass(storageClass, 1, common.ShareProtoNvmf, common.NSDefault)
	Expect(err).ToNot(HaveOccurred(), "Creating storage class %s", storageClass)

	// Create a PVC
	common.MkPVC(common.DefaultVolumeSizeMb, pvcName, storageClass, common.VolFileSystem, common.NSDefault)
	pvc, err := common.GetPVC(pvcName, common.NSDefault)
	Expect(err).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	timeout := "90s"
	pollPeriod := "1s"

	// Create an application pod and wait for the PVC to be bound to it.
	_, err = common.CreateFioPod(podName, pvcName, common.VolFileSystem, common.NSDefault)
	Expect(err).ToNot(HaveOccurred(), "Failed to create rebuild test fio pod")
	Eventually(func() bool { return common.IsPvcBound(pvcName, common.NSDefault) }, timeout, pollPeriod).Should(Equal(true))

	uuid := string(pvc.ObjectMeta.UID)
	repl, err := common.GetNumReplicas(uuid)
	Expect(err).To(BeNil())
	Expect(repl).Should(Equal(int64(1)))

	// Wait for volume to be published before adding a child.
	// This ensures that a nexus exists when the child is added.
	Eventually(func() bool { return common.IsVolumePublished(uuid) }, timeout, pollPeriod).Should(Equal(true))

	// Add another child which should kick off a rebuild.
	err = common.UpdateNumReplicas(uuid, 2)
	Expect(err).ToNot(HaveOccurred(), "Update the number of replicas")
	repl, err = common.GetNumReplicas(uuid)
	Expect(err).To(BeNil())
	Expect(repl).Should(Equal(int64(2)))

	// Wait for the added child to show up.
	Eventually(func() int { return common.GetNumChildren(uuid) }, timeout, pollPeriod).Should(BeEquivalentTo(2))

	getChildrenFunc := func(uuid string) []common.NexusChild {
		children, err := common.GetChildren(uuid)
		if err != nil {
			panic("Failed to get children")
		}
		Expect(len(children)).Should(Equal(2))
		return children
	}

	// Check the added child and nexus are both degraded.
	Eventually(func() string { return getChildrenFunc(uuid)[1].State }, timeout, pollPeriod).Should(BeEquivalentTo("CHILD_DEGRADED"))
	Eventually(func() (string, error) { return common.GetNexusState(uuid) }, timeout, pollPeriod).Should(BeEquivalentTo("NEXUS_DEGRADED"))

	// Check everything eventually goes healthy following a rebuild.
	Eventually(func() string { return getChildrenFunc(uuid)[0].State }, timeout, pollPeriod).Should(BeEquivalentTo("CHILD_ONLINE"))
	Eventually(func() string { return getChildrenFunc(uuid)[1].State }, timeout, pollPeriod).Should(BeEquivalentTo("CHILD_ONLINE"))
	Eventually(func() (string, error) { return common.GetNexusState(uuid) }, timeout, pollPeriod).Should(BeEquivalentTo("NEXUS_ONLINE"))
	err = common.DeletePod(podName, common.NSDefault)
	Expect(err).ToNot(HaveOccurred(), "Deleting rebuild test fio pod")
	common.RmPVC(pvcName, storageClass, common.NSDefault)
	err = common.RmStorageClass(storageClass)
	Expect(err).ToNot(HaveOccurred(), "Deleting storage class %s", storageClass)
}

func TestRebuild(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Rebuild Test Suite", "rebuild")
}

var _ = Describe("Mayastor rebuild test", func() {

	BeforeEach(func() {
		// Check ready to run
		err := common.BeforeEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		// Check resource leakage.
		err := common.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should run a rebuild job to completion", func() {
		basicRebuildTest()
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
