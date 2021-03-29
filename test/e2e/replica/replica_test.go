package replica_test

import (
	"testing"

	"e2e-basic/common"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	pvcName      = "replica-test-pvc"
	storageClass = "replica-test-nvmf"
)

const fioPodName = "fio"

func addUnpublishedReplicaTest() {
	err := common.MkStorageClass(storageClass, 1, common.ShareProtoNvmf, common.NSDefault)
	Expect(err).ToNot(HaveOccurred(), "Creating storage class %s", storageClass)

	// Create a PVC
	common.MkPVC(common.DefaultVolumeSizeMb, pvcName, storageClass, common.VolFileSystem, common.NSDefault)
	pvc, err := common.GetPVC(pvcName, common.NSDefault)
	Expect(err).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	timeout := "90s"
	pollPeriod := "1s"

	// Add another child before publishing the volume.
	uuid := string(pvc.ObjectMeta.UID)
	err = common.UpdateNumReplicas(uuid, 2)
	Expect(err).ToNot(HaveOccurred(), "Update number of replicas")
	repl, err := common.GetNumReplicas(uuid)
	Expect(err).To(BeNil())
	Expect(repl).Should(Equal(int64(2)))

	// Use the PVC and wait for the volume to be published
	_, err = common.CreateFioPod(fioPodName, pvcName, common.VolFileSystem, common.NSDefault)
	Expect(err).ToNot(HaveOccurred(), "Create fio pod")
	Eventually(func() bool { return common.IsVolumePublished(uuid) }, timeout, pollPeriod).Should(Equal(true))

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

	err = common.DeletePod(fioPodName, common.NSDefault)
	Expect(err).ToNot(HaveOccurred(), "Delete fio test pod")
	common.RmPVC(pvcName, storageClass, common.NSDefault)

	err = common.RmStorageClass(storageClass)
	Expect(err).ToNot(HaveOccurred(), "Deleting storage class %s", storageClass)
}

func TestReplica(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Replica Test Suite", "replica")
}

var _ = Describe("Mayastor replica tests", func() {

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

	It("should test the addition of a replica to an unpublished volume", func() {
		addUnpublishedReplicaTest()
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
