package replica_test

import (
	"testing"

	"e2e-basic/common"
	"e2e-basic/common/junit"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	pvcName      = "replica-test-pvc"
	storageClass = "mayastor-nvmf"
)

const fioPodName = "fio"

func addUnpublishedReplicaTest() {
	// Create a PVC
	common.MkPVC(pvcName, storageClass)
	pvc, err := common.GetPVC(pvcName)
	Expect(err).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	timeout := "90s"
	pollPeriod := "1s"

	// Add another child before publishing the volume.
	uuid := string(pvc.ObjectMeta.UID)
	common.UpdateNumReplicas(uuid, 2)
	repl, err := common.GetNumReplicas(uuid)
	Expect(err).To(BeNil())
	Expect(repl).Should(Equal(int64(2)))

	// Use the PVC and wait for the volume to be published
	common.CreateFioPod(fioPodName, pvcName)
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
}

func TestReplica(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(junit.ConstructJunitFileName("replica-junit.xml"))
	RunSpecsWithDefaultAndCustomReporters(t, "Replica Test Suite",
		[]Reporter{junitReporter})

}

var _ = Describe("Mayastor replica tests", func() {
	It("should test the addition of a replica to an unpublished volume", func() {
		addUnpublishedReplicaTest()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))
	common.SetupTestEnv()
	close(done)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	common.DeletePod(fioPodName)
	common.RmPVC(pvcName, storageClass)
	common.TeardownTestEnv()
})
