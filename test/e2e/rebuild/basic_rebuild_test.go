package basic_rebuild_test

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
	pvcName      = "rebuild-test-pvc"
	storageClass = "mayastor-nvmf"
)

const ApplicationPod = "fio.yaml"

func basicRebuildTest() {
	// Create a PVC
	common.MkPVC(pvcName, storageClass)
	pvc, err := common.GetPVC(pvcName)
	Expect(err).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	timeout := "90s"
	pollPeriod := "1s"

	// Create an application pod and wait for the PVC to be bound to it.
	common.ApplyDeployYaml(ApplicationPod)
	Eventually(func() bool { return common.IsPvcBound(pvcName) }, timeout, pollPeriod).Should(Equal(true))

	uuid := string(pvc.ObjectMeta.UID)
	repl, err := common.GetNumReplicas(uuid)
	Expect(err).To(BeNil())
	Expect(repl).Should(Equal(int64(1)))

	// Wait for volume to be published before adding a child.
	// This ensures that a nexus exists when the child is added.
	Eventually(func() bool { return common.IsVolumePublished(uuid) }, timeout, pollPeriod).Should(Equal(true))

	// Add another child which should kick off a rebuild.
	common.UpdateNumReplicas(uuid, 2)
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
}

func TestRebuild(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(junit.ConstructJunitFileName("rebuild-junit.xml"))

	RunSpecsWithDefaultAndCustomReporters(t, "Rebuild Test Suite",
		[]Reporter{junitReporter})
}

var _ = Describe("Mayastor rebuild test", func() {
	It("should run a rebuild job to completion", func() {
		basicRebuildTest()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))
	common.SetupTestEnv()
	close(done)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	common.DeleteDeployYaml(ApplicationPod)
	common.RmPVC(pvcName, storageClass)
	common.TeardownTestEnv()
})
