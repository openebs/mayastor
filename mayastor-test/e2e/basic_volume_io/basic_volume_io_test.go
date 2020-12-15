// JIRA: CAS-505
// JIRA: CAS-506
package basic_volume_io_test

import (
	"e2e-basic/common"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var defTimeoutSecs = "90s"

func TestBasicVolumeIO(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Basic volume IO tests, nvm-of tcp and iSCSI")
}

func basicVolumeIOTest(scName string ) {
	volName := "basic-vol-io-test-" + scName
	// Create the volume
	common.MkPVC(volName, scName)

	// Create the fio Pod
	fioPodName := "fio-" + volName
	pod, err := common.CreateFioPod(fioPodName, volName)
	Expect(err).ToNot(HaveOccurred())
	Expect(pod).ToNot(BeNil())

	// Wait for the fio Pod to transition to running
	Eventually(func() bool {
		return common.IsPodRunning(fioPodName)
	},
		defTimeoutSecs,
		"1s",
	).Should(Equal(true))

	// Run the fio test
	common.RunFio(fioPodName, 20)

	// Delete the fio pod
	err = common.DeletePod(fioPodName)
	Expect(err).ToNot(HaveOccurred())

	// Delete the volume
	common.RmPVC(volName, scName)
}

var _ = Describe("Mayastor Volume IO test", func() {
	It("should verify an NVMe-oF TCP volume can process IO", func() {
		basicVolumeIOTest("mayastor-nvmf")
	})
	It("should verify an iSCSI volume can process IO", func() {
		basicVolumeIOTest("mayastor-iscsi")
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true),zap.WriteTo(GinkgoWriter)))
	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")
	common.TeardownTestEnv()
})
