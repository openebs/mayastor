// JIRA: CAS-505
// JIRA: CAS-506
package basic_volume_io_test

import (
	"e2e-basic/common"
	"e2e-basic/common/junit"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var defTimeoutSecs = "120s"

type volSc struct {
	volName string
	scName  string
}

var podNames []string
var volNames []volSc

func TestBasicVolumeIO(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(junit.ConstructJunitFileName("basic-volume-io-junit.xml"))

	RunSpecsWithDefaultAndCustomReporters(t, "Basic volume IO tests, NVMe-oF TCP and iSCSI", []Reporter{junitReporter})
}

func basicVolumeIOTest(scName string) {
	volName := "basic-vol-io-test-" + scName
	// Create the volume
	common.MkPVC(volName, scName)
	tmp := volSc{volName, scName}
	volNames = append(volNames, tmp)

	// Create the fio Pod
	fioPodName := "fio-" + volName
	pod, err := common.CreateFioPod(fioPodName, volName)
	Expect(err).ToNot(HaveOccurred())
	Expect(pod).ToNot(BeNil())
	podNames = append(podNames, fioPodName)

	// Wait for the fio Pod to transition to running
	Eventually(func() bool {
		return common.IsPodRunning(fioPodName)
	},
		defTimeoutSecs,
		"1s",
	).Should(Equal(true))

	// Run the fio test
	common.RunFio(fioPodName, 20)
	podNames = podNames[:len(podNames)-1]

	// Delete the fio pod
	err = common.DeletePod(fioPodName)
	Expect(err).ToNot(HaveOccurred())

	// Delete the volume
	common.RmPVC(volName, scName)
	volNames = volNames[:len(volNames)-1]
}

var _ = Describe("Mayastor Volume IO test", func() {

	AfterEach(func() {
		logf.Log.Info("AfterEach")

		// Check resource leakage.
		err := common.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should verify an NVMe-oF TCP volume can process IO", func() {
		basicVolumeIOTest("mayastor-nvmf")
	})
	It("should verify an iSCSI volume can process IO", func() {
		basicVolumeIOTest("mayastor-iscsi")
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))
	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.	By("tearing down the test environment")
	common.TeardownTestEnv()
})
