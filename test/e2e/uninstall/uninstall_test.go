package basic_test

import (
	"e2e-basic/common"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"os"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// Encapsulate the logic to find where the deploy yamls are
func getDeployYamlDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Clean(filename + "/../../../../deploy")
}

// Helper for passing yaml from the deploy directory to kubectl
func deleteDeployYaml(filename string) {
	cmd := exec.Command("kubectl", "delete", "-f", filename)
	cmd.Dir = getDeployYamlDir()
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

// Helper for deleting mayastor CRDs
func deleteCRD(crdName string) {
	cmd := exec.Command("kubectl", "delete", "crd", crdName)
	_ = cmd.Run()
}

// Teardown mayastor on the cluster under test.
// We deliberately call out to kubectl, rather than constructing the client-go
// objects, so that we can verfiy the local deploy yamls are correct.
func teardownMayastor() {
	// The correct sequence for a reusable  cluster is
	// Delete all pods in the default namespace
	// Delete all pvcs
	// Delete all mayastor pools
	// Then uninstall mayastor
	podsDeleted, podCount := common.DeleteAllPods()
	pvcsDeleted, pvcsFound := common.DeleteAllVolumeResources()
	common.DeletePools()

	logf.Log.Info("Cleanup done, Uninstalling mayastor")
	// Deletes can stall indefinitely, try to mitigate this
	// by running the deletes on different threads
	go deleteDeployYaml("csi-daemonset.yaml")
	time.Sleep(10 * time.Second)
	go deleteDeployYaml("mayastor-daemonset.yaml")
	time.Sleep(5 * time.Second)
	go deleteDeployYaml("moac-deployment.yaml")
	time.Sleep(5 * time.Second)
	go deleteDeployYaml("nats-deployment.yaml")
	time.Sleep(5 * time.Second)

	{
		iters := 18
		logf.Log.Info("Waiting for Mayastor pods to be deleted", "timeout seconds", iters*10)
		numMayastorPods := common.MayastorUndeletedPodCount()
		for attempts := 0; attempts < iters && numMayastorPods != 0; attempts++ {
			time.Sleep(10 * time.Second)
			numMayastorPods = common.MayastorUndeletedPodCount()
			logf.Log.Info("", "numMayastorPods", numMayastorPods)
		}
	}

	// The focus is on trying to make the cluster reusable, so we try to delete everything.
	// TODO: When we start using a cluster for a single test run  move these set of deletes to after all checks.
	deleteDeployYaml("mayastorpoolcrd.yaml")
	deleteDeployYaml("moac-rbac.yaml")
	deleteDeployYaml("storage-class.yaml")
	deleteCRD("mayastornodes.openebs.io")
	deleteCRD("mayastorvolumes.openebs.io")
	// Attempt to forcefully delete pods
	// TODO replace this function call when a single cluster is used for a single test run, with a check.
	forceDeleted := common.ForceDeleteMayastorPods()
	deleteDeployYaml("namespace.yaml")
	// FIXME: Temporarily disable this assert CAS-651 has been raised
	// Expect(forceDeleted).To(BeFalse())
	if forceDeleted {
		logf.Log.Info("Mayastor PODS were force deleted at uninstall!!!!!!!!!!!!")
	}

	Expect(podsDeleted).To(BeTrue())
	Expect(podCount).To(BeZero())
	Expect(pvcsFound).To(BeFalse())
	Expect(pvcsDeleted).To(BeTrue())
	Expect(common.MayastorUndeletedPodCount()).To(Equal(0))
}

func TestTeardownSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	reportDir := os.Getenv("e2e_reports_dir")
	junitReporter := reporters.NewJUnitReporter(reportDir + "/uninstall-junit.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Basic Teardown Suite",
		[]Reporter{junitReporter})
}

var _ = Describe("Mayastor setup", func() {
	It("should teardown using yamls", func() {
		teardownMayastor()
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
