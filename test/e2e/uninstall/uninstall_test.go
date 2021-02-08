package basic_test

import (
	"e2e-basic/common"
	"e2e-basic/common/junit"

	"os"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var cleanup = false

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

// Create mayastor namespace
func deleteNamespace() {
	cmd := exec.Command("kubectl", "delete", "namespace", "mayastor")
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// Teardown mayastor on the cluster under test.
// We deliberately call out to kubectl, rather than constructing the client-go
// objects, so that we can verfiy the local deploy yamls are correct.
func teardownMayastor() {
	var podsDeleted bool
	var pvcsDeleted bool
	var podCount int
	var pvcsFound bool

	logf.Log.Info("Settings:", "cleanup", cleanup)
	if !cleanup {
		found, err := common.CheckForTestPods()
		if err != nil {
			logf.Log.Error(err, "Failed to checking for test pods.")
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

	} else {
		// The correct sequence for a reusable  cluster is
		// Delete all pods in the default namespace
		// Delete all pvcs
		// Delete all mayastor pools
		// Then uninstall mayastor
		podsDeleted, podCount = common.DeleteAllPods()
		pvcsDeleted, pvcsFound = common.DeleteAllVolumeResources()
	}

	common.DeletePools()

	logf.Log.Info("Cleanup done, Uninstalling mayastor")
	// Deletes can stall indefinitely, try to mitigate this
	// by running the deletes on different threads
	go deleteDeployYaml("csi-daemonset.yaml")
	go deleteDeployYaml("mayastor-daemonset.yaml")
	go deleteDeployYaml("moac-deployment.yaml")
	go deleteDeployYaml("nats-deployment.yaml")

	{
		const timeOutSecs = 240
		const sleepSecs = 10
		maxIters := (timeOutSecs + sleepSecs - 1) / sleepSecs
		numMayastorPods := common.MayastorUndeletedPodCount()
		if numMayastorPods != 0 {
			logf.Log.Info("Waiting for Mayastor pods to be deleted",
				"timeout", timeOutSecs)
		}
		for iter := 0; iter < maxIters && numMayastorPods != 0; iter++ {
			logf.Log.Info("\tWaiting ",
				"seconds", sleepSecs,
				"numMayastorPods", numMayastorPods,
				"iter", iter)
			numMayastorPods = common.MayastorUndeletedPodCount()
			time.Sleep(sleepSecs * time.Second)
		}
	}

	// The focus is on trying to make the cluster reusable, so we try to delete everything.
	// TODO: When we start using a cluster for a single test run  move these set of deletes to after all checks.
	deleteDeployYaml("mayastorpoolcrd.yaml")
	deleteDeployYaml("moac-rbac.yaml")
	deleteDeployYaml("storage-class.yaml")
	deleteCRD("mayastornodes.openebs.io")
	deleteCRD("mayastorvolumes.openebs.io")

	if cleanup {
		// Attempt to forcefully delete mayastor pods
		forceDeleted := common.ForceDeleteMayastorPods()
		// FIXME: Temporarily disable this assert CAS-651 has been fixed
		// Expect(forceDeleted).To(BeFalse())
		if forceDeleted {
			logf.Log.Info("WARNING: Mayastor pods were force deleted at uninstall!!!")
		}
		deleteNamespace()
		// delete the namespace prior to possibly failing the uninstall
		// to yield a reusable cluster on fail.
		Expect(podsDeleted).To(BeTrue())
		Expect(podCount).To(BeZero())
		Expect(pvcsFound).To(BeFalse())
		Expect(pvcsDeleted).To(BeTrue())
	} else {
		// FIXME: Temporarily disable this assert CAS-651 has been fixed
		// and force delete lingering mayastor pods.
		// Expect(common.MayastorUndeletedPodCount()).To(Equal(0))
		if common.MayastorUndeletedPodCount() != 0 {
			logf.Log.Info("WARNING: Mayastor pods not deleted at uninstall, forcing deletion.")
			common.ForceDeleteMayastorPods()
		}
		// More verbose here as deleting the namespace is often where this
		// test hangs.
		logf.Log.Info("Deleting the mayastor namespace")
		deleteNamespace()
		logf.Log.Info("Deleted the mayastor namespace")
	}
}

func TestTeardownSuite(t *testing.T) {
	RegisterFailHandler(Fail)

	if os.Getenv("e2e_uninstall_cleanup") != "0" {
		cleanup = true
	}
	junitReporter := reporters.NewJUnitReporter(junit.ConstructJunitFileName("uninstall-junit.xml"))
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
