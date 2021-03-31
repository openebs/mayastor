package basic_test

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"
	"e2e-basic/common/locations"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Helper for deleting mayastor CRDs
func deleteCRD(crdName string) {
	cmd := exec.Command("kubectl", "delete", "crd", crdName)
	_ = cmd.Run()
}

// Create mayastor namespace
func deleteNamespace() {
	cmd := exec.Command("kubectl", "delete", "namespace", common.NSMayastor)
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// Teardown mayastor on the cluster under test.
// We deliberately call out to kubectl, rather than constructing the client-go
// objects, so that we can verify the local deploy yaml files are correct.
func teardownMayastor() {
	var cleaned bool
	cleanup := e2e_config.GetConfig().Uninstall.Cleanup != 0

	logf.Log.Info("Settings:", "cleanup", cleanup)
	if cleanup {
		cleaned = common.CleanUp()
	} else {

		found, err := common.CheckForTestPods()
		if err != nil {
			logf.Log.Info("Failed to checking for test pods.", "error", err)
		} else {
			Expect(found).To(BeFalse(), "Application pods were found, none expected.")
		}

		found, err = common.CheckForPVCs()
		if err != nil {
			logf.Log.Info("Failed to check for PVCs", "error", err)
		}
		Expect(found).To(BeFalse(), "PersistentVolumeClaims were found, none expected.")

		found, err = common.CheckForPVs()
		if err != nil {
			logf.Log.Info("Failed to check PVs", "error", err)
		}
		Expect(found).To(BeFalse(), "PersistentVolumes were found, none expected.")

		found, err = common.CheckForMSVs()
		if err != nil {
			logf.Log.Info("Failed to check MSVs", "error", err)
		}
		Expect(found).To(BeFalse(), "Mayastor volume CRDs were found, none expected.")

		err = common.CheckAllPoolsAreOnline()
		Expect(err).ToNot(HaveOccurred())

	}

	poolsDeleted := common.DeleteAllPools()
	Expect(poolsDeleted).To(BeTrue())

	logf.Log.Info("Cleanup done, Uninstalling mayastor")
	yamlsDir := locations.GetGeneratedYamlsDir()
	// Deletes can stall indefinitely, try to mitigate this
	// by running the deletes on different threads
	go common.KubeCtlDeleteYaml("csi-daemonset.yaml", yamlsDir)
	go common.KubeCtlDeleteYaml("mayastor-daemonset.yaml", yamlsDir)
	go common.KubeCtlDeleteYaml("moac-deployment.yaml", yamlsDir)
	go common.KubeCtlDeleteYaml("nats-deployment.yaml", yamlsDir)

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

	deployDir := locations.GetMayastorDeployDir()
	common.KubeCtlDeleteYaml("mayastorpoolcrd.yaml", deployDir)
	common.KubeCtlDeleteYaml("moac-rbac.yaml", yamlsDir)

	// MOAC implicitly creates these CRDs, should we delete?
	deleteCRD("mayastornodes.openebs.io")
	deleteCRD("mayastorvolumes.openebs.io")

	if cleanup {
		// Attempt to forcefully delete mayastor pods
		deleted, podCount, err := common.ForceDeleteMayastorPods()
		Expect(err).ToNot(HaveOccurred(), "ForceDeleteMayastorPods failed %v", err)
		Expect(podCount).To(BeZero(), "All Mayastor pods have not been deleted")
		// Only delete the namespace if there are no pending resources
		// otherwise this hangs.
		deleteNamespace()
		if deleted {
			logf.Log.Info("Mayastor pods were force deleted on cleanup!")
		}
		if cleaned {
			logf.Log.Info("Application pods or volume resources were deleted on cleanup!")
		}
	} else {
		Expect(common.MayastorUndeletedPodCount()).To(Equal(0), "All Mayastor pods were not removed on uninstall")
		// More verbose here as deleting the namespace is often where this
		// test hangs.
		logf.Log.Info("Deleting the mayastor namespace")
		deleteNamespace()
		logf.Log.Info("Deleted the mayastor namespace")
	}
}

func TestTeardownSuite(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Basic Teardown Suite", "uninstall")
}

var _ = Describe("Mayastor setup", func() {
	It("should teardown using yamls", func() {
		teardownMayastor()
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
