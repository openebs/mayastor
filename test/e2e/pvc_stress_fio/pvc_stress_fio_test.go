// JIRA: CAS-500
package pvc_stress_fio_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	Cmn "e2e-basic/common"
	"e2e-basic/common/junit"

	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

var defTimeoutSecs = "60s"

// Create Delete iterations
var cdIterations = 100

// Create Read Update Delete iterations
var crudIterations = 100

// volume name and associated storage class name
// parameters required by RmPVC
type volSc struct {
	volName string
	scName  string
}

var podNames []string
var volNames []volSc

// Create a PVC and verify that (also see and keep in sync with README.md#pvc_stress_fio)
//	1. The PVC status transitions to bound,
//	2. The associated PV is created and its status transitions bound
//	3. The associated MV is created and has a State "healthy"
//  4. Optionally that a test application (fio) can read and write to the volume
// then Delete the PVC and verify that
//	1. The PVC is deleted
//	2. The associated PV is deleted
//  3. The associated MV is deleted
func testPVC(volName string, scName string, runFio bool) {
	fmt.Printf("volume: %s, storageClass:%s, run FIO:%v\n", volName, scName, runFio)
	// PVC create options
	createOpts := &coreV1.PersistentVolumeClaim{
		ObjectMeta: metaV1.ObjectMeta{
			Name:      volName,
			Namespace: "default",
		},
		Spec: coreV1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []coreV1.PersistentVolumeAccessMode{coreV1.ReadWriteOnce},
			Resources: coreV1.ResourceRequirements{
				Requests: coreV1.ResourceList{
					coreV1.ResourceStorage: resource.MustParse("64Mi"),
				},
			},
		},
	}
	// Create the PVC.
	_, createErr := Cmn.CreatePVC(createOpts)
	Expect(createErr).To(BeNil())

	// Confirm the PVC has been created.
	pvc, getPvcErr := Cmn.GetPVC(volName)
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// For cleanup
	tmp := volSc{volName, scName}
	volNames = append(volNames, tmp)

	// Wait for the PVC to be bound.
	Eventually(func() coreV1.PersistentVolumeClaimPhase {
		return Cmn.GetPvcStatusPhase(volName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(coreV1.ClaimBound))

	// Refresh the PVC contents, so that we can get the PV name.
	pvc, getPvcErr = Cmn.GetPVC(volName)
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Wait for the PV to be provisioned
	Eventually(func() *coreV1.PersistentVolume {
		pv, getPvErr := Cmn.GetPV(pvc.Spec.VolumeName)
		if getPvErr != nil {
			return nil
		}
		return pv

	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Not(BeNil()))

	// Wait for the PV to be bound.
	Eventually(func() coreV1.PersistentVolumePhase {
		return Cmn.GetPvStatusPhase(pvc.Spec.VolumeName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(coreV1.VolumeBound))

	// Wait for the MSV to be provisioned
	Eventually(func() *Cmn.MayastorVolStatus {
		return Cmn.GetMSV(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs, //timeout
		"1s",           // polling interval
	).Should(Not(BeNil()))

	// Wait for the MSV to be healthy
	Eventually(func() string {
		return Cmn.GetMsvState(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal("healthy"))

	if runFio {
		// Create the fio Pod
		fioPodName := "fio-" + volName
		pod, err := Cmn.CreateFioPod(fioPodName, volName)
		Expect(err).ToNot(HaveOccurred())
		Expect(pod).ToNot(BeNil())

		// For cleanup
		podNames = append(podNames, fioPodName)

		// Wait for the fio Pod to transition to running
		Eventually(func() bool {
			return Cmn.IsPodRunning(fioPodName)
		},
			defTimeoutSecs,
			"1s",
		).Should(Equal(true))

		// Run the fio test
		Cmn.RunFio(fioPodName, 5)

		// Delete the fio pod
		err = Cmn.DeletePod(fioPodName)
		Expect(err).ToNot(HaveOccurred())

		// cleanup
		podNames = podNames[:len(podNames)-1]
	}

	// Delete the PVC
	deleteErr := Cmn.DeletePVC(volName)
	Expect(deleteErr).To(BeNil())

	// Wait for the PVC to be deleted.
	Eventually(func() bool {
		return Cmn.IsPVCDeleted(volName)
	},
		"120s", // timeout
		"1s",   // polling interval
	).Should(Equal(true))

	// Wait for the PV to be deleted.
	Eventually(func() bool {
		return Cmn.IsPVDeleted(pvc.Spec.VolumeName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// Wait for the MSV to be deleted.
	Eventually(func() bool {
		return Cmn.IsMSVDeleted(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// cleanup
	volNames = volNames[:len(volNames)-1]
}

func stressTestPVC(iters int, runFio bool) {
	decoration := ""
	if runFio {
		decoration = "-io"
	}
	for ix := 1; ix <= iters; ix++ {
		testPVC(fmt.Sprintf("stress-pvc-nvmf%s-%d", decoration, ix), "mayastor-nvmf", runFio)
		testPVC(fmt.Sprintf("stress-pvc-iscsi%s-%d", decoration, ix), "mayastor-iscsi", runFio)
	}
}

func TestPVCStress(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(junit.ConstructJunitFileName("pvc-stress-junit.xml"))
	RunSpecsWithDefaultAndCustomReporters(t, "PVC Stress Test Suite",
		[]Reporter{junitReporter})
}

var _ = Describe("Mayastor PVC Stress test", func() {
	AfterEach(func() {
		// Check resource leakage
		err := Cmn.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should stress test creation and deletion of PVCs provisioned over iSCSI and NVMe-of", func() {
		stressTestPVC(cdIterations, false)
	})

	It("should stress test creation and deletion of PVCs provisioned over iSCSI and NVMe-of", func() {
		stressTestPVC(crudIterations, true)
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))

	Cmn.SetupTestEnv()
	tmp := os.Getenv("e2e_pvc_stress_cd_cycles")
	if len(tmp) != 0 {
		var err error
		cdIterations, err = strconv.Atoi(tmp)
		Expect(err).NotTo(HaveOccurred())
		logf.Log.Info("Cycle count changed by environment ", "Create/Delete", cdIterations)
	}

	tmp = os.Getenv("e2e_pvc_stress_crud_cycles")
	if len(tmp) != 0 {
		var err error
		crudIterations, err = strconv.Atoi(tmp)
		Expect(err).NotTo(HaveOccurred())
		logf.Log.Info("Cycle count changed by environment", "Create/Read/Update/Delete", crudIterations)
	}
	logf.Log.Info("Number of cycles are", "Create/Delete", cdIterations, "Create/Read/Update/Delete", crudIterations)

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")
	Cmn.TeardownTestEnv()
})
