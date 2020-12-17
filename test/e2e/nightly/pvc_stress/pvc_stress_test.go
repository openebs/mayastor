// JIRA: CAS-500
package pvc_stress_test

import (
	"fmt"
	"testing"

	Cmn "e2e-basic/common"

	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var defTimeoutSecs = "30s"

// Create a PVC and verify that
//	1. The PVC status transitions to bound,
//	2. The associated PV is created and its status transitions bound
//	3. The associated MV is created and has a State "healthy"
// then Delete the PVC and verify that
//	1. The PVC is deleted
//	2. The associated PV is deleted
//  3. The associated MV is deleted
func testPVC(volName string, scName string) {
	fmt.Printf("%s, %s\n", volName, scName)
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

	// Delete the PVC
	deleteErr := Cmn.DeletePVC(volName)
	Expect(deleteErr).To(BeNil())

	// Wait for the PVC to be deleted.
	Eventually(func() bool {
		return Cmn.IsPVCDeleted(volName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
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
}

func stressTestPVC() {
	for ix := 0; ix < 100; ix++ {
		testPVC(fmt.Sprintf("stress-pvc-nvmf-%d", ix), "mayastor-nvmf")
		testPVC(fmt.Sprintf("stress-pvc-iscsi-%d", ix), "mayastor-iscsi")
	}
}

func TestPVCStress(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PVC Stress Test Suite")
}

var _ = Describe("Mayastor PVC Stress test", func() {
	It("should stress test use of PVCs provisioned over iSCSI and NVMe-of", func() {
		stressTestPVC()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.LoggerTo(GinkgoWriter, true))

	Cmn.SetupTestEnv()
	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")
	Cmn.TeardownTestEnv()
})
