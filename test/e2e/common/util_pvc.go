package common

// Utility functions for Persistent Volume Claims and Persistent Volumes
import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var defTimeoutSecs = "90s"

// Check for a deleted Persistent Volume Claim,
// either the object does not exist
// or the status phase is invalid.
func IsPVCDeleted(volName string, nameSpace string) bool {
	pvc, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).Get(context.TODO(), volName, metav1.GetOptions{})
	if err != nil {
		// Unfortunately there is no associated error code so we resort to string comparison
		if strings.HasPrefix(err.Error(), "persistentvolumeclaims") &&
			strings.HasSuffix(err.Error(), " not found") {
			return true
		}
	}
	// After the PVC has been deleted it may still accessible, but status phase will be invalid
	Expect(err).To(BeNil())
	Expect(pvc).ToNot(BeNil())
	switch pvc.Status.Phase {
	case
		corev1.ClaimBound,
		corev1.ClaimPending,
		corev1.ClaimLost:
		return false
	default:
		return true
	}
}

// Check for a deleted Persistent Volume,
// either the object does not exist
// or the status phase is invalid.
func IsPVDeleted(volName string) bool {
	pv, err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), volName, metav1.GetOptions{})
	if err != nil {
		// Unfortunately there is no associated error code so we resort to string comparison
		if strings.HasPrefix(err.Error(), "persistentvolumes") &&
			strings.HasSuffix(err.Error(), " not found") {
			return true
		}
	}
	// After the PV has been deleted it may still accessible, but status phase will be invalid
	Expect(err).To(BeNil())
	Expect(pv).ToNot(BeNil())
	switch pv.Status.Phase {
	case
		corev1.VolumeBound,
		corev1.VolumeAvailable,
		corev1.VolumeFailed,
		corev1.VolumePending,
		corev1.VolumeReleased:
		return false
	default:
		return true
	}
}

// IsPvcBound returns true if a PVC with the given name is bound otherwise false is returned.
func IsPvcBound(pvcName string, nameSpace string) bool {
	return GetPvcStatusPhase(pvcName, nameSpace) == corev1.ClaimBound
}

// Retrieve status phase of a Persistent Volume Claim
func GetPvcStatusPhase(volname string, nameSpace string) (phase corev1.PersistentVolumeClaimPhase) {
	pvc, getPvcErr := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).Get(context.TODO(), volname, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())
	return pvc.Status.Phase
}

// Retrieve status phase of a Persistent Volume
func GetPvStatusPhase(volname string) (phase corev1.PersistentVolumePhase) {
	pv, getPvErr := gTestEnv.KubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), volname, metav1.GetOptions{})
	Expect(getPvErr).To(BeNil())
	Expect(pv).ToNot(BeNil())
	return pv.Status.Phase
}

// Create a PVC and verify that
//	1. The PVC status transitions to bound,
//	2. The associated PV is created and its status transitions bound
//	3. The associated MV is created and has a State "healthy"
func MkPVC(volSizeMb int, volName string, scName string, volType VolumeType, nameSpace string) string {
	logf.Log.Info("Creating", "volume", volName, "storageClass", scName, "volume type", volType)
	volSizeMbStr := fmt.Sprintf("%dMi", volSizeMb)
	// PVC create options
	createOpts := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volName,
			Namespace: nameSpace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(volSizeMbStr),
				},
			},
		},
	}

	if volType == VolRawBlock {
		var blockVolumeMode = corev1.PersistentVolumeBlock
		createOpts.Spec.VolumeMode = &blockVolumeMode
	}

	// Create the PVC.
	PVCApi := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims
	_, createErr := PVCApi(nameSpace).Create(context.TODO(), createOpts, metav1.CreateOptions{})
	Expect(createErr).To(BeNil())

	// Confirm the PVC has been created.
	pvc, getPvcErr := PVCApi(nameSpace).Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	ScApi := gTestEnv.KubeInt.StorageV1().StorageClasses
	sc, getScErr := ScApi().Get(context.TODO(), scName, metav1.GetOptions{})
	Expect(getScErr).To(BeNil())
	if *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		return string(pvc.ObjectMeta.UID)
	}

	// Wait for the PVC to be bound.
	Eventually(func() corev1.PersistentVolumeClaimPhase {
		return GetPvcStatusPhase(volName, nameSpace)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(corev1.ClaimBound))

	// Refresh the PVC contents, so that we can get the PV name.
	pvc, getPvcErr = PVCApi(nameSpace).Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Wait for the PV to be provisioned
	Eventually(func() *corev1.PersistentVolume {
		pv, getPvErr := gTestEnv.KubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
		if getPvErr != nil {
			return nil
		}
		return pv

	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Not(BeNil()))

	// Wait for the PV to be bound.
	Eventually(func() corev1.PersistentVolumePhase {
		return GetPvStatusPhase(pvc.Spec.VolumeName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(corev1.VolumeBound))

	Eventually(func() *MayastorVolStatus {
		return GetMSV(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs,
		"1s",
	).Should(Not(BeNil()))

	logf.Log.Info("Created", "volume", volName, "uuid", pvc.ObjectMeta.UID, "storageClass", scName, "volume type", volType)
	return string(pvc.ObjectMeta.UID)
}

// Delete a PVC in the default namespace and verify that
//	1. The PVC is deleted
//	2. The associated PV is deleted
//  3. The associated MV is deleted
func RmPVC(volName string, scName string, nameSpace string) {
	logf.Log.Info("Removing volume", "volume", volName, "storageClass", scName)

	PVCApi := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims

	// Confirm the PVC has been created.
	pvc, getPvcErr := PVCApi(nameSpace).Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Delete the PVC
	deleteErr := PVCApi(nameSpace).Delete(context.TODO(), volName, metav1.DeleteOptions{})
	Expect(deleteErr).To(BeNil())

	// Wait for the PVC to be deleted.
	Eventually(func() bool {
		return IsPVCDeleted(volName, nameSpace)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// Wait for the PV to be deleted.
	Eventually(func() bool {
		return IsPVDeleted(pvc.Spec.VolumeName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// Wait for the MSV to be deleted.
	Eventually(func() bool {
		return IsMSVDeleted(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))
}

/// Create a PVC in default namespace, no options and no context
func CreatePVC(pvc *v1.PersistentVolumeClaim, nameSpace string) (*v1.PersistentVolumeClaim, error) {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).Create(context.TODO(), pvc, metav1.CreateOptions{})
}

/// Retrieve a PVC in default namespace, no options and no context
func GetPVC(volName string, nameSpace string) (*v1.PersistentVolumeClaim, error) {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).Get(context.TODO(), volName, metav1.GetOptions{})
}

/// Delete a PVC in default namespace, no options and no context
func DeletePVC(volName string, nameSpace string) error {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).Delete(context.TODO(), volName, metav1.DeleteOptions{})
}

/// Retrieve a PV in default namespace, no options and no context
func GetPV(volName string) (*v1.PersistentVolume, error) {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), volName, metav1.GetOptions{})
}

func CheckForPVCs() (bool, error) {
	logf.Log.Info("CheckForPVCs")
	foundResources := false

	nameSpaces, err := gTestEnv.KubeInt.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err == nil {
		for _, ns := range nameSpaces.Items {
			if strings.HasPrefix(ns.Name, NSE2EPrefix) || ns.Name == NSDefault {
				pvcs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(ns.Name).List(context.TODO(), metav1.ListOptions{})
				if err == nil && pvcs != nil && len(pvcs.Items) != 0 {
					logf.Log.Info("CheckForVolumeResources: found PersistentVolumeClaims",
						"PersistentVolumeClaims", pvcs.Items)
					foundResources = true
				}
			}
		}
	}

	return foundResources, err
}

func CheckForPVs() (bool, error) {
	logf.Log.Info("CheckForPVs")
	foundResources := false

	pvs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err == nil && pvs != nil && len(pvs.Items) != 0 {
		logf.Log.Info("CheckForVolumeResources: found PersistentVolumes",
			"PersistentVolumes", pvs.Items)
		foundResources = true
	}
	return foundResources, err
}
