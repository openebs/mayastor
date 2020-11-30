package common

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/gomega"

	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var defTimeoutSecs = "90s"

func ApplyDeployYaml(filename string) {
	cmd := exec.Command("kubectl", "apply", "-f", filename)
	cmd.Dir = ""
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

func DeleteDeployYaml(filename string) {
	cmd := exec.Command("kubectl", "delete", "-f", filename)
	cmd.Dir = ""
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

func LabelNode(nodename string, label string) {
	cmd := exec.Command("kubectl", "label", "node", nodename, label)
	cmd.Dir = ""
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

// Status part of the mayastor volume CRD
type mayastorVolStatus struct {
	state  string
	reason string
	node   string
}

func GetMSV(uuid string, dynamicClient *dynamic.Interface) *mayastorVolStatus {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
	msv, err := (*dynamicClient).Resource(msvGVR).Namespace("mayastor").Get(context.TODO(), uuid, metav1.GetOptions{})
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if msv == nil {
		return nil
	}
	status, found, err := unstructured.NestedFieldCopy(msv.Object, "status")
	if err != nil {
		fmt.Println(err)
		return nil
	}

	if !found {
		return nil
	}
	msVol := mayastorVolStatus{}
	v := reflect.ValueOf(status)
	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			sKey := key.Interface().(string)
			val := v.MapIndex(key)
			switch sKey {
			case "state":
				msVol.state = val.Interface().(string)
				break
			case "reason":
				msVol.reason = val.Interface().(string)
				break
			case "node":
				msVol.node = val.Interface().(string)
				break
			}
		}
	}
	return &msVol
}

// Check for a deleted Mayastor Volume,
// the object does not exist if deleted
func IsMSVDeleted(uuid string, dynamicClient *dynamic.Interface) bool {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msv, err := (*dynamicClient).Resource(msvGVR).Namespace("mayastor").Get(context.TODO(), uuid, metav1.GetOptions{})

	if err != nil {
		// Unfortunately there is no associated error code so we resort to string comparison
		if strings.HasPrefix(err.Error(), "mayastorvolumes.openebs.io") &&
			strings.HasSuffix(err.Error(), " not found") {
			return true
		}
	}

	Expect(err).To(BeNil())
	Expect(msv).ToNot(BeNil())
	return false
}

// Check for a deleted Persistent Volume Claim,
// either the object does not exist
// or the status phase is invalid.
func IsPVCDeleted(volName string, kubeInt *kubernetes.Interface) bool {
	pvc, err := (*kubeInt).CoreV1().PersistentVolumeClaims("default").Get(context.TODO(), volName, metav1.GetOptions{})
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
func IsPVDeleted(volName *string, kubeInt *kubernetes.Interface) bool {
	vn := *volName
	pv, err := (*kubeInt).CoreV1().PersistentVolumes().Get(context.TODO(), vn, metav1.GetOptions{})
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

// Retrieve status phase of a Persistent Volume Claim
func GetPvcStatusPhase(volname string, kubeInt *kubernetes.Interface) (phase corev1.PersistentVolumeClaimPhase) {
	pvc, getPvcErr := (*kubeInt).CoreV1().PersistentVolumeClaims("default").Get(context.TODO(), volname, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())
	return pvc.Status.Phase
}

// Retrieve status phase of a Persistent Volume
func GetPvStatusPhase(volname string, kubeInt *kubernetes.Interface) (phase corev1.PersistentVolumePhase) {
	pv, getPvErr := (*kubeInt).CoreV1().PersistentVolumes().Get(context.TODO(), volname, metav1.GetOptions{})
	Expect(getPvErr).To(BeNil())
	Expect(pv).ToNot(BeNil())
	return pv.Status.Phase
}

// Retrieve the state of a Mayastor Volume
func GetMsvState(uuid string, dynamicClient *dynamic.Interface) string {
	msv := GetMSV(uuid, dynamicClient)
	Expect(msv).ToNot(BeNil())
	return fmt.Sprintf("%s", msv.state)
}

// Retrieve the nexus node hosting the Mayastor Volume
func GetMsvNode(uuid string, dynamicClient *dynamic.Interface) string {
	msv := GetMSV(uuid, dynamicClient)
	Expect(msv).ToNot(BeNil())
	return fmt.Sprintf("%s", msv.node)
}

// Create a PVC and verify that
//	1. The PVC status transitions to bound,
//	2. The associated PV is created and its status transitions bound
//	3. The associated MV is created and has a State "healthy"
func MkPVC(volName string, scName string, dynamicClient *dynamic.Interface, kubeInt *kubernetes.Interface) string {
	fmt.Printf("creating %s, %s\n", volName, scName)
	// PVC create options
	createOpts := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volName,
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("64Mi"),
				},
			},
		},
	}

	// Create the PVC.
	PVCApi := (*kubeInt).CoreV1().PersistentVolumeClaims
	_, createErr := PVCApi("default").Create(context.TODO(), createOpts, metav1.CreateOptions{})
	Expect(createErr).To(BeNil())

	// Confirm the PVC has been created.
	pvc, getPvcErr := PVCApi("default").Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Wait for the PVC to be bound.
	Eventually(func() corev1.PersistentVolumeClaimPhase {
		return GetPvcStatusPhase(volName, kubeInt)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(corev1.ClaimBound))

	// Refresh the PVC contents, so that we can get the PV name.
	pvc, getPvcErr = PVCApi("default").Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Wait for the PV to be provisioned
	Eventually(func() *corev1.PersistentVolume {
		pv, getPvErr := (*kubeInt).CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
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
		return GetPvStatusPhase(pvc.Spec.VolumeName, kubeInt)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(corev1.VolumeBound))

	msv := GetMSV(string(pvc.ObjectMeta.UID), dynamicClient)
	Expect(msv).ToNot(BeNil())
	return string(pvc.ObjectMeta.UID)
}

// Delete the PVC and verify that
//	1. The PVC is deleted
//	2. The associated PV is deleted
//  3. The associated MV is deleted
func RmPVC(volName string, scName string, dynamicClient *dynamic.Interface, kubeInt *kubernetes.Interface) {
	fmt.Printf("removing %s, %s\n", volName, scName)

	PVCApi := (*kubeInt).CoreV1().PersistentVolumeClaims

	// Confirm the PVC has been created.
	pvc, getPvcErr := PVCApi("default").Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Delete the PVC
	deleteErr := PVCApi("default").Delete(context.TODO(), volName, metav1.DeleteOptions{})
	Expect(deleteErr).To(BeNil())

	// Wait for the PVC to be deleted.
	Eventually(func() bool {
		return IsPVCDeleted(volName, kubeInt)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// Wait for the PV to be deleted.
	Eventually(func() bool {
		return IsPVDeleted(&(pvc.Spec.VolumeName), kubeInt)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// Wait for the MSV to be deleted.
	Eventually(func() bool {
		return IsMSVDeleted(string(pvc.ObjectMeta.UID), dynamicClient)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))
}

func RunFio() {
	cmd := exec.Command(
		"kubectl",
		"exec",
		"-it",
		"fio",
		"--",
		"fio",
		"--name=benchtest",
		"--size=50m",
		"--filename=/volume/test",
		"--direct=1",
		"--rw=randrw",
		"--ioengine=libaio",
		"--bs=4k",
		"--iodepth=16",
		"--numjobs=1",
		"--time_based",
		"--runtime=20",
	)
	cmd.Dir = ""
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

func FioReadyPod(k8sClient *client.Client) bool {
	var fioPod corev1.Pod
	if (*k8sClient).Get(context.TODO(), types.NamespacedName{Name: "fio", Namespace: "default"}, &fioPod) != nil {
		return false
	}
	return fioPod.Status.Phase == v1.PodRunning
}
