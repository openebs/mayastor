// JIRA: CAS-500
package pvc_stress_test

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/deprecated/scheme"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var cfg *rest.Config
var k8sClient client.Client
var kubeInt kubernetes.Interface
var k8sManager ctrl.Manager
var testEnv *envtest.Environment
var dynamicClient dynamic.Interface
var defTimeoutSecs = "30s"

// Status part of the mayastor volume CRD
type mayastorVolStatus struct {
	state  string
	reason string
	node   string
	/* Not required for now.
	nexus struct {
		children [ ]map[string]string
		deviceUri string
		state string
	}
	replicas []map[string]string
	*/
}

func getMSV(uuid string) *mayastorVolStatus {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msv, err := dynamicClient.Resource(msvGVR).Namespace("mayastor").Get(context.TODO(), uuid, metav1.GetOptions{})
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
func isMSVDeleted(uuid string) bool {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msv, err := dynamicClient.Resource(msvGVR).Namespace("mayastor").Get(context.TODO(), uuid, metav1.GetOptions{})

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
func isPVCDeleted(volName string) bool {
	pvc, err := kubeInt.CoreV1().PersistentVolumeClaims("default").Get(context.TODO(), volName, metav1.GetOptions{})
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
func isPVDeleted(volName string) bool {
	pv, err := kubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), volName, metav1.GetOptions{})
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
func getPvcClaimStatusPhase(volname string) (phase corev1.PersistentVolumeClaimPhase) {
	pvc, getPvcErr := kubeInt.CoreV1().PersistentVolumeClaims("default").Get(context.TODO(), volname, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())
	return pvc.Status.Phase
}

// Retrieve status phase of a Persistent Volume
func getPvStatusPhase(volname string) (phase corev1.PersistentVolumePhase) {
	pv, getPvErr := kubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), volname, metav1.GetOptions{})
	Expect(getPvErr).To(BeNil())
	Expect(pv).ToNot(BeNil())
	return pv.Status.Phase
}

// Retrieve the state of a Mayastor Volume
func getMsvState(uuid string) (state string) {
	msv := getMSV(uuid)
	Expect(msv).ToNot(BeNil())
	return msv.state
}

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
	PVCApi := kubeInt.CoreV1().PersistentVolumeClaims
	_, createErr := PVCApi("default").Create(context.TODO(), createOpts, metav1.CreateOptions{})
	Expect(createErr).To(BeNil())

	// Confirm the PVC has been created.
	pvc, getPvcErr := PVCApi("default").Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Wait for the PVC to be bound.
	Eventually(func() corev1.PersistentVolumeClaimPhase {
		return getPvcClaimStatusPhase(volName)
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
		pv, getPvErr := kubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
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
		return getPvStatusPhase(pvc.Spec.VolumeName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(corev1.VolumeBound))

	msv := getMSV(string(pvc.ObjectMeta.UID))
	Expect(msv).ToNot(BeNil())
	Expect(msv.state).Should(Equal("healthy"))

	// Wait for the MSV to be healthy
	Eventually(func() string {
		return getMsvState(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal("healthy"))

	// Delete the PVC
	deleteErr := PVCApi("default").Delete(context.TODO(), volName, metav1.DeleteOptions{})
	Expect(deleteErr).To(BeNil())

	// Wait for the PVC to be deleted.
	Eventually(func() bool {
		return isPVCDeleted(volName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// Wait for the PV to be deleted.
	Eventually(func() bool {
		return isPVDeleted(pvc.Spec.VolumeName)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	// Wait for the MSV to be deleted.
	Eventually(func() bool {
		return isMSVDeleted(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))
}

func stressTestPVC() {
	for ix := 0; ix < 100; ix++ {
		testPVC(fmt.Sprintf("stress-pvc-nvmf-%d", ix), "mayastor-nvmf")
		testPVC(fmt.Sprintf("stress-pvc-iscsi-%d", ix), "mayastor-iscsi")
		// FIXME: Without this delay getPvcClaimStatusPhase returns Pending
		// even though kubectl shows that the pvc is Bound.
		//pause()
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

	By("bootstrapping test environment")
	useCluster := true
	testEnv = &envtest.Environment{
		UseExistingCluster:       &useCluster,
		AttachControlPlaneOutput: true,
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(cfg).ToNot(BeNil())

	k8sManager, err = ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
	})
	Expect(err).ToNot(HaveOccurred())

	go func() {
		err = k8sManager.Start(ctrl.SetupSignalHandler())
		Expect(err).ToNot(HaveOccurred())
	}()

	mgrSyncCtx, mgrSyncCtxCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer mgrSyncCtxCancel()
	if synced := k8sManager.GetCache().WaitForCacheSync(mgrSyncCtx.Done()); !synced {
		fmt.Println("Failed to sync")
	}

	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).ToNot(BeNil())

	restConfig := config.GetConfigOrDie()
	Expect(restConfig).ToNot(BeNil())

	kubeInt = kubernetes.NewForConfigOrDie(restConfig)
	Expect(kubeInt).ToNot(BeNil())

	dynamicClient = dynamic.NewForConfigOrDie(restConfig)
	Expect(dynamicClient).ToNot(BeNil())

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})
