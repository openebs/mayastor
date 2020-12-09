package common

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/gomega"

	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

// Status part of the mayastor volume CRD
type MayastorVolStatus struct {
	State  string
	Reason string
	Node   string
}

func GetMSV(uuid string) *MayastorVolStatus {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
	msv, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").Get(context.TODO(), uuid, metav1.GetOptions{})
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
	msVol := MayastorVolStatus{}
	v := reflect.ValueOf(status)
	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			sKey := key.Interface().(string)
			val := v.MapIndex(key)
			switch sKey {
			case "state":
				msVol.State = val.Interface().(string)
				break
			case "reason":
				msVol.Reason = val.Interface().(string)
				break
			case "node":
				msVol.Node = val.Interface().(string)
				break
			}
		}
	}
	return &msVol
}

// Check for a deleted Mayastor Volume,
// the object does not exist if deleted
func IsMSVDeleted(uuid string) bool {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msv, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").Get(context.TODO(), uuid, metav1.GetOptions{})

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
func IsPVCDeleted(volName string) bool {
	pvc, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").Get(context.TODO(), volName, metav1.GetOptions{})
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

// Retrieve status phase of a Persistent Volume Claim
func GetPvcStatusPhase(volname string) (phase corev1.PersistentVolumeClaimPhase) {
	pvc, getPvcErr := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").Get(context.TODO(), volname, metav1.GetOptions{})
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

// Retrieve the state of a Mayastor Volume
func GetMsvState(uuid string) string {
	msv := GetMSV(uuid)
	Expect(msv).ToNot(BeNil())
	return fmt.Sprintf("%s", msv.State)
}

// Retrieve the nexus node hosting the Mayastor Volume
func GetMsvNode(uuid string) string {
	msv := GetMSV(uuid)
	Expect(msv).ToNot(BeNil())
	return fmt.Sprintf("%s", msv.Node)
}

// Create a PVC and verify that
//	1. The PVC status transitions to bound,
//	2. The associated PV is created and its status transitions bound
//	3. The associated MV is created and has a State "healthy"
func MkPVC(volName string, scName string) string {
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
	PVCApi := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims
	_, createErr := PVCApi("default").Create(context.TODO(), createOpts, metav1.CreateOptions{})
	Expect(createErr).To(BeNil())

	// Confirm the PVC has been created.
	pvc, getPvcErr := PVCApi("default").Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Wait for the PVC to be bound.
	Eventually(func() corev1.PersistentVolumeClaimPhase {
		return GetPvcStatusPhase(volName)
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

	msv := GetMSV(string(pvc.ObjectMeta.UID))
	Expect(msv).ToNot(BeNil())
	return string(pvc.ObjectMeta.UID)
}

// Delete the PVC and verify that
//	1. The PVC is deleted
//	2. The associated PV is deleted
//  3. The associated MV is deleted
func RmPVC(volName string, scName string) {
	fmt.Printf("removing %s, %s\n", volName, scName)

	PVCApi := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims

	// Confirm the PVC has been created.
	pvc, getPvcErr := PVCApi("default").Get(context.TODO(), volName, metav1.GetOptions{})
	Expect(getPvcErr).To(BeNil())
	Expect(pvc).ToNot(BeNil())

	// Delete the PVC
	deleteErr := PVCApi("default").Delete(context.TODO(), volName, metav1.DeleteOptions{})
	Expect(deleteErr).To(BeNil())

	// Wait for the PVC to be deleted.
	Eventually(func() bool {
		return IsPVCDeleted(volName)
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

func RunFio(podName string, duration int) {
	argRuntime := fmt.Sprintf("--runtime=%d", duration)
	cmd := exec.Command(
		"kubectl",
		"exec",
		"-it",
		podName,
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
		argRuntime,
	)
	cmd.Dir = ""
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

func FioReadyPod() bool {
	var fioPod corev1.Pod
	if gTestEnv.K8sClient.Get(context.TODO(), types.NamespacedName{Name: "fio", Namespace: "default"}, &fioPod) != nil {
		return false
	}
	return fioPod.Status.Phase == v1.PodRunning
}

func IsPodRunning(podName string) bool {
	var pod corev1.Pod
	if gTestEnv.K8sClient.Get(context.TODO(), types.NamespacedName{Name: podName, Namespace: "default"}, &pod) != nil {
		return false
	}
	return pod.Status.Phase == v1.PodRunning
}

/// Create a PVC in default namespace, no options and no context
func CreatePVC(pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").Create(context.TODO(), pvc, metav1.CreateOptions{})
}

/// Retrieve a PVC in default namespace, no options and no context
func GetPVC(volName string) (*v1.PersistentVolumeClaim, error) {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").Get(context.TODO(), volName, metav1.GetOptions{})
}

/// Delete a PVC in default namespace, no options and no context
func DeletePVC(volName string) error {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").Delete(context.TODO(), volName, metav1.DeleteOptions{})
}

/// Retrieve a PV in default namespace, no options and no context
func GetPV(volName string) (*v1.PersistentVolume, error) {
	return gTestEnv.KubeInt.CoreV1().PersistentVolumes().Get(context.TODO(), volName, metav1.GetOptions{})
}

/// Create a Pod in default namespace, no options and no context
func CreatePod(podDef *corev1.Pod) (*corev1.Pod, error) {
	return gTestEnv.KubeInt.CoreV1().Pods("default").Create(context.TODO(), podDef, metav1.CreateOptions{})
}

/// Delete a Pod in default namespace, no options and no context
func DeletePod(podName string) error {
	return gTestEnv.KubeInt.CoreV1().Pods("default").Delete(context.TODO(), podName, metav1.DeleteOptions{})
}

func CreateFioPod(podName string, volName string) (*corev1.Pod, error) {
	podDef := CreateFioPodDef(podName, volName)
	return CreatePod(podDef)
}

/// Create a test fio pod in default namespace, no options and no context
/// mayastor volume is mounted on /volume
func CreateFioPodDef(podName string, volName string) *corev1.Pod {
	podDef := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    podName,
					Image:   "nixery.dev/shell/fio/tini",
					Command: []string{"tini", "--"},
					Args:    []string{"sleep", "1000000"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "ms-volume",
							MountPath: "/volume",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "ms-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: volName,
						},
					},
				},
			},
		},
	}
	return &podDef
}

type NodeLocation struct {
	NodeName     string
	IPAddress    string
	MayastorNode bool
}

// returns vector of populated NodeLocation structs
func GetNodeLocs() ([]NodeLocation, error) {
	nodeList := corev1.NodeList{}

	if gTestEnv.K8sClient.List(context.TODO(), &nodeList, &client.ListOptions{}) != nil {
		return nil, errors.New("failed to list nodes")
	}
	NodeLocs := make([]NodeLocation, 0, len(nodeList.Items))
	for _, k8snode := range nodeList.Items {
		addrstr := ""
		namestr := ""
		mayastorNode := false
		for label, value := range k8snode.Labels {
			if label == "openebs.io/engine" && value == "mayastor" {
				mayastorNode = true
			}
		}
		for _, addr := range k8snode.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				addrstr = addr.Address
			}
			if addr.Type == corev1.NodeHostName {
				namestr = addr.Address
			}
		}
		if namestr != "" && addrstr != "" {
			NodeLocs = append(NodeLocs, NodeLocation{NodeName: namestr, IPAddress: addrstr, MayastorNode: mayastorNode})
		} else {
			return nil, errors.New("node lacks expected fields")
		}
	}
	return NodeLocs, nil
}

// create a storage class
func MkStorageClass(scName string, scReplicas int, protocol string, provisioner string) {
	createOpts := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scName,
			Namespace: "default",
		},
		Provisioner: provisioner,
	}
	createOpts.Parameters = make(map[string]string)
	createOpts.Parameters["protocol"] = protocol
	createOpts.Parameters["repl"] = strconv.Itoa(scReplicas)

	ScApi := gTestEnv.KubeInt.StorageV1().StorageClasses
	_, createErr := ScApi().Create(context.TODO(), createOpts, metav1.CreateOptions{})
	Expect(createErr).To(BeNil())
}

// remove a storage class
func RmStorageClass(scName string) {
	ScApi := gTestEnv.KubeInt.StorageV1().StorageClasses
	deleteErr := ScApi().Delete(context.TODO(), scName, metav1.DeleteOptions{})
	Expect(deleteErr).To(BeNil())
}

// Add a node selector to the given pod definition
func ApplyNodeSelectorToPodObject(pod *corev1.Pod, label string, value string) {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}
	pod.Spec.NodeSelector[label] = value
}

// Add a node selector to the deployment spec and apply
func ApplyNodeSelectorToDeployment(deploymentName string, namespace string, label string, value string) {
	depApi := gTestEnv.KubeInt.AppsV1().Deployments
	deployment, err := depApi(namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	Expect(err).ToNot(HaveOccurred())
	if deployment.Spec.Template.Spec.NodeSelector == nil {
		deployment.Spec.Template.Spec.NodeSelector = make(map[string]string)
	}
	deployment.Spec.Template.Spec.NodeSelector[label] = value
	_, err = depApi("mayastor").Update(context.TODO(), deployment, metav1.UpdateOptions{})
	Expect(err).ToNot(HaveOccurred())
}

// Remove all node selectors from the deployment spec and apply
func RemoveAllNodeSelectorsFromDeployment(deploymentName string, namespace string) {
	depApi := gTestEnv.KubeInt.AppsV1().Deployments
	deployment, err := depApi(namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	Expect(err).ToNot(HaveOccurred())
	if deployment.Spec.Template.Spec.NodeSelector != nil {
		deployment.Spec.Template.Spec.NodeSelector = nil
		_, err = depApi("mayastor").Update(context.TODO(), deployment, metav1.UpdateOptions{})
	}
	Expect(err).ToNot(HaveOccurred())
}

// Adjust the number of replicas in the deployment
func SetDeploymentReplication(deploymentName string, namespace string, replicas *int32) {
	depApi := gTestEnv.KubeInt.AppsV1().Deployments
	deployment, err := depApi(namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	Expect(err).ToNot(HaveOccurred())
	deployment.Spec.Replicas = replicas
	_, err = depApi("mayastor").Update(context.TODO(), deployment, metav1.UpdateOptions{})
	Expect(err).ToNot(HaveOccurred())
}

// TODO remove dependency on kubectl
// label is a string in the form "key=value"
// function still succeeds if label already present
func LabelNode(nodename string, label string) {
	cmd := exec.Command("kubectl", "label", "node", nodename, label, "--overwrite=true")
	cmd.Dir = ""
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

// TODO remove dependency on kubectl
// function still succeeds if label not present
func UnlabelNode(nodename string, label string) {
	cmd := exec.Command("kubectl", "label", "node", nodename, label+"-")
	cmd.Dir = ""
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

// Wait until all instances of the specified pod are absent from the given node
func WaitForPodAbsentFromNode(podNameRegexp string, namespace string, nodeName string, timeoutSeconds int) error {
	var validID = regexp.MustCompile(podNameRegexp)
	var podAbsent bool = false

	podApi := gTestEnv.KubeInt.CoreV1().Pods

	for i := 0; i < timeoutSeconds && podAbsent == false; i++ {
		podAbsent = true
		time.Sleep(time.Second)
		podList, err := podApi(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return errors.New("failed to list pods")
		}
		for _, pod := range podList.Items {
			if pod.Spec.NodeName == nodeName {
				if validID.MatchString(pod.Name) {
					podAbsent = false
					break
				}
			}
		}
	}
	if podAbsent == false {
		return errors.New("timed out waiting for pod")
	}
	return nil
}

// Wait until the instance of the specified pod is present and in the running
// state on the given node
func WaitForPodRunningOnNode(podNameRegexp string, namespace string, nodeName string, timeoutSeconds int) error {
	var validID = regexp.MustCompile(podNameRegexp)
	podReady := false

	podApi := gTestEnv.KubeInt.CoreV1().Pods

	for i := 0; i < timeoutSeconds && podReady == false; i++ {
		time.Sleep(time.Second)
		podList, err := podApi(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return errors.New("failed to list pods")
		}
		for _, pod := range podList.Items {
			if pod.Spec.NodeName == nodeName && pod.Status.Phase == v1.PodRunning {
				if validID.MatchString(pod.Name) {
					podReady = true
					break
				}
			}
		}
	}
	if podReady == false {
		return errors.New("timed out waiting for pod")
	}
	return nil
}

// returns true if the pod is present on the given node
func PodPresentOnNode(podNameRegexp string, namespace string, nodeName string) bool {
	var validID = regexp.MustCompile(podNameRegexp)
	podApi := gTestEnv.KubeInt.CoreV1().Pods
	podList, err := podApi(namespace).List(context.TODO(), metav1.ListOptions{})
	Expect(err).ToNot(HaveOccurred())

	for _, pod := range podList.Items {
		if pod.Spec.NodeName == nodeName {
			if validID.MatchString(pod.Name) {
				return true
			}
		}
	}
	return false
}
