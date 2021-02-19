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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
	State    string
	Node     string
	Replicas []string
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

	msVol.Replicas = make([]string, 0, 4)

	v := reflect.ValueOf(status)
	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			sKey := key.Interface().(string)
			val := v.MapIndex(key)
			switch sKey {
			case "state":
				msVol.State = val.Interface().(string)
			case "nexus":
				nexusInt := val.Interface().(map[string]interface{})
				if node, ok := nexusInt["node"].(string); ok {
					msVol.Node = node
				}
			case "replicas":
				replicas := val.Interface().([]interface{})
				for _, replica := range replicas {
					replicaMap := reflect.ValueOf(replica)
					if replicaMap.Kind() == reflect.Map {
						for _, field := range replicaMap.MapKeys() {
							switch field.Interface().(string) {
							case "node":
								value := replicaMap.MapIndex(field)
								msVol.Replicas = append(msVol.Replicas, value.Interface().(string))
							}
						}
					}
				}
			}
		}
		// Note: msVol.Node can be unassigned here if the volume is not mounted
		Expect(msVol.State).NotTo(Equal(""))
		Expect(len(msVol.Replicas)).To(BeNumerically(">", 0))
		return &msVol
	}
	return nil
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

func DeleteMSV(uuid string) error {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").Delete(context.TODO(), uuid, metav1.DeleteOptions{})
	return err
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

// IsPvcBound returns true if a PVC with the given name is bound otherwise false is returned.
func IsPvcBound(pvcName string) bool {
	return GetPvcStatusPhase(pvcName) == corev1.ClaimBound
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
	return msv.State
}

// Retrieve the nexus node hosting the Mayastor Volume,
// and the names of the replica nodes
func GetMsvNodes(uuid string) (string, []string) {
	msv := GetMSV(uuid)
	Expect(msv).ToNot(BeNil())
	return msv.Node, msv.Replicas
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

	Eventually(func() *MayastorVolStatus {
		return GetMSV(string(pvc.ObjectMeta.UID))
	},
		defTimeoutSecs,
		"1s",
	).Should(Not(BeNil()))

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
	return IsPodRunning("fio")
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

/// Delete all pods in the default namespace
// returns:
// 1) success i.e. true if all pods were deleted or there were no pods to delete.
// 2) the number of pods found
func DeleteAllPods() (bool, int) {
	logf.Log.Info("DeleteAllPods")
	success := true
	numPods := 0
	pods, err := gTestEnv.KubeInt.CoreV1().Pods("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllPods: list pods failed.", "error", err)
		success = false
	}
	if err == nil && pods != nil {
		numPods = len(pods.Items)
		for _, pod := range pods.Items {
			logf.Log.Info("DeleteAllPods: Deleting", "pod", pod.Name)
			if err := DeletePod(pod.Name); err != nil {
				success = false
			}
		}
	}
	return success, numPods
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
					Name:  podName,
					Image: "dmonakhov/alpine-fio",
					Args:  []string{"sleep", "1000000"},
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
	MasterNode   bool
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
		masterNode := false
		for label, value := range k8snode.Labels {
			if label == "openebs.io/engine" && value == "mayastor" {
				mayastorNode = true
			}
			if label == "node-role.kubernetes.io/master" {
				masterNode = true
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
			NodeLocs = append(NodeLocs, NodeLocation{
				NodeName:     namestr,
				IPAddress:    addrstr,
				MayastorNode: mayastorNode,
				MasterNode:   masterNode,
			})
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
	depAPI := gTestEnv.KubeInt.AppsV1().Deployments
	var err error

	// this is to cater for a race condition, occasionally seen,
	// when the deployment is changed between Get and Update
	for attempts := 0; attempts < 10; attempts++ {
		deployment, err := depAPI(namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		deployment.Spec.Replicas = replicas
		deployment, err = depAPI("mayastor").Update(context.TODO(), deployment, metav1.UpdateOptions{})
		if err == nil {
			break
		}
		fmt.Printf("Re-trying update attempt due to error: %v\n", err)
		time.Sleep(1 * time.Second)
	}
	Expect(err).ToNot(HaveOccurred())
}

// TODO remove dependency on kubectl
// label is a string in the form "key=value"
// function still succeeds if label already present
func LabelNode(nodename string, label string, value string) {
	labelAssign := fmt.Sprintf("%s=%s", label, value)
	cmd := exec.Command("kubectl", "label", "node", nodename, labelAssign, "--overwrite=true")
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

// Get the execution status of the given pod, or nil if it does not exist
func getPodStatus(podNameRegexp string, namespace string, nodeName string) *v1.PodPhase {
	var validID = regexp.MustCompile(podNameRegexp)
	podAPI := gTestEnv.KubeInt.CoreV1().Pods
	podList, err := podAPI(namespace).List(context.TODO(), metav1.ListOptions{})
	Expect(err).ToNot(HaveOccurred())
	for _, pod := range podList.Items {
		if pod.Spec.NodeName == nodeName && validID.MatchString(pod.Name) {
			return &pod.Status.Phase
		}
	}
	return nil // pod not found
}

// Wait until the instance of the specified pod is present and in the running
// state on the given node
func WaitForPodRunningOnNode(podNameRegexp string, namespace string, nodeName string, timeoutSeconds int) error {
	for i := 0; i < timeoutSeconds; i++ {
		stat := getPodStatus(podNameRegexp, namespace, nodeName)

		if stat != nil && *stat == v1.PodRunning {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return errors.New("timed out waiting for pod to be running")
}

// Wait until the instance of the specified pod is absent or not in the running
// state on the given node
func WaitForPodNotRunningOnNode(podNameRegexp string, namespace string, nodeName string, timeoutSeconds int) error {
	for i := 0; i < timeoutSeconds; i++ {
		stat := getPodStatus(podNameRegexp, namespace, nodeName)

		if stat == nil || *stat != v1.PodRunning {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return errors.New("timed out waiting for pod to stop running")
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

// Return a group version resource for a MSV
func getMsvGvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
}

// Get the k8s MSV CRD
func getMsv(uuid string) (*unstructured.Unstructured, error) {
	msvGVR := getMsvGvr()
	return gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").Get(context.TODO(), uuid, metav1.GetOptions{})
}

// Get a field within the MSV.
// The "fields" argument specifies the path within the MSV where the field should be found.
// E.g. for the replicaCount field which is nested under the MSV spec the function should be called like:
//		getMsvFieldValue(<uuid>, "spec", "replicaCount")
func getMsvFieldValue(uuid string, fields ...string) (interface{}, error) {
	msv, err := getMsv(uuid)
	if err != nil {
		return nil, fmt.Errorf("Failed to get MSV with error %v", err)
	}
	if msv == nil {
		return nil, fmt.Errorf("MSV with uuid %s does not exist", uuid)
	}

	field, found, err := unstructured.NestedFieldCopy(msv.Object, fields...)
	if err != nil {
		// The last field is the one that we were looking for.
		lastFieldIndex := len(fields) - 1
		return nil, fmt.Errorf("Failed to get field %s with error %v", fields[lastFieldIndex], err)
	}
	if !found {
		// The last field is the one that we were looking for.
		lastFieldIndex := len(fields) - 1
		return nil, fmt.Errorf("Failed to find field %s", fields[lastFieldIndex])
	}
	return field, nil
}

// GetNumReplicas returns the number of replicas in the MSV.
// An error is returned if the number of replicas cannot be retrieved.
func GetNumReplicas(uuid string) (int64, error) {
	// Get the number of replicas from the MSV.
	repl, err := getMsvFieldValue(uuid, "spec", "replicaCount")
	if err != nil {
		return 0, err
	}
	if repl == nil {
		return 0, fmt.Errorf("Failed to get replicaCount")
	}

	return reflect.ValueOf(repl).Interface().(int64), nil
}

// UpdateNumReplicas sets the number of replicas in the MSV to the desired number.
// An error is returned if the number of replicas cannot be updated.
func UpdateNumReplicas(uuid string, numReplicas int64) error {
	msv, err := getMsv(uuid)
	if err != nil {
		return fmt.Errorf("Failed to get MSV with error %v", err)
	}
	if msv == nil {
		return fmt.Errorf("MSV not found")
	}

	// Set the number of replicas in the MSV.
	err = unstructured.SetNestedField(msv.Object, numReplicas, "spec", "replicaCount")
	if err != nil {
		return err
	}

	// Update the k8s MSV object.
	msvGVR := getMsvGvr()
	_, err = gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").Update(context.TODO(), msv, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("Failed to update MSV: %v", err)
	}
	return nil
}

// GetNumChildren returns the number of nexus children listed in the MSV
func GetNumChildren(uuid string) int {
	children, err := getMsvFieldValue(uuid, "status", "nexus", "children")
	if err != nil {
		return 0
	}
	if children == nil {
		return 0
	}

	switch reflect.TypeOf(children).Kind() {
	case reflect.Slice:
		return reflect.ValueOf(children).Len()
	}
	return 0
}

// NexusChild represents the information stored in the MSV about the child
type NexusChild struct {
	State string
	URI   string
}

// GetChildren returns a slice containing information about the children.
// An error is returned if the child information cannot be retrieved.
func GetChildren(uuid string) ([]NexusChild, error) {
	children, err := getMsvFieldValue(uuid, "status", "nexus", "children")
	if err != nil {
		return nil, fmt.Errorf("Failed to get children with error %v", err)
	}
	if children == nil {
		return nil, fmt.Errorf("Failed to find children")
	}

	nexusChildren := make([]NexusChild, 2)

	switch reflect.TypeOf(children).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(children)
		for i := 0; i < s.Len(); i++ {
			child := s.Index(i).Elem()
			if child.Kind() == reflect.Map {
				for _, key := range child.MapKeys() {
					skey := key.Interface().(string)
					switch skey {
					case "state":
						nexusChildren[i].State = child.MapIndex(key).Interface().(string)
					case "uri":
						nexusChildren[i].URI = child.MapIndex(key).Interface().(string)
					}
				}
			}
		}
	}

	return nexusChildren, nil
}

// GetNexusState returns the nexus state from the MSV.
// An error is returned if the nexus state cannot be retrieved.
func GetNexusState(uuid string) (string, error) {
	// Get the state of the nexus from the MSV.
	state, err := getMsvFieldValue(uuid, "status", "nexus", "state")
	if err != nil {
		return "", err
	}
	if state == nil {
		return "", fmt.Errorf("Failed to get nexus state")
	}

	return reflect.ValueOf(state).Interface().(string), nil
}

// IsVolumePublished returns true if the volume is published.
// A volume is published if the "targetNodes" field exists in the MSV.
func IsVolumePublished(uuid string) bool {
	_, err := getMsvFieldValue(uuid, "status", "targetNodes")
	if err != nil {
		return false
	}
	return true
}

func CheckForPVCs() (bool, error) {
	logf.Log.Info("CheckForPVCs")
	foundResources := false

	pvcs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").List(context.TODO(), metav1.ListOptions{})
	if err == nil && pvcs != nil && len(pvcs.Items) != 0 {
		logf.Log.Info("CheckForVolumeResources: found PersistentVolumeClaims",
			"PersistentVolumeClaims", pvcs.Items)
		foundResources = true
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

func CheckForMSVs() (bool, error) {
	logf.Log.Info("CheckForMSVs")
	foundResources := false

	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
	if err == nil && msvs != nil && len(msvs.Items) != 0 {
		logf.Log.Info("CheckForVolumeResources: found MayastorVolumes",
			"MayastorVolumes", msvs.Items)
		foundResources = true
	}
	return foundResources, err
}

func CheckForTestPods() (bool, error) {
	logf.Log.Info("CheckForTestPods")
	foundPods := false

	pods, err := gTestEnv.KubeInt.CoreV1().Pods("default").List(context.TODO(), metav1.ListOptions{})
	if err == nil && pods != nil && len(pods.Items) != 0 {
		logf.Log.Info("CheckForTestPods",
			"Pods", pods.Items)
		foundPods = true
	}
	return foundPods, err
}

// Make best attempt to delete PVCs, PVs and MSVs
func DeleteAllVolumeResources() (bool, bool) {
	logf.Log.Info("DeleteAllVolumeResources")
	foundResources := false
	success := true

	// Delete all PVCs found
	// Phase 1 to delete dangling resources
	pvcs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllVolumeResources: list PVCs failed.", "error", err)
		success = false
	}
	if err == nil && pvcs != nil && len(pvcs.Items) != 0 {
		foundResources = true
		logf.Log.Info("DeleteAllVolumeResources: deleting PersistentVolumeClaims")
		for _, pvc := range pvcs.Items {
			if err := DeletePVC(pvc.Name); err != nil {
				success = false
			}
		}
	}

	// Delete all PVs found
	pvs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllVolumeResources: list PVs failed.", "error", err)
	}
	if err == nil && pvs != nil && len(pvs.Items) != 0 {
		logf.Log.Info("DeleteAllVolumeResources: deleting PersistentVolumes")
		for _, pv := range pvs.Items {
			if err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().Delete(context.TODO(), pv.Name, metav1.DeleteOptions{}); err != nil {
				success = false
			}
		}
	}

	// Wait 2 minutes for resources to be deleted
	for attempts := 0; attempts < 120; attempts++ {
		numPvcs := 0
		pvcs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").List(context.TODO(), metav1.ListOptions{})
		if err == nil && pvcs != nil {
			numPvcs = len(pvcs.Items)
		}

		numPvs := 0
		pvs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
		if err == nil && pvs != nil {
			numPvs = len(pvs.Items)
		}

		if numPvcs == 0 && numPvs == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// If after deleting PVCs and PVs Mayastor volumes are leftover
	// try cleaning them up explicitly
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		// This function may be called by AfterSuite by uninstall test so listing MSVs may fail correctly
		logf.Log.Info("DeleteAllVolumeResources: list MSVs failed.", "Error", err)
	}
	if err == nil && msvs != nil && len(msvs.Items) != 0 {
		logf.Log.Info("DeleteAllVolumeResources: deleting MayastorVolumes")
		for _, msv := range msvs.Items {
			if err := DeleteMSV(msv.GetName()); err != nil {
				success = false
			}
		}
	}

	// Wait 2 minutes for resources to be deleted
	for attempts := 0; attempts < 120; attempts++ {
		numMsvs := 0
		msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
		if err == nil && msvs != nil {
			numMsvs = len(msvs.Items)
		}
		if numMsvs == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	return success, foundResources
}

func DeletePools() {
	poolGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorpools",
	}

	pools, err := gTestEnv.DynamicClient.Resource(poolGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		// This function may be called by AfterSuite by uninstall test so listing MSVs may fail correctly
		logf.Log.Info("DeletePools: list MSPs failed.", "Error", err)
	}
	if err == nil && pools != nil && len(pools.Items) != 0 {
		logf.Log.Info("DeletePools: deleting MayastorPools")
		for _, pool := range pools.Items {
			logf.Log.Info("DeletePools: deleting", "pool", pool.GetName())
			err = gTestEnv.DynamicClient.Resource(poolGVR).Namespace("mayastor").Delete(context.TODO(), pool.GetName(), metav1.DeleteOptions{})
			if err != nil {
				logf.Log.Error(err, "Failed to delete pool", pool.GetName())
			}
		}
	}

	numPools := 0
	// Wait 2 minutes for resources to be deleted
	for attempts := 0; attempts < 120; attempts++ {
		pools, err := gTestEnv.DynamicClient.Resource(poolGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
		if err == nil && pools != nil {
			numPools = len(pools.Items)
		}
		if numPools == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	logf.Log.Info("DeletePools: ", "Pool count", numPools)
	if numPools != 0 {
		logf.Log.Info("DeletePools: ", "Pools", pools.Items)
	}
}

func DeleteAllPoolFinalizers() {
	poolGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorpools",
	}

	pools, err := gTestEnv.DynamicClient.Resource(poolGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		// This function may be called by AfterSuite by uninstall test so listing MSVs may fail correctly
		logf.Log.Info("DeleteAllPoolFinalisers: list MSPs failed.", "Error", err)
	}
	if err == nil && pools != nil && len(pools.Items) != 0 {
		for _, pool := range pools.Items {
			empty := make([]string, 0)
			logf.Log.Info("DeleteAllPoolFinalizers", "pool", pool.GetName())
			finalizers := pool.GetFinalizers()
			if finalizers != nil {
				logf.Log.Info("Removing all finalizers", "pool", pool.GetName(), "finalizer", finalizers)
				pool.SetFinalizers(empty)
				_, err = gTestEnv.DynamicClient.Resource(poolGVR).Namespace("mayastor").Update(context.TODO(), &pool, metav1.UpdateOptions{})
				if err != nil {
					logf.Log.Error(err, "Pool update finalizer")
				}
			}
		}
	}
}

func AfterSuiteCleanup() {
	logf.Log.Info("AfterSuiteCleanup")
	//	_, _ = DeleteAllVolumeResources()
}

// Check that no PVs, PVCs and MSVs are still extant.
// Returns an error if resources exists.
func AfterEachCheck() error {
	var errorMsg = ""

	logf.Log.Info("AfterEachCheck")

	// Phase 1 to delete dangling resources
	pvcs, _ := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").List(context.TODO(), metav1.ListOptions{})
	if len(pvcs.Items) != 0 {
		errorMsg += " found leftover PersistentVolumeClaims"
		logf.Log.Info("AfterEachCheck: found leftover PersistentVolumeClaims, test fails.")
	}

	pvs, _ := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if len(pvs.Items) != 0 {
		errorMsg += " found leftover PersistentVolumes"
		logf.Log.Info("AfterEachCheck: found leftover PersistentVolumes, test fails.")
	}

	// Mayastor volumes
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
	msvs, _ := gTestEnv.DynamicClient.Resource(msvGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
	if len(msvs.Items) != 0 {
		errorMsg += " found leftover MayastorVolumes"
		logf.Log.Info("AfterEachCheck: found leftover MayastorVolumes, test fails.")
	}

	if len(errorMsg) != 0 {
		return errors.New(errorMsg)
	}
	return nil
}

func MayastorUndeletedPodCount() int {
	pods, err := gTestEnv.KubeInt.CoreV1().Pods("mayastor").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Error(err, "MayastorUndeletedPodCount: list pods failed.")
		return 0
	}
	if pods != nil {
		return len(pods.Items)
	}
	logf.Log.Info("MayastorUndeletedPodCount: nil list returned.")
	return 0
}

// Force deletion of all existing mayastor pods
// Returns true if pods were deleted, false otherwise
func ForceDeleteMayastorPods() bool {
	logf.Log.Info("EnsureMayastorDeleted")
	pods, err := gTestEnv.KubeInt.CoreV1().Pods("mayastor").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Error(err, "EnsureMayastorDeleted: list pods failed.")
		return false
	}
	if pods == nil || len(pods.Items) == 0 {
		return false
	}

	logf.Log.Info("EnsureMayastorDeleted: MayastorPods found.", "Count", len(pods.Items))
	for _, pod := range pods.Items {
		logf.Log.Info("EnsureMayastorDeleted: Force deleting", "pod", pod.Name)
		cmd := exec.Command("kubectl", "-n", "mayastor", "delete", "pod", pod.Name, "--grace-period", "0", "--force")
		_, err := cmd.CombinedOutput()
		if err != nil {
			logf.Log.Error(err, "EnsureMayastorDeleted", "podName", pod.Name)
		}
	}

	// We have made the best effort to cleanup, give things time to settle.
	for attempts := 0; attempts < 30 && MayastorUndeletedPodCount() != 0; attempts++ {
		time.Sleep(2 * time.Second)
	}

	logf.Log.Info("EnsureMayastorDeleted: lingering Mayastor pods were found !!!!!!!!")
	return true
}
