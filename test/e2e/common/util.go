package common

import (
	"context"
	"errors"
	"os/exec"
	"regexp"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const NSMayastor = "mayastor"

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

// create a storage class
func MkStorageClass(scName string, scReplicas int, protocol string, provisioner string) error {
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
	return createErr
}

// remove a storage class
func RmStorageClass(scName string) error {
	ScApi := gTestEnv.KubeInt.StorageV1().StorageClasses
	deleteErr := ScApi().Delete(context.TODO(), scName, metav1.DeleteOptions{})
	return deleteErr
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
	_, err = depApi(NSMayastor).Update(context.TODO(), deployment, metav1.UpdateOptions{})
	Expect(err).ToNot(HaveOccurred())
}

// Remove all node selectors from the deployment spec and apply
func RemoveAllNodeSelectorsFromDeployment(deploymentName string, namespace string) {
	depApi := gTestEnv.KubeInt.AppsV1().Deployments
	deployment, err := depApi(namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	Expect(err).ToNot(HaveOccurred())
	if deployment.Spec.Template.Spec.NodeSelector != nil {
		deployment.Spec.Template.Spec.NodeSelector = nil
		_, err = depApi(NSMayastor).Update(context.TODO(), deployment, metav1.UpdateOptions{})
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
		deployment, err = depAPI(NSMayastor).Update(context.TODO(), deployment, metav1.UpdateOptions{})
		if err == nil {
			break
		}
		logf.Log.Info("Re-trying update attempt due to error", "error", err)
		time.Sleep(1 * time.Second)
	}
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

func AfterSuiteCleanup() {
	logf.Log.Info("AfterSuiteCleanup")
	// Place holder function,
	// to facilitate post-mortem analysis do nothing
	// however we may choose to cleanup based on
	// test configuration.
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
	msvs, _ := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if len(msvs.Items) != 0 {
		errorMsg += " found leftover MayastorVolumes"
		logf.Log.Info("AfterEachCheck: found leftover MayastorVolumes, test fails.")
	}

	if len(errorMsg) != 0 {
		return errors.New(errorMsg)
	}
	return nil
}
