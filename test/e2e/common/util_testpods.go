package common

// Utility functions for test pods.
import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"os/exec"

	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

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

func IsPodRunning(podName string) bool {
	var pod corev1.Pod
	if gTestEnv.K8sClient.Get(context.TODO(), types.NamespacedName{Name: podName, Namespace: "default"}, &pod) != nil {
		return false
	}
	return pod.Status.Phase == v1.PodRunning
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
