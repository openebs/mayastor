package common

// Utility functions for test pods.
import (
	"context"
	"errors"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"os/exec"
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

//  These variables match the settings used in createFioPodDef
var FioFsMountPoint = "/volume"
var FioBlockFilename = "/dev/sdm"
var FioFsFilename = FioFsMountPoint + "/fiotestfile"

// FIXME: this function runs fio with a bunch of parameters which are not configurable.
func RunFio(podName string, duration int, filename string, args ...string) ([]byte, error) {
	argRuntime := fmt.Sprintf("--runtime=%d", duration)
	argFilename := fmt.Sprintf("--filename=%s", filename)

	logf.Log.Info("RunFio",
		"podName", podName,
		"duration", duration,
		"filename", filename,
		"args", args)

	cmdArgs := []string{
		"exec",
		"-it",
		podName,
		"--",
		"fio",
		"--name=benchtest",
		"--size=50m",
		argFilename,
		"--direct=1",
		"--rw=randrw",
		"--ioengine=libaio",
		"--bs=4k",
		"--iodepth=16",
		"--numjobs=1",
		"--time_based",
		argRuntime,
	}
	if args != nil {
		cmdArgs = append(cmdArgs, args...)
	}
	cmd := exec.Command(
		"kubectl",
		cmdArgs...,
	)
	cmd.Dir = ""
	output, err := cmd.CombinedOutput()
	return output, err
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
	logf.Log.Info("Creating", "pod", podDef.Name)
	return gTestEnv.KubeInt.CoreV1().Pods("default").Create(context.TODO(), podDef, metav1.CreateOptions{})
}

/// Delete a Pod in default namespace, no options and no context
func DeletePod(podName string) error {
	logf.Log.Info("Deleting", "pod", podName)
	return gTestEnv.KubeInt.CoreV1().Pods("default").Delete(context.TODO(), podName, metav1.DeleteOptions{})
}

/// Create a test fio pod in default namespace, no options and no context
func createFioPodDef(podName string, volName string, rawBlock bool) *corev1.Pod {
	volMounts := []corev1.VolumeMount{
		{
			Name:      "ms-volume",
			MountPath: FioFsMountPoint,
		},
	}
	volDevices := []corev1.VolumeDevice{
		{
			Name:       "ms-volume",
			DevicePath: FioBlockFilename,
		},
	}

	podDef := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  podName,
					Image: "dmonakhov/alpine-fio",
					Args:  []string{"sleep", "1000000"},
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
	if rawBlock {
		podDef.Spec.Containers[0].VolumeDevices = volDevices
	} else {
		podDef.Spec.Containers[0].VolumeMounts = volMounts
	}
	return &podDef
}

/// Create a test fio pod in default namespace, no options and no context
/// mayastor volume is mounted on /volume
func CreateFioPodDef(podName string, volName string) *corev1.Pod {
	return createFioPodDef(podName, volName, false)
}

/// Create a test fio pod in default namespace, no options and no context
/// mayastor volume is mounted on /volume
func CreateFioPod(podName string, volName string) (*corev1.Pod, error) {
	logf.Log.Info("Creating fio pod definition", "name", podName, "volume type", "filesystem")
	podDef := createFioPodDef(podName, volName, false)
	return CreatePod(podDef)
}

/// Create a test fio pod in default namespace, no options and no context
/// mayastor device is mounted on /dev/sdm
func CreateRawBlockFioPod(podName string, volName string) (*corev1.Pod, error) {
	logf.Log.Info("Creating fio pod definition", "name", podName, "volume type", "raw block")
	podDef := createFioPodDef(podName, volName, true)
	return CreatePod(podDef)
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

// Check test pods in a namespace for restarts and failed/unknown state
func CheckPods(namespace string) error {
	podApi := gTestEnv.KubeInt.CoreV1().Pods
	var errorStrings []string
	podList, err := podApi(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.New("failed to list pods")
	}

	for _, pod := range podList.Items {
		containerStatuses := pod.Status.ContainerStatuses
		for _, containerStatus := range containerStatuses {
			if containerStatus.RestartCount != 0 {
				logf.Log.Info(pod.Name, "restarts", containerStatus.RestartCount)
				errorStrings = append(errorStrings, fmt.Sprintf("%s restarted %d times", pod.Name, containerStatus.RestartCount))
			}
			if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodUnknown {
				logf.Log.Info(pod.Name, "phase", pod.Status.Phase)
				errorStrings = append(errorStrings, fmt.Sprintf("%s phase is %v", pod.Name, pod.Status.Phase))
			}
		}
	}

	if len(errorStrings) != 0 {
		return errors.New(strings.Join(errorStrings[:], "; "))
	}
	return nil
}
