package common

// Utility functions for cleaning up a cluster
import (
	"context"
	"os/exec"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

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

func DeleteAllPools() {
	poolGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorpools",
	}

	pools, err := gTestEnv.DynamicClient.Resource(poolGVR).Namespace("mayastor").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		// This function may be called by AfterSuite by uninstall test so listing MSVs may fail correctly
		logf.Log.Info("DeleteAllPools: list MSPs failed.", "Error", err)
	}
	if err == nil && pools != nil && len(pools.Items) != 0 {
		logf.Log.Info("DeleteAllPools: deleting MayastorPools")
		for _, pool := range pools.Items {
			logf.Log.Info("DeleteAllPools: deleting", "pool", pool.GetName())
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

	logf.Log.Info("DeleteAllPools: ", "Pool count", numPools)
	if numPools != 0 {
		logf.Log.Info("DeleteAllPools: ", "Pools", pools.Items)
	}
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
