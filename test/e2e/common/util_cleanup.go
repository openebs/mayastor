package common

// Utility functions for cleaning up a cluster
import (
	"context"
	"os/exec"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var ZeroInt64 = int64(0)

/// Delete all pods in the default namespace
func DeleteAllPods(nameSpace string) (int, error) {
	logf.Log.Info("DeleteAllPods")
	numPods := 0

	pods, err := gTestEnv.KubeInt.CoreV1().Pods(nameSpace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllPods: list pods failed.", "error", err)
	} else {
		numPods = len(pods.Items)
		logf.Log.Info("DeleteAllPods: found", "pods", numPods)
		for _, pod := range pods.Items {
			logf.Log.Info("DeleteAllPods: Deleting", "pod", pod.Name)
			delErr := gTestEnv.KubeInt.CoreV1().Pods(nameSpace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &ZeroInt64})
			if delErr != nil {
				logf.Log.Info("DeleteAllPods: failed to delete the pod", "podName", pod.Name, "error", delErr)
			}
		}
	}
	return numPods, err
}

// Make best attempt to delete PersistentVolumeClaims
// returns ok -> operations succeeded, resources undeleted, delete resources failed
func DeleteAllPvcs(nameSpace string) (int, error) {
	logf.Log.Info("DeleteAllPvcs")

	// Delete all PVCs found
	pvcs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllPvcs: list PersistentVolumeClaims failed.", "error", err)
	} else if len(pvcs.Items) != 0 {
		for _, pvc := range pvcs.Items {
			logf.Log.Info("DeleteAllPvcs: deleting", "PersistentVolumeClaim", pvc.Name)
			delErr := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{GracePeriodSeconds: &ZeroInt64})
			if delErr != nil {
				logf.Log.Info("DeleteAllPvcs: failed to delete", "PersistentVolumeClaim", pvc.Name, "error", delErr)
			}
		}
	}

	// Wait 2 minutes for PVCS to be deleted
	numPvcs := 0
	for attempts := 0; attempts < 120; attempts++ {
		pvcs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims(nameSpace).List(context.TODO(), metav1.ListOptions{})
		if err == nil {
			numPvcs = len(pvcs.Items)
			if numPvcs == 0 {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	logf.Log.Info("DeleteAllPvcs:", "number of PersistentVolumeClaims", numPvcs, "error", err)
	return numPvcs, err
}

// Make best attempt to delete PersistentVolumes
func DeleteAllPvs() (int, error) {
	// Delete all PVs found
	// First remove all finalizers
	pvs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllPvs: list PersistentVolumes failed.", "error", err)
	} else if len(pvs.Items) != 0 {
		empty := make([]string, 0)
		for _, pv := range pvs.Items {
			finalizers := pv.GetFinalizers()
			if len(finalizers) != 0 {
				logf.Log.Info("DeleteAllPvs: deleting finalizer for",
					"PersistentVolume", pv.Name, "finalizers", finalizers)
				pv.SetFinalizers(empty)
				_, _ = gTestEnv.KubeInt.CoreV1().PersistentVolumes().Update(context.TODO(), &pv, metav1.UpdateOptions{})
			}
		}
	}

	// then wait for up to 2 minute for resources to be cleared
	numPvs := 0
	for attempts := 0; attempts < 120; attempts++ {
		pvs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
		if err == nil {
			numPvs = len(pvs.Items)
			if numPvs == 0 {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	// Then delete the PVs
	pvs, err = gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllPvs: list PersistentVolumes failed.", "error", err)
	} else if len(pvs.Items) != 0 {
		for _, pv := range pvs.Items {
			logf.Log.Info("DeleteAllPvs: deleting PersistentVolume",
				"PersistentVolume", pv.Name)
			if delErr := gTestEnv.KubeInt.CoreV1().PersistentVolumes().Delete(context.TODO(), pv.Name, metav1.DeleteOptions{GracePeriodSeconds: &ZeroInt64}); delErr != nil {
				logf.Log.Info("DeleteAllPvs: failed to delete PersistentVolume",
					"PersistentVolume", pv.Name, "error", delErr)
			}
		}
	}
	// Wait 2 minutes for resources to be deleted
	numPvs = 0
	for attempts := 0; attempts < 120; attempts++ {
		pvs, err := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
		if err == nil {
			numPvs = len(pvs.Items)
			if numPvs == 0 {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	logf.Log.Info("DeleteAllPvs:", "number of PersistentVolumes", numPvs, "error", err)
	return numPvs, err
}

// Make best attempt to delete MayastorVolumes
func DeleteAllMsvs() (int, error) {
	// If after deleting PVCs and PVs Mayastor volumes are leftover
	// try cleaning them up explicitly
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		// This function may be called by AfterSuite by uninstall test so listing MSVs may fail correctly
		logf.Log.Info("DeleteAllMsvs: list MSVs failed.", "Error", err)
	}
	if err == nil && msvs != nil && len(msvs.Items) != 0 {
		for _, msv := range msvs.Items {
			logf.Log.Info("DeleteAllMsvs: deleting MayastorVolume", "MayastorVolume", msv.GetName())
			if delErr := DeleteMSV(msv.GetName()); delErr != nil {
				logf.Log.Info("DeleteAllMsvs: failed deleting MayastorVolume", "MayastorVolume", msv.GetName(), "error", delErr)
			}
		}
	}

	numMsvs := 0
	// Wait 2 minutes for resources to be deleted
	for attempts := 0; attempts < 120; attempts++ {
		msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
		if err == nil && msvs != nil {
			numMsvs = len(msvs.Items)
			if numMsvs == 0 {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	logf.Log.Info("DeleteAllMsvs:", "number of MayastorVolumes", numMsvs)

	return numMsvs, err
}

func DeleteAllPoolFinalizers() (bool, error) {
	deletedFinalizer := false
	var deleteErr error

	poolGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorpools",
	}

	pools, err := gTestEnv.DynamicClient.Resource(poolGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("DeleteAllPoolFinalisers: list MSPs failed.", "Error", err)
		return false, err
	} else if len(pools.Items) != 0 {
		for _, pool := range pools.Items {
			empty := make([]string, 0)
			logf.Log.Info("DeleteAllPoolFinalizers", "pool", pool.GetName())
			finalizers := pool.GetFinalizers()
			if finalizers != nil {
				logf.Log.Info("Removing all finalizers", "pool", pool.GetName(), "finalizer", finalizers)
				pool.SetFinalizers(empty)
				_, err = gTestEnv.DynamicClient.Resource(poolGVR).Namespace(NSMayastor).Update(context.TODO(), &pool, metav1.UpdateOptions{})
				if err != nil {
					deleteErr = err
					logf.Log.Info("Pool update finalizer", "error", err)
				} else {
					deletedFinalizer = true
				}
			}
		}
	}
	return deletedFinalizer, deleteErr
}

func DeleteAllPools() bool {
	poolGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorpools",
	}

	pools, err := gTestEnv.DynamicClient.Resource(poolGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		// This function may be called by AfterSuite by uninstall test so listing MSVs may fail correctly
		logf.Log.Info("DeleteAllPools: list MSPs failed.", "Error", err)
	}
	if err == nil && pools != nil && len(pools.Items) != 0 {
		logf.Log.Info("DeleteAllPools: deleting MayastorPools")
		for _, pool := range pools.Items {
			logf.Log.Info("DeleteAllPools: deleting", "pool", pool.GetName())
			err = gTestEnv.DynamicClient.Resource(poolGVR).Namespace(NSMayastor).Delete(context.TODO(), pool.GetName(), metav1.DeleteOptions{GracePeriodSeconds: &ZeroInt64})
			if err != nil {
				logf.Log.Info("DeleteAllPools: failed to delete pool", pool.GetName(), "error", err)
			}
		}
	}

	numPools := 0
	// Wait 2 minutes for resources to be deleted
	for attempts := 0; attempts < 120; attempts++ {
		pools, err := gTestEnv.DynamicClient.Resource(poolGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
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
	return numPools == 0
}

//  >=0 definitive number of mayastor pods
// < 0 indeterminate
func MayastorUndeletedPodCount() int {
	ns, err := gTestEnv.KubeInt.CoreV1().Namespaces().Get(context.TODO(), NSMayastor, metav1.GetOptions{})
	if err != nil {
		logf.Log.Info("MayastorUndeletedPodCount: get namespace", "error", err)
		//FIXME: if the error is namespace not found return 0
		return -1
	}
	if ns == nil {
		// No namespace => no mayastor pods
		return 0
	}
	pods, err := gTestEnv.KubeInt.CoreV1().Pods(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("MayastorUndeletedPodCount: list pods failed.", "error", err)
		return -1
	}
	return len(pods.Items)
}

// Force deletion of all existing mayastor pods
// returns  the number of pods still present, and error
func ForceDeleteMayastorPods() (bool, int, error) {
	var err error
	podsDeleted := false

	logf.Log.Info("EnsureMayastorDeleted")
	pods, err := gTestEnv.KubeInt.CoreV1().Pods(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logf.Log.Info("EnsureMayastorDeleted: list pods failed.", "error", err)
		return podsDeleted, 0, err
	} else if len(pods.Items) == 0 {
		return podsDeleted, 0, nil
	}

	logf.Log.Info("EnsureMayastorDeleted: MayastorPods found.", "Count", len(pods.Items))
	for _, pod := range pods.Items {
		logf.Log.Info("EnsureMayastorDeleted: Force deleting", "pod", pod.Name)
		cmd := exec.Command("kubectl", "-n", NSMayastor, "delete", "pod", pod.Name, "--grace-period", "0", "--force")
		_, err := cmd.CombinedOutput()
		if err != nil {
			logf.Log.Info("EnsureMayastorDeleted", "podName", pod.Name, "error", err)
		} else {
			podsDeleted = true
		}
	}

	podCount := 0
	// We have made the best effort to cleanup, give things time to settle.
	for attempts := 0; attempts < 60 && MayastorUndeletedPodCount() != 0; attempts++ {
		pods, err = gTestEnv.KubeInt.CoreV1().Pods(NSMayastor).List(context.TODO(), metav1.ListOptions{})
		if err == nil {
			podCount = len(pods.Items)
			if podCount == 0 {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	return podsDeleted, podCount, err
}

// "Big" sweep, attempts to remove artefacts left over in the cluster
// that would prevent future successful test runs.
// returns true if cleanup was successful i.e. all resources were deleted
// and no errors were encountered.
func CleanUp() bool {
	var errs []error
	podCount := 0
	pvcCount := 0

	nameSpaces, err := gTestEnv.KubeInt.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err == nil {
		for _, ns := range nameSpaces.Items {
			if strings.HasPrefix(ns.Name, NSE2EPrefix) || ns.Name == NSDefault {
				tmp, err := DeleteAllPods(ns.Name)
				if err != nil {
					errs = append(errs, err)
				}
				podCount += tmp
				tmp, err = DeleteAllPvcs(ns.Name)
				if err != nil {
					errs = append(errs, err)
				}
				pvcCount += tmp
			}
		}
	} else {
		errs = append(errs, err)
	}

	pvCount, err := DeleteAllPvs()
	if err != nil {
		errs = append(errs, err)
	}
	msvCount, err := DeleteAllMsvs()
	if err != nil {
		errs = append(errs, err)
	}
	// Pools should not have finalizers if there are no associated volume resources.
	poolFinalizerDeleted, delPoolFinalizeErr := DeleteAllPoolFinalizers()

	logf.Log.Info("Resource cleanup",
		"podCount", podCount,
		"pvcCount", pvcCount,
		"pvCount", pvCount,
		"msvCount", msvCount,
		"err", errs,
		"poolFinalizerDeleted", poolFinalizerDeleted,
		"delPoolFinalizeErr", delPoolFinalizeErr,
	)

	scList, err := gTestEnv.KubeInt.StorageV1().StorageClasses().List(context.TODO(), metav1.ListOptions{})
	if err == nil {
		for _, sc := range scList.Items {
			if sc.Provisioner == "io.openebs.csi-mayastor" {
				logf.Log.Info("Deleting", "storageClass", sc.Name)
				_ = gTestEnv.KubeInt.StorageV1().StorageClasses().Delete(context.TODO(), sc.Name, metav1.DeleteOptions{GracePeriodSeconds: &ZeroInt64})
			}
		}
	} else {
		errs = append(errs, err)
	}

	for _, ns := range nameSpaces.Items {
		if strings.HasPrefix(ns.Name, NSE2EPrefix) {
			err = RmNamespace(ns.Name)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	// log all the errors
	for _, err := range errs {
		logf.Log.Info("", "error", err)
	}

	// For now ignore delMsvErr, until we figure out how to ignore "no resource of this type" errors
	return (podCount+pvcCount+pvCount+msvCount) == 0 && len(errs) == 0
}
