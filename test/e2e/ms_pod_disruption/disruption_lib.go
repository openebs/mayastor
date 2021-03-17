package ms_pod_disruption

import (
	"e2e-basic/common"
	"fmt"
	"os/exec"
	"time"

	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	. "github.com/onsi/gomega"
)

const (
	defTimeoutSecs           = "90s"
	disconnectionTimeoutSecs = "180s"
	podUnscheduleTimeoutSecs = 100
	podRescheduleTimeoutSecs = 180
	repairTimeoutSecs        = "180s"
	mayastorRegexp           = "^mayastor-.....$"
	moacRegexp               = "^moac-..........-.....$"
	engineLabel              = "openebs.io/engine"
	mayastorLabel            = "mayastor"
)

type DisruptionEnv struct {
	replicaToRemove string
	unusedNodes     []string
	uuid            string
	volToDelete     string
	storageClass    string
	fioPodName      string
}

// prevent mayastor pod from running on the given node
func SuppressMayastorPodOn(nodeName string) {
	common.UnlabelNode(nodeName, engineLabel)
	err := common.WaitForPodNotRunningOnNode(mayastorRegexp, common.NSMayastor, nodeName, podUnscheduleTimeoutSecs)
	Expect(err).ToNot(HaveOccurred())
}

// allow mayastor pod to run on the given node
func UnsuppressMayastorPodOn(nodeName string) {
	// add the mayastor label to the node
	common.LabelNode(nodeName, engineLabel, mayastorLabel)
	err := common.WaitForPodRunningOnNode(mayastorRegexp, common.NSMayastor, nodeName, podRescheduleTimeoutSecs)
	Expect(err).ToNot(HaveOccurred())
}

// allow mayastor pod to run on the suppressed node
func (env *DisruptionEnv) UnsuppressMayastorPod() {
	if env.replicaToRemove != "" {
		UnsuppressMayastorPodOn(env.replicaToRemove)
		env.replicaToRemove = ""
	}
}

// return the node of the replica to remove,
// and a vector of the mayastor nodes not in the volume
func getNodes(uuid string) (string, []string) {
	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())

	nexus, replicaNodes := common.GetMsvNodes(uuid)
	Expect(nexus).NotTo(Equal(""))

	toRemove := ""
	// find a node which is not the nexus and is a replica
	for _, node := range replicaNodes {
		if node != nexus {
			toRemove = node
			break
		}
	}
	Expect(toRemove).NotTo(Equal(""))

	// get a list of all of the unused mayastor nodes in the cluster
	var unusedNodes []string
	for _, node := range nodeList {
		if node.MayastorNode {
			found := false
			for _, repnode := range replicaNodes {
				if repnode == node.NodeName {
					found = true
					break
				}
			}
			if !found {
				unusedNodes = append(unusedNodes, node.NodeName)
			}
		}
	}
	// we need at least 1 spare node to re-enable to allow the volume to become healthy again
	Expect(len(unusedNodes)).NotTo(Equal(0))
	logf.Log.Info("identified nodes", "nexus", nexus, "node of replica to remove", toRemove)
	return toRemove, unusedNodes
}

// Deploy fio pod, configured to finish and verify, when the trigger-file is created
func deployHaltableFio(fioPodName string, pvcName string) {
	podObj := common.CreateFioPodDef(fioPodName, pvcName, common.VolFileSystem, common.NSDefault)

	args := []string{
		"--",
		"--time_based",
		fmt.Sprintf("--runtime=%d", 6000),
		fmt.Sprintf("--filename=%s", common.FioFsFilename),
		fmt.Sprintf("--size=%dm", common.DefaultFioSizeMb),
		"--trigger-file=/fiostop",
	}
	args = append(args, common.GetFioArgs()...)
	podObj.Spec.Containers[0].Args = args

	_, err := common.CreatePod(podObj, common.NSDefault)
	Expect(err).ToNot(HaveOccurred())
}

func haltFio(fioPodName string) {
	cmd := exec.Command("kubectl", "exec", "-it", fioPodName, "--", "touch", "/fiostop")
	cmd.Dir = ""
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// Run fio against the cluster while a replica mayastor pod is unscheduled and then rescheduled
// TODO - disable the replica on the nexus node
func (env *DisruptionEnv) PodLossTest() {
	// Disable mayastor on the spare nodes so that moac initially cannot
	// assign one to the volume to replace the faulted one. We want to make
	// the volume degraded long enough for the test to detect it.
	for _, node := range env.unusedNodes {
		logf.Log.Info("suppressing mayastor on unused node", "node", node)
		SuppressMayastorPodOn(node)
	}
	logf.Log.Info("removing mayastor replica", "node", env.replicaToRemove)
	SuppressMayastorPodOn(env.replicaToRemove)

	logf.Log.Info("waiting for pod removal to affect the nexus", "timeout", disconnectionTimeoutSecs)
	Eventually(func() string {
		volState := common.GetMsvState(env.uuid)
		logf.Log.Info("checking volume state", "state", volState)
		return volState
	},
		disconnectionTimeoutSecs, // timeout
		"5s",                     // polling interval
	).Should(Equal("degraded"))

	logf.Log.Info("volume condition", "state", common.GetMsvState(env.uuid))

	logf.Log.Info("allow fio time to run against the degraded volume")
	time.Sleep(20 * time.Second)

	// re-enable mayastor on all unused nodes
	for _, node := range env.unusedNodes {
		logf.Log.Info("unsuppressing mayastor on unused node", "node", node)
		UnsuppressMayastorPodOn(node)
	}
	logf.Log.Info("waiting for the volume to be repaired", "timeout", repairTimeoutSecs)
	Eventually(func() string {
		volState := common.GetMsvState(env.uuid)
		logf.Log.Info("checking volume state", "state", volState)
		return volState
	},
		repairTimeoutSecs, // timeout
		"5s",              // polling interval
	).Should(Equal("healthy"))

	logf.Log.Info("allow fio time to continue running against the repaired volume")
	time.Sleep(20 * time.Second)

	logf.Log.Info("enabling mayastor pod", "node", env.replicaToRemove)
	UnsuppressMayastorPodOn(env.replicaToRemove)

	logf.Log.Info("allow fio time to continue running with the restored cluster")
	time.Sleep(20 * time.Second)

	// the pod should still be running if no errors have occurred
	res, err := common.CheckPodCompleted(env.fioPodName, common.NSDefault)
	Expect(err).ToNot(HaveOccurred())
	Expect(res).To(Equal(corev1.PodRunning))

	logf.Log.Info("halting fio")
	haltFio(env.fioPodName)

	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Second)
		res, err = common.CheckPodCompleted(env.fioPodName, common.NSDefault)
		Expect(err).ToNot(HaveOccurred())

		if res != corev1.PodRunning {
			break
		}
	}
	Expect(res).To(Equal(corev1.PodSucceeded))
}

// Common steps required when setting up the test.
// Creates the PVC, deploys fio, and records variables needed for the
// test in the DisruptionEnv structure
func Setup(pvcName string, storageClassName string, fioPodName string) DisruptionEnv {
	env := DisruptionEnv{}

	env.volToDelete = pvcName
	env.storageClass = storageClassName
	env.uuid = common.MkPVC(common.DefaultVolumeSizeMb, pvcName, storageClassName, common.VolFileSystem, common.NSDefault)

	deployHaltableFio(fioPodName, pvcName)

	env.fioPodName = fioPodName
	logf.Log.Info("waiting for pod", "name", env.fioPodName)
	Eventually(func() bool {
		return common.IsPodRunning(env.fioPodName, common.NSDefault)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	env.replicaToRemove, env.unusedNodes = getNodes(env.uuid)
	return env
}

// Common steps required when tearing down the test
func (env *DisruptionEnv) Teardown() {
	var err error

	env.UnsuppressMayastorPod()

	for _, node := range env.unusedNodes {
		UnsuppressMayastorPodOn(node)
	}
	if env.fioPodName != "" {
		err = common.DeletePod(env.fioPodName, common.NSDefault)
		env.fioPodName = ""
	}
	if env.volToDelete != "" {
		common.RmPVC(env.volToDelete, env.storageClass, common.NSDefault)
		env.volToDelete = ""
	}
	Expect(err).ToNot(HaveOccurred())
}
