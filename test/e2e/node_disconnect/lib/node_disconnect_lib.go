package node_disconnect_lib

import (
	"e2e-basic/common"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	. "github.com/onsi/gomega"
)

const (
	defTimeoutSecs           = "90s"
	disconnectionTimeoutSecs = "180s"
	podUnscheduleTimeoutSecs = 100
	podRescheduleTimeoutSecs = 180
	repairTimeoutSecs        = "180s"
)

type DisconnectEnv struct {
	replicaToRemove  string
	allMayastorNodes []string
	unusedNodes      []string
	uuid             string
	volToDelete      string
	storageClass     string
	fioPodName       string
}

// Deploy an instance of fio on a node labelled as "podrefuge"
func createFioOnRefugeNode(podName string, volClaimName string) {
	podObj := common.CreateFioPodDef(podName, volClaimName, common.VolFileSystem, common.NSDefault)
	common.ApplyNodeSelectorToPodObject(podObj, "openebs.io/podrefuge", "true")
	_, err := common.CreatePod(podObj, common.NSDefault)
	Expect(err).ToNot(HaveOccurred())
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
func (env *DisconnectEnv) UnsuppressMayastorPod() {
	if env.replicaToRemove != "" {
		UnsuppressMayastorPodOn(env.replicaToRemove)
		env.replicaToRemove = ""
	}
}

// return the node of the replica to remove, the nodes in the
// volume and a vector of the mayastor-hosting nodes in the cluster
func getNodes(uuid string) (string, []string, []string) {
	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())

	var replicaToRemove = ""
	nexusNode, replicaNodes := common.GetMsvNodes(uuid)
	Expect(nexusNode).NotTo(Equal(""))

	// find a node which is not the nexus and is a replica
	for _, node := range replicaNodes {
		if node != nexusNode {
			replicaToRemove = node
			break
		}
	}
	Expect(replicaToRemove).NotTo(Equal(""))

	// get a list of all of the mayastor nodes in the cluster
	var allMayastorNodes []string
	for _, node := range nodeList {
		if node.MayastorNode {
			allMayastorNodes = append(allMayastorNodes, node.NodeName)
		}
	}
	logf.Log.Info("identified nodes", "nexus", nexusNode, "node of replica to remove", replicaToRemove)
	return replicaToRemove, replicaNodes, allMayastorNodes
}

// Run fio against the cluster while a replica mayastor pod is unscheduled and then rescheduled
func (env *DisconnectEnv) PodLossTest() {
	Expect(len(env.allMayastorNodes)).To(BeNumerically(">=", 2)) // must support >= 2 replicas

	// disable mayastor on the spare nodes so that moac cannot assign
	// them to the volume to replace the faulted one. We want to keep
	// the volume degraded before restoring the suppressed node.
	for _, node := range env.unusedNodes {
		logf.Log.Info("suppressing mayastor on unused node", "node", node)
		SuppressMayastorPodOn(node)
	}
	logf.Log.Info("removing mayastor replica", "node", env.replicaToRemove)
	SuppressMayastorPodOn(env.replicaToRemove)

	logf.Log.Info("waiting for pod removal to affect the nexus", "timeout", disconnectionTimeoutSecs)
	Eventually(func() string {
		logf.Log.Info("running fio against the volume")
		_, err := common.RunFio(env.fioPodName, 5, common.FioFsFilename)
		Expect(err).ToNot(HaveOccurred())
		return common.GetMsvState(env.uuid)
	},
		disconnectionTimeoutSecs, // timeout
		"1s",                     // polling interval
	).Should(Equal("degraded"))

	logf.Log.Info("volume condition", "state", common.GetMsvState(env.uuid))

	logf.Log.Info("running fio against the degraded volume")
	_, err := common.RunFio(env.fioPodName, 20, common.FioFsFilename)
	Expect(err).ToNot(HaveOccurred())

	logf.Log.Info("enabling mayastor pod", "node", env.replicaToRemove)
	env.UnsuppressMayastorPod()

	logf.Log.Info("waiting for the volume to be repaired", "timeout", repairTimeoutSecs)
	Eventually(func() string {
		logf.Log.Info("running fio while volume is being repaired")
		_, err := common.RunFio(env.fioPodName, 5, common.FioFsFilename)
		Expect(err).ToNot(HaveOccurred())
		return common.GetMsvState(env.uuid)
	},
		repairTimeoutSecs, // timeout
		"1s",              // polling interval
	).Should(Equal("healthy"))

	logf.Log.Info("volume condition", "state", common.GetMsvState(env.uuid))

	logf.Log.Info("running fio against the repaired volume")
	_, err = common.RunFio(env.fioPodName, 20, common.FioFsFilename)
	Expect(err).ToNot(HaveOccurred())
}

// Common steps required when setting up the test.
// Creates the PVC, deploys fio, and records variables needed for the
// test in the DisconnectEnv structure
func Setup(pvcName string, storageClassName string, fioPodName string) DisconnectEnv {
	env := DisconnectEnv{}

	env.volToDelete = pvcName
	env.storageClass = storageClassName
	env.uuid = common.MkPVC(common.DefaultVolumeSizeMb, pvcName, storageClassName, common.VolFileSystem, common.NSDefault)

	podObj := common.CreateFioPodDef(fioPodName, pvcName, common.VolFileSystem, common.NSDefault)
	_, err := common.CreatePod(podObj, common.NSDefault)
	Expect(err).ToNot(HaveOccurred())

	env.fioPodName = fioPodName
	logf.Log.Info("waiting for pod", "name", env.fioPodName)
	Eventually(func() bool {
		return common.IsPodRunning(env.fioPodName, common.NSDefault)
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))

	var replicaNodes []string
	env.replicaToRemove, replicaNodes, env.allMayastorNodes = getNodes(env.uuid)

	// Identify mayastor nodes not currently part of the volume
	for _, node := range env.allMayastorNodes {
		unused := true
		for _, replica := range replicaNodes {
			if node == replica { // part of the current volume
				unused = false
				break
			}
		}
		if unused {
			env.unusedNodes = append(env.unusedNodes, node)
		}
	}
	return env
}

// Common steps required when tearing down the test
func (env *DisconnectEnv) Teardown() {
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
