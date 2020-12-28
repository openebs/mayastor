package node_disconnect_lib

import (
	"e2e-basic/common"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/gomega"
)

const (
	defTimeoutSecs           = "90s"
	disconnectionTimeoutSecs = "90s"
	repairTimeoutSecs        = "90s"
)

type DisconnectEnv struct {
	nodeToIsolate    string
	otherNodes       []string
	uuid             string
	disconnectMethod string
	volToDelete      string
	storageClass     string
	fioPodName       string
}

// Deploy an instance of fio on a node labelled as "podrefuge"
func createFioOnRefugeNode(podName string, volClaimName string) {
	podObj := common.CreateFioPodDef(podName, volClaimName)
	common.ApplyNodeSelectorToPodObject(podObj, "openebs.io/podrefuge", "true")
	_, err := common.CreatePod(podObj)
	Expect(err).ToNot(HaveOccurred())
}

// disconnect a node from the other nodes in the cluster
func DisconnectNode(nodeName string, otherNodes []string, method string) {
	for _, targetIP := range otherNodes {
		cmd := exec.Command("bash", "../lib/io_connect_node.sh", nodeName, targetIP, "DISCONNECT", method)
		cmd.Dir = "./"
		_, err := cmd.CombinedOutput()
		Expect(err).ToNot(HaveOccurred())
	}
}

// reconnect a node to the other nodes in the cluster
func (env *DisconnectEnv) ReconnectNode(checkError bool) {
	for _, targetIP := range env.otherNodes {
		cmd := exec.Command("bash", "../lib/io_connect_node.sh", env.nodeToIsolate, targetIP, "RECONNECT", env.disconnectMethod)
		cmd.Dir = "./"
		_, err := cmd.CombinedOutput()
		if checkError {
			Expect(err).ToNot(HaveOccurred())
		}
	}
}

// return the node name to isolate and a vector of IP addresses to isolate
func getNodes(uuid string) (string, []string) {
	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())

	var nodeToIsolate = ""
	nexusNode, replicas := common.GetMsvNodes(uuid)
	Expect(nexusNode).NotTo(Equal(""))
	fmt.Printf("nexus node is \"%s\"\n", nexusNode)

	var otherAddresses []string

	// find a node which is not the nexus and is a replica
	for _, node := range replicas {
		if node != nexusNode {
			nodeToIsolate = node
			break
		}
	}
	Expect(nodeToIsolate).NotTo(Equal(""))

	// get a list of the other ip addresses in the cluster
	for _, node := range nodeList {
		if node.NodeName != nodeToIsolate {
			otherAddresses = append(otherAddresses, node.IPAddress)
		}
	}
	Expect(len(otherAddresses)).To(BeNumerically(">", 0))

	fmt.Printf("node to isolate is \"%s\"\n", nodeToIsolate)
	return nodeToIsolate, otherAddresses
}

// Run fio against the cluster while a replica is being removed and reconnected to the network
func (env *DisconnectEnv) LossTest() {
	fmt.Printf("running spawned fio\n")
	go common.RunFio(env.fioPodName, 20)

	time.Sleep(5 * time.Second)
	fmt.Printf("disconnecting \"%s\"\n", env.nodeToIsolate)

	DisconnectNode(env.nodeToIsolate, env.otherNodes, env.disconnectMethod)

	fmt.Printf("waiting up to %s for disconnection to affect the nexus\n", disconnectionTimeoutSecs)
	Eventually(func() string {
		return common.GetMsvState(env.uuid)
	},
		disconnectionTimeoutSecs, // timeout
		"1s",                     // polling interval
	).Should(Equal("degraded"))

	fmt.Printf("volume is in state \"%s\"\n", common.GetMsvState(env.uuid))

	fmt.Printf("running fio while node is disconnected\n")
	common.RunFio(env.fioPodName, 20)

	fmt.Printf("reconnecting \"%s\"\n", env.nodeToIsolate)
	env.ReconnectNode(true)

	fmt.Printf("running fio when node is reconnected\n")
	common.RunFio(env.fioPodName, 20)
}

// Remove the replica without running IO and verify that the volume becomes degraded but is still functional
func (env *DisconnectEnv) LossWhenIdleTest() {
	fmt.Printf("disconnecting \"%s\"\n", env.nodeToIsolate)

	DisconnectNode(env.nodeToIsolate, env.otherNodes, env.disconnectMethod)

	fmt.Printf("waiting up to %s for disconnection to affect the nexus\n", disconnectionTimeoutSecs)
	Eventually(func() string {
		return common.GetMsvState(env.uuid)
	},
		disconnectionTimeoutSecs, // timeout
		"1s",                     // polling interval
	).Should(Equal("degraded"))

	fmt.Printf("volume is in state \"%s\"\n", common.GetMsvState(env.uuid))

	fmt.Printf("running fio while node is disconnected\n")
	common.RunFio(env.fioPodName, 20)

	fmt.Printf("reconnecting \"%s\"\n", env.nodeToIsolate)
	env.ReconnectNode(true)

	fmt.Printf("running fio when node is reconnected\n")
	common.RunFio(env.fioPodName, 20)
}

// Run fio against the cluster while a replica node is being removed,
// wait for the volume to become degraded, then wait for it to be repaired.
// Run fio against repaired volume, and again after node is reconnected.
func (env *DisconnectEnv) ReplicaReassignTest() {
	// This test needs at least 4 nodes, a refuge node, a mayastor node to isolate, and 2 other mayastor nodes
	Expect(len(env.otherNodes)).To(BeNumerically(">=", 3))

	fmt.Printf("running spawned fio\n")
	go common.RunFio(env.fioPodName, 20)

	time.Sleep(5 * time.Second)
	fmt.Printf("disconnecting \"%s\"\n", env.nodeToIsolate)

	DisconnectNode(env.nodeToIsolate, env.otherNodes, env.disconnectMethod)

	fmt.Printf("waiting up to %s for disconnection to affect the nexus\n", disconnectionTimeoutSecs)
	Eventually(func() string {
		return common.GetMsvState(env.uuid)
	},
		disconnectionTimeoutSecs, // timeout
		"1s",                     // polling interval
	).Should(Equal("degraded"))

	fmt.Printf("volume is in state \"%s\"\n", common.GetMsvState(env.uuid))

	fmt.Printf("waiting up to %s for the volume to be repaired\n", repairTimeoutSecs)
	Eventually(func() string {
		return common.GetMsvState(env.uuid)
	},
		repairTimeoutSecs, // timeout
		"1s",              // polling interval
	).Should(Equal("healthy"))

	fmt.Printf("volume is in state \"%s\"\n", common.GetMsvState(env.uuid))

	fmt.Printf("running fio while node is disconnected\n")
	common.RunFio(env.fioPodName, 20)

	fmt.Printf("reconnecting \"%s\"\n", env.nodeToIsolate)
	env.ReconnectNode(true)

	fmt.Printf("running fio when node is reconnected\n")
	common.RunFio(env.fioPodName, 20)
}

// Common steps required when setting up the test.
// Creates the PVC, deploys fio, determines the nodes used by the volume
// and selects a non-nexus replica to isolate
func Setup(pvcName string, storageClassName string, fioPodName string, disconnectMethod string) DisconnectEnv {
	env := DisconnectEnv{}

	env.uuid = common.MkPVC(pvcName, storageClassName)
	env.volToDelete = pvcName
	env.storageClass = storageClassName
	env.disconnectMethod = disconnectMethod

	createFioOnRefugeNode(fioPodName, pvcName)

	fmt.Printf("waiting for fio\n")
	Eventually(func() bool {
		return common.FioReadyPod()
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))
	env.fioPodName = fioPodName

	env.nodeToIsolate, env.otherNodes = getNodes(env.uuid)
	return env
}

// Common steps required when tearing down the test
func (env *DisconnectEnv) Teardown() {
	if env.fioPodName != "" {
		fmt.Printf("removing fio pod\n")
		err := common.DeletePod(env.fioPodName)
		Expect(err).ToNot(HaveOccurred())
		env.fioPodName = ""
	}
	if env.volToDelete != "" {
		common.RmPVC(env.volToDelete, env.storageClass)
		env.volToDelete = ""
	}
}
