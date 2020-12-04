package node_disconnect_lib

import (
	"e2e-basic/common"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/gomega"
)

var (
	defTimeoutSecs = "90s"
)

func DisconnectNode(vmname string, otherNodes []string, method string) {
	for _, targetIP := range otherNodes {
		cmd := exec.Command("bash", "../lib/io_connect_node.sh", vmname, targetIP, "DISCONNECT", method)
		cmd.Dir = "./"
		_, err := cmd.CombinedOutput()
		Expect(err).ToNot(HaveOccurred())
	}
}

func ReconnectNode(vmname string, otherNodes []string, checkError bool, method string) {
	for _, targetIP := range otherNodes {
		cmd := exec.Command("bash", "../lib/io_connect_node.sh", vmname, targetIP, "RECONNECT", method)
		cmd.Dir = "./"
		_, err := cmd.CombinedOutput()
		if checkError {
			Expect(err).ToNot(HaveOccurred())
		}
	}
}

// return the node name to isolate and a vector of IP addresses to isolate
func GetNodes(uuid string) (string, []string) {
	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())

	var nodeToIsolate = ""
	nexusNode := common.GetMsvNode(uuid)
	fmt.Printf("nexus node is \"%s\"\n", nexusNode)

	var otherAddresses []string

	// find a node which is not the nexus
	for _, node := range nodeList {
		if node.NodeName != nexusNode && node.MayastorNode == true {
			nodeToIsolate = node.NodeName
			break
		}
	}
	Expect(nodeToIsolate != "")

	// get a list of the other ip addresses in the cluster
	for _, node := range nodeList {
		if node.NodeName != nodeToIsolate {
			otherAddresses = append(otherAddresses, node.IPAddress)
		}
	}
	Expect(len(otherAddresses) != 0)

	fmt.Printf("node to isolate is \"%s\"\n", nodeToIsolate)
	return nodeToIsolate, otherAddresses
}

func LossTest(nodeToIsolate string, otherNodes []string, disconnectionMethod string, uuid string) {
	fmt.Printf("running spawned fio\n")
	go common.RunFio("fio", 20)

	time.Sleep(5 * time.Second)
	fmt.Printf("disconnecting \"%s\"\n", nodeToIsolate)

	DisconnectNode(nodeToIsolate, otherNodes, disconnectionMethod)

	fmt.Printf("waiting 60s for disconnection to affect the nexus\n")
	time.Sleep(60 * time.Second)

	fmt.Printf("running fio while node is disconnected\n")
	common.RunFio("fio", 20)

	volumeState := common.GetMsvState(uuid)
	fmt.Printf("Volume state is \"%s\"\n", volumeState)
	Expect(volumeState == "degraded")

	fmt.Printf("reconnecting \"%s\"\n", nodeToIsolate)
	ReconnectNode(nodeToIsolate, otherNodes, true, disconnectionMethod)

	fmt.Printf("running fio when node is reconnected\n")
	common.RunFio("fio", 20)
}

func Setup(pvc_name string, storage_class_name string, fio_yaml_path string) string {
	uuid := common.MkPVC(fmt.Sprintf(pvc_name), storage_class_name)

	common.ApplyDeployYaml(fio_yaml_path)

	fmt.Printf("waiting for fio\n")
	Eventually(func() bool {
		return common.FioReadyPod()
	},
		defTimeoutSecs, // timeout
		"1s",           // polling interval
	).Should(Equal(true))
	return uuid
}

func Teardown(pvc_name string, storage_class_name string, fio_yaml_path string) {

	fmt.Printf("removing fio pod\n")
	common.DeleteDeployYaml(fio_yaml_path)

	fmt.Printf("removing pvc\n")
	common.RmPVC(fmt.Sprintf(pvc_name), storage_class_name)
}
