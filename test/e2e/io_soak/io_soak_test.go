// JIRA: MQ-25
// JIRA: MQ-26
package io_soak_test

import (
	"e2e-basic/common"
	rep "e2e-basic/common/reporter"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var defTimeoutSecs = "120s"

type volSc struct {
	volName string
	scName  string
}

var podNames []string
var volNames []volSc
var scNames []string

func TestIOSoak(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "IO soak test, NVMe-oF TCP and iSCSI", rep.GetReporters("io-soak"))
}

func runFio(podName string, duration int, tt int, ttb int, doneC chan<-string, errC chan<-error) {
	logf.Log.Info("Running fio", "pod", podName, "duration", duration)
	argRuntime := fmt.Sprintf("--runtime=%d", duration)
	argThinkTime := fmt.Sprintf("--thinktime=%d", tt)
	argThinkTimeBlocks := fmt.Sprintf("--thinktime_blocks=%d", ttb)
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
		argThinkTime,
		argThinkTimeBlocks,
		argRuntime,
	)
	cmd.Dir = ""
	output, err := cmd.CombinedOutput()
	logf.Log.Info("Finished running fio", "pod", podName, "duration", duration)
	logf.Log.Info(string(output))
	if err != nil {
		errC <- err
	} else {
		doneC <- podName
	}
}

func makeVols(scName string, count int) {
	for ix := 1; ix <= count; ix++ {
		volName := fmt.Sprintf("%s-%d", scName, ix)
		common.MkPVC(volName, scName)
		tmp := volSc{volName, scName}
		volNames = append(volNames, tmp)
	}
}

func rmVols(scName string, count int) {
	for ix := 1; ix <= count; ix++ {
		volName := fmt.Sprintf("%s-%d", scName, ix)
		common.RmPVC(volName, scName)
	}
}

/// proto - protocol "nvmf" or "isci"
/// replicas - number of replicas for each volume
/// loadFactor - number of volumes for each mayastor instance
func IOSoakTest(proto string, replicas int, loadFactor int, duration int) {

	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())

	num_mayastor_nodes := 0
	jobCount := 0
	sort.Slice(nodeList, func(i, j int) bool { return nodeList[i].NodeName < nodeList[j].NodeName })
	for i, node := range nodeList {
		if node.MayastorNode && !node.MasterNode {
			logf.Log.Info("MayastorNode", "name", node.NodeName, "index", i)
			jobCount += loadFactor
			num_mayastor_nodes += 1
		}
	}

	Expect(replicas <= num_mayastor_nodes).To(BeTrue())

	scName := fmt.Sprintf("io-soak-%s", proto)
	common.MkStorageClass(scName, replicas, proto, "io.openebs.csi-mayastor")
	scNames = append(scNames, scName)

	logf.Log.Info("IOSoakTest", "jobs", jobCount, "volumes", jobCount, "test pods", jobCount)

	// Create the required set of volumes
	makeVols(scName, jobCount)
    doneC, errC := make(chan string), make(chan error)

    // Create the test pods for the volumes
	for _, vol := range volNames {
		// Create the fio Pod
		fioPodName := "fio-" + vol.volName
		pod, err := common.CreateFioPod(fioPodName, vol.volName)
		Expect(err).ToNot(HaveOccurred())
		Expect(pod).ToNot(BeNil())
		podNames = append(podNames, fioPodName)
	}

	// Wait for the test pods to be ready
	for _, podName := range podNames  {
		// Wait for the fio Pod to transition to running
		Eventually(func() bool {
			return common.IsPodRunning(podName)
		},
			defTimeoutSecs,
			"1s",
		).Should(Equal(true))
	}

	// Execute the application in the pods concurrently.
	for x, podName := range podNames  {
		// Run the fio test
		go runFio(podName, duration, x*500, x*100, doneC, errC)
	}

	// Check that the pods have executed successfully
	for _, _ = range podNames {
        select {
        case podName := <-doneC:
			// Delete the fio pod
			err = common.DeletePod(podName)
			Expect(err).ToNot(HaveOccurred())
        case err := <-errC:
			Expect(err).ToNot(HaveOccurred())
        }
	}

	logf.Log.Info("All runs complete, removing volumes")

	rmVols(scName, jobCount)
	common.RmStorageClass(scName)
}

var _ = Describe("Mayastor Volume IO test", func() {

	AfterEach(func() {
		logf.Log.Info("AfterEach")
		// Check resource leakage.
		err := common.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should verify an NVMe-oF TCP volume can process IO on multiple volumes simultaneously", func() {
		replicas := 2
		loadFactor := 2
		duration := 30
		var err error
		tmp := os.Getenv("e2e_io_soak_load_factor")
		if tmp != "" {
			loadFactor, err = strconv.Atoi(tmp)
			Expect(err).ToNot(HaveOccurred())
		}
		tmp = os.Getenv("e2e_io_soak_duration")
		if tmp != "" {
			duration, err = strconv.Atoi(tmp)
			Expect(err).ToNot(HaveOccurred())
		}
		tmp = os.Getenv("e2e_io_soak_replicas")
		if tmp != "" {
			replicas, err = strconv.Atoi(tmp)
			Expect(err).ToNot(HaveOccurred())
		}
		logf.Log.Info("Parameters", "replicas", replicas, "loadFactor", loadFactor, "duration", duration)
		IOSoakTest("nvmf", replicas, loadFactor, duration)
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))
	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
//	common.DeleteAllPods()
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.	By("tearing down the test environment")
	common.TeardownTestEnv()
})
