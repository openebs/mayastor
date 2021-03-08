// JIRA: MQ-25
// JIRA: MQ-26
package io_soak

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"
	rep "e2e-basic/common/reporter"

	"fmt"
	"sort"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	coreV1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var defTimeoutSecs = "120s"

type IoSoakJob interface {
	makeVolume()
	makeTestPod() (*coreV1.Pod, error)
	removeTestPod() error
	removeVolume()
	run(time.Duration, chan<- string, chan<- error)
	getPodName() string
}

var scNames []string
var jobs []IoSoakJob

func TestIOSoak(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "IO soak test, NVMe-oF TCP and iSCSI", rep.GetReporters("io-soak"))
}

func monitor(errC chan<- error) {
	logf.Log.Info("IOSoakTest monitor, checking mayastor and test pods")
	for {
		time.Sleep(30 * time.Second)
		err := common.CheckPods(common.NSMayastor)
		if err != nil {
			logf.Log.Info("IOSoakTest monitor", "namespace", common.NSMayastor, "error", err)
			errC <- err
			break
		}
		err = common.CheckPods("default")
		if err != nil {
			logf.Log.Info("IOSoakTest monitor", "namespace", "default", "error", err)
			errC <- err
			break
		}
	}
}

/// proto - protocol "nvmf" or "isci"
/// replicas - number of replicas for each volume
/// loadFactor - number of volumes for each mayastor instance
func IOSoakTest(protocols []common.ShareProto, replicas int, loadFactor int, duration time.Duration) {
	nodeList, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())

	numMayastorNodes := 0
	jobCount := 0
	sort.Slice(nodeList, func(i, j int) bool { return nodeList[i].NodeName < nodeList[j].NodeName })
	for i, node := range nodeList {
		if node.MayastorNode && !node.MasterNode {
			logf.Log.Info("MayastorNode", "name", node.NodeName, "index", i)
			jobCount += loadFactor
			numMayastorNodes += 1
		}
	}

	Expect(replicas <= numMayastorNodes).To(BeTrue())
	logf.Log.Info("IOSoakTest", "jobs", jobCount, "volumes", jobCount, "test pods", jobCount)

	for _, proto := range protocols {
		scName := fmt.Sprintf("io-soak-%s", proto)
		logf.Log.Info("Creating", "storage class", scName)
		err = common.MkStorageClass(scName, replicas, proto)
		Expect(err).ToNot(HaveOccurred())
		scNames = append(scNames, scName)
	}

	// Create the set of jobs
	idx := 1
	for idx <= jobCount {
		for _, scName := range scNames {
			if idx > jobCount {
				break
			}
			logf.Log.Info("Creating", "job", "fio filesystem job", "id", idx)
			jobs = append(jobs, MakeFioFsJob(scName, idx))
			idx++

			if idx > jobCount {
				break
			}
			logf.Log.Info("Creating", "job", "fio raw block job", "id", idx)
			jobs = append(jobs, MakeFioRawBlockJob(scName, idx))
			idx++
		}
	}

	logf.Log.Info("Creating volumes")
	// Create the job volumes
	for _, job := range jobs {
		job.makeVolume()
	}

	logf.Log.Info("Creating test pods")
	// Create the job test pods
	for _, job := range jobs {
		pod, err := job.makeTestPod()
		Expect(err).ToNot(HaveOccurred())
		Expect(pod).ToNot(BeNil())
	}

	logf.Log.Info("Waiting for test pods to be ready")
	// Wait for the test pods to be ready
	for _, job := range jobs {
		// Wait for the test Pod to transition to running
		Eventually(func() bool {
			return common.IsPodRunning(job.getPodName())
		},
			defTimeoutSecs,
			"1s",
		).Should(Equal(true))
	}

	logf.Log.Info("Starting test execution in all test pods")
	// Run the test jobs
	doneC, errC := make(chan string), make(chan error)
	go monitor(errC)
	for _, job := range jobs {
		go job.run(duration, doneC, errC)
	}

	logf.Log.Info("Waiting for test execution to complete on all test pods")
	// Wait and check that all test pods have executed successfully
	for range jobs {
		select {
		case podName := <-doneC:
			logf.Log.Info("Completed", "pod", podName)
		case err := <-errC:
			close(doneC)
			logf.Log.Info("fio run error", "error", err)
			Expect(err).To(BeNil())
		}
	}

	logf.Log.Info("All runs complete, deleting test pods")
	for _, job := range jobs {
		err := job.removeTestPod()
		Expect(err).ToNot(HaveOccurred())
	}

	logf.Log.Info("All runs complete, deleting volumes")
	for _, job := range jobs {
		job.removeVolume()
	}

	logf.Log.Info("All runs complete, deleting storage classes")
	for _, scName := range scNames {
		err = common.RmStorageClass(scName)
		Expect(err).ToNot(HaveOccurred())
	}
}

var _ = Describe("Mayastor Volume IO test", func() {

	AfterEach(func() {
		logf.Log.Info("AfterEach")
		// Check resource leakage.
		err := common.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should verify an NVMe-oF TCP volume can process IO on multiple volumes simultaneously", func() {
		e2eCfg := e2e_config.GetConfig()
		loadFactor := e2eCfg.IOSoakTest.LoadFactor
		replicas := e2eCfg.IOSoakTest.Replicas
		strProtocols := e2eCfg.IOSoakTest.Protocols
		var protocols []common.ShareProto
		for _, proto := range strProtocols {
			protocols = append(protocols, common.ShareProto(proto))
		}
		duration, err := time.ParseDuration(e2eCfg.IOSoakTest.Duration)
		Expect(err).ToNot(HaveOccurred(), "Duration configuration string format is invalid.")
		logf.Log.Info("Parameters", "replicas", replicas, "loadFactor", loadFactor, "duration", duration)
		IOSoakTest(protocols, replicas, loadFactor, duration)
	})
})

var _ = BeforeSuite(func(done Done) {
	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.	By("tearing down the test environment")
	common.TeardownTestEnv()
})
