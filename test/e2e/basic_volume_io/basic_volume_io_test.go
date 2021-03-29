// JIRA: CAS-505
// JIRA: CAS-506
package basic_volume_io_test

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	coreV1 "k8s.io/api/core/v1"
	storageV1 "k8s.io/api/storage/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var defTimeoutSecs = "120s"

func TestBasicVolumeIO(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Basic volume IO tests, NVMe-oF TCP and iSCSI", "basic-volume-io")
}

func basicVolumeIOTest(protocol common.ShareProto, volumeType common.VolumeType, mode storageV1.VolumeBindingMode) {
	params := e2e_config.GetConfig().BasicVolumeIO
	logf.Log.Info("Test", "parameters", params)
	scName := strings.ToLower(fmt.Sprintf("basic-vol-io-repl-%d-%s-%s-%s", params.Replicas, string(protocol), volumeType, mode))
	err := common.MakeStorageClass(scName, params.Replicas, protocol, common.NSDefault, &mode)
	Expect(err).ToNot(HaveOccurred(), "Creating storage class %s", scName)

	volName := strings.ToLower(fmt.Sprintf("basic-vol-io-repl-%d-%s-%s-%s", params.Replicas, string(protocol), volumeType, mode))

	// Create the volume
	uid := common.MkPVC(params.VolSizeMb, volName, scName, volumeType, common.NSDefault)
	logf.Log.Info("Volume", "uid", uid)

	// Create the fio Pod
	fioPodName := "fio-" + volName
	pod := common.CreateFioPodDef(fioPodName, volName, volumeType, common.NSDefault)
	Expect(pod).ToNot(BeNil())

	var args = []string{
		"--",
	}
	switch volumeType {
	case common.VolFileSystem:
		args = append(args, fmt.Sprintf("--filename=%s", common.FioFsFilename))
		args = append(args, fmt.Sprintf("--size=%dm", params.FsVolSizeMb))
	case common.VolRawBlock:
		args = append(args, fmt.Sprintf("--filename=%s", common.FioBlockFilename))
	}
	args = append(args, common.GetFioArgs()...)
	logf.Log.Info("fio", "arguments", args)
	pod.Spec.Containers[0].Args = args

	pod, err = common.CreatePod(pod, common.NSDefault)
	Expect(err).ToNot(HaveOccurred())
	Expect(pod).ToNot(BeNil())

	// Wait for the fio Pod to transition to running
	Eventually(func() bool {
		return common.IsPodRunning(fioPodName, common.NSDefault)
	},
		defTimeoutSecs,
		"1s",
	).Should(Equal(true))
	logf.Log.Info("fio test pod is running.")

	logf.Log.Info("Waiting for run to complete", "timeout", params.FioTimeout)
	tSecs := 0
	var phase coreV1.PodPhase
	for {
		if tSecs > params.FioTimeout {
			break
		}
		time.Sleep(1 * time.Second)
		tSecs += 1
		phase, err = common.CheckPodCompleted(fioPodName, common.NSDefault)
		Expect(err).To(BeNil(), "CheckPodComplete got error %s", err)
		if phase != coreV1.PodRunning {
			break
		}
	}
	Expect(phase == coreV1.PodSucceeded).To(BeTrue(), "fio pod phase is %s", phase)
	logf.Log.Info("fio completed", "duration", tSecs)

	// Delete the fio pod
	err = common.DeletePod(fioPodName, common.NSDefault)
	Expect(err).ToNot(HaveOccurred())

	// Delete the volume
	common.RmPVC(volName, scName, common.NSDefault)

	err = common.RmStorageClass(scName)
	Expect(err).ToNot(HaveOccurred(), "Deleting storage class %s", scName)
}

var _ = Describe("Mayastor Volume IO test", func() {

	BeforeEach(func() {
		// Check ready to run
		err := common.BeforeEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		// Check resource leakage.
		err := common.AfterEachCheck()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should verify an NVMe-oF TCP volume can process IO on a Filesystem volume with immediate binding", func() {
		basicVolumeIOTest(common.ShareProtoNvmf, common.VolFileSystem, storageV1.VolumeBindingImmediate)
	})

	It("should verify an NVMe-oF TCP volume can process IO on a Raw Block volume with immediate binding", func() {
		basicVolumeIOTest(common.ShareProtoNvmf, common.VolRawBlock, storageV1.VolumeBindingImmediate)
	})

	It("should verify an NVMe-oF TCP volume can process IO on a Filesystem volume with delayed binding", func() {
		basicVolumeIOTest(common.ShareProtoNvmf, common.VolFileSystem, storageV1.VolumeBindingWaitForFirstConsumer)
	})

	It("should verify an NVMe-oF TCP volume can process IO on a Raw Block volume with delayed binding", func() {
		basicVolumeIOTest(common.ShareProtoNvmf, common.VolRawBlock, storageV1.VolumeBindingWaitForFirstConsumer)
	})

	It("should verify an iSCSI volume can process IO on a Filesystem volume with immediate binding", func() {
		basicVolumeIOTest(common.ShareProtoIscsi, common.VolFileSystem, storageV1.VolumeBindingImmediate)
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
