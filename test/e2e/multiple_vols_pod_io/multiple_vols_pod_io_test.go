package multiple_vols_pod_io_test

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	coreV1 "k8s.io/api/core/v1"
	storageV1 "k8s.io/api/storage/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var defTimeoutSecs = "120s"

func TestMultipleVolumeIO(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Multiple volumes single pod IO tests", "multiple-volume-pod-io")
}

func multipleVolumeIOTest(replicas int, volumeCount int, protocol common.ShareProto, volumeType common.VolumeType, binding storageV1.VolumeBindingMode) {
	logf.Log.Info("MultipleVolumeIOTest", "replicas", replicas, "volumeCount", volumeCount, "protocol", protocol, "volumeType", volumeType, "binding", binding)
	scName := strings.ToLower(fmt.Sprintf("msv-repl-%d-%s-%s-%s", replicas, string(protocol), volumeType, binding))
	err := common.MakeStorageClass(scName, replicas, protocol, common.NSDefault, &binding)
	Expect(err).ToNot(HaveOccurred(), "Creating storage class %s", scName)

	var volNames []string
	var volumes []coreV1.Volume
	var volMounts []coreV1.VolumeMount
	var volDevices []coreV1.VolumeDevice
	var fioFiles []string

	// Create the volumes and associated bits
	for ix := 1; ix <= volumeCount; ix += 1 {
		volName := fmt.Sprintf("ms-vol-%s-%d", protocol, ix)
		uid := common.MkPVC(common.DefaultVolumeSizeMb, volName, scName, volumeType, common.NSDefault)
		logf.Log.Info("Volume", "uid", uid)
		volNames = append(volNames, volName)

		vol := coreV1.Volume{
			Name: fmt.Sprintf("ms-volume-%d", ix),
			VolumeSource: coreV1.VolumeSource{
				PersistentVolumeClaim: &coreV1.PersistentVolumeClaimVolumeSource{
					ClaimName: volName,
				},
			},
		}

		volumes = append(volumes, vol)

		if volumeType == common.VolFileSystem {
			mount := coreV1.VolumeMount{
				Name:      fmt.Sprintf("ms-volume-%d", ix),
				MountPath: fmt.Sprintf("/volume-%d", ix),
			}
			volMounts = append(volMounts, mount)
			fioFiles = append(fioFiles, fmt.Sprintf("/volume-%d/fio-test-file", ix))
		} else {
			device := coreV1.VolumeDevice{
				Name:       fmt.Sprintf("ms-volume-%d", ix),
				DevicePath: fmt.Sprintf("/dev/sdm-%d", ix),
			}
			volDevices = append(volDevices, device)
			fioFiles = append(fioFiles, fmt.Sprintf("/dev/sdm-%d", ix))
		}
	}

	logf.Log.Info("Volumes created")
	// Create the fio Pod
	fioPodName := "fio-multi-vol"
	var fioSize int
	pod := common.CreateFioPodDef(fioPodName, "aa", volumeType, common.NSDefault)
	pod.Spec.Volumes = volumes
	switch volumeType {
	case common.VolFileSystem:
		pod.Spec.Containers[0].VolumeMounts = volMounts
		fioSize = common.DefaultFioSizeMb
	case common.VolRawBlock:
		pod.Spec.Containers[0].VolumeDevices = volDevices
		fioSize = 0
	}

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

	for _, fioFile := range fioFiles {
		_, err := common.RunFio(fioPodName, 20, fioFile, fioSize)
		Expect(err).ToNot(HaveOccurred())
	}

	// Delete the fio pod
	err = common.DeletePod(fioPodName, common.NSDefault)
	Expect(err).ToNot(HaveOccurred())

	// Delete the volumes
	for _, volName := range volNames {
		common.RmPVC(volName, scName, common.NSDefault)
	}

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

	cfg := e2e_config.GetConfig().MultipleVolumesPodIO

	logf.Log.Info("MultipleVolumeIO test", "configuration", cfg)

	// TODO: enable all test cases.

	// It("should verify mayastor can process IO on multiple filesystem volumes with 1 replica mounted on a single pod with immediate binding", func() {
	//		multipleVolumeIOTest(1, cfg.VolumeCount, common.ShareProtoNvmf, common.VolFileSystem, storageV1.VolumeBindingImmediate)
	//  })

	//	It("should verify mayastor can process IO on multiple raw block volumes with 1 replica mounted on a single pod with immediate binding", func() {
	//		multipleVolumeIOTest(1, cfg.VolumeCount, common.ShareProtoNvmf, common.VolRawBlock, storageV1.VolumeBindingImmediate)
	//	})

	//	It("should verify mayastor can process IO on multiple filesystem volumes with 1 replica mounted on a single pod with late binding", func() {
	//		multipleVolumeIOTest(1, cfg.VolumeCount, common.ShareProtoNvmf, common.VolFileSystem, storageV1.VolumeBindingWaitForFirstConsumer)
	//	})

	//	It("should verify mayastor can process IO on multiple raw block volumes with 1 replica mounted on a single pod with late binding", func() {
	//		multipleVolumeIOTest(1, cfg.VolumeCount, common.ShareProtoNvmf, common.VolRawBlock, storageV1.VolumeBindingWaitForFirstConsumer)
	//	})

	It("should verify mayastor can process IO on multiple filesystem volumes with multiple replicas mounted on a single pod with immediate binding", func() {
		multipleVolumeIOTest(cfg.MultipleReplicaCount, cfg.VolumeCount, common.ShareProtoNvmf, common.VolFileSystem, storageV1.VolumeBindingImmediate)
	})

	It("should verify mayastor can process IO on multiple raw block volumes with multiple replicas mounted on a single pod with immediate binding", func() {
		multipleVolumeIOTest(cfg.MultipleReplicaCount, cfg.VolumeCount, common.ShareProtoNvmf, common.VolRawBlock, storageV1.VolumeBindingImmediate)
	})

	//	It("should verify mayastor can process IO on multiple filesystem volumes with multiple replicas mounted on a single pod with late binding", func() {
	//		multipleVolumeIOTest(cfg.MultipleReplicaCount, cfg.VolumeCount, common.ShareProtoNvmf, common.VolFileSystem, storageV1.VolumeBindingWaitForFirstConsumer)
	//	})

	//	It("should verify mayastor can process IO on multiple raw block volumes with multiple replicas mounted on a single pod with late binding", func() {
	//		multipleVolumeIOTest(cfg.MultipleReplicaCount, cfg.VolumeCount, common.ShareProtoNvmf, common.VolRawBlock, storageV1.VolumeBindingWaitForFirstConsumer)
	//	})

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
