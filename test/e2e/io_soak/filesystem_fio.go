package io_soak

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"

	"fmt"
	"time"

	coreV1 "k8s.io/api/core/v1"
)

// IO soak filesystem fio job
type FioFsSoakJob struct {
	volName  string
	scName   string
	podName  string
	id       int
	duration int
	volUUID  string
}

func (job FioFsSoakJob) makeVolume() {
	job.volUUID = common.MkPVC(common.DefaultVolumeSizeMb, job.volName, job.scName, common.VolFileSystem, common.NSDefault)
}

func (job FioFsSoakJob) removeVolume() {
	common.RmPVC(job.volName, job.scName, common.NSDefault)
}

func (job FioFsSoakJob) makeTestPod(selector map[string]string) (*coreV1.Pod, error) {
	pod := common.CreateFioPodDef(job.podName, job.volName, common.VolFileSystem, common.NSDefault)
	pod.Spec.NodeSelector = selector

	e2eCfg := e2e_config.GetConfig()

	args := []string{
		"--",
		fmt.Sprintf("--startdelay=%d", e2eCfg.IOSoakTest.FioStartDelay),
		"--time_based",
		fmt.Sprintf("--runtime=%d", job.duration),
		fmt.Sprintf("--filename=%s", common.FioFsFilename),
		fmt.Sprintf("--thinktime=%d", GetThinkTime(job.id)),
		fmt.Sprintf("--thinktime_blocks=%d", GetThinkTimeBlocks(job.id)),
		fmt.Sprintf("--size=%dm", common.DefaultFioSizeMb),
	}
	args = append(args, GetIOSoakFioArgs()...)
	pod.Spec.Containers[0].Args = args

	pod, err := common.CreatePod(pod, common.NSDefault)
	return pod, err
}

func (job FioFsSoakJob) removeTestPod() error {
	return common.DeletePod(job.podName, common.NSDefault)
}

func (job FioFsSoakJob) getPodName() string {
	return job.podName
}

func (job FioFsSoakJob) describe() string {
	return fmt.Sprintf("pod: %s, vol: %s, volUUID: %s", job.podName, job.podName, job.volUUID)
}

func MakeFioFsJob(scName string, id int, duration time.Duration) FioFsSoakJob {
	nm := fmt.Sprintf("fio-filesystem-%s-%d", scName, id)
	return FioFsSoakJob{
		volName:  nm,
		scName:   scName,
		podName:  nm,
		id:       id,
		duration: int(duration.Seconds()),
	}
}
