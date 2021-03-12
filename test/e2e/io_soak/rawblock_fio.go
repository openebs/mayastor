package io_soak

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"

	"fmt"
	"time"

	coreV1 "k8s.io/api/core/v1"
)

// IO soak raw block fio  job

type FioRawBlockSoakJob struct {
	volName string
	scName  string
	podName string
	id      int
	duration int
}

func (job FioRawBlockSoakJob) makeVolume() {
	common.MkPVC(common.DefaultVolumeSizeMb, job.volName, job.scName, common.VolRawBlock, common.NSDefault)
}

func (job FioRawBlockSoakJob) removeVolume() {
	common.RmPVC(job.volName, job.scName, common.NSDefault)
}

func (job FioRawBlockSoakJob) makeTestPod(selector map[string]string) (*coreV1.Pod, error) {
	pod := common.CreateFioPodDef(job.podName, job.volName, common.VolRawBlock, common.NSDefault)
	pod.Spec.NodeSelector = selector

	image := "" + e2e_config.GetConfig().Registry + "/mayastor/e2e-fio"
	pod.Spec.Containers[0].Image = image

	args := []string{
		"--",
		"--time_based",
		fmt.Sprintf("--runtime=%d", job.duration),
		fmt.Sprintf("--filename=%s", common.FioBlockFilename),
		fmt.Sprintf("--thinktime=%d", GetThinkTime(job.id)),
		fmt.Sprintf("--thinktime_blocks=%d", GetThinkTimeBlocks(job.id)),
	}
	args = append(args, FioArgs...)
	pod.Spec.Containers[0].Args = args

	pod, err := common.CreatePod(pod, common.NSDefault)
	return pod, err
}

func (job FioRawBlockSoakJob) removeTestPod() error {
	return common.DeletePod(job.podName, common.NSDefault)
}

func (job FioRawBlockSoakJob) run(duration time.Duration, doneC chan<- string, errC chan<- error) {
	thinkTime := 1 // 1 microsecond
	thinkTimeBlocks := 1000

	FioDutyCycles := e2e_config.GetConfig().IOSoakTest.FioDutyCycles
	if len(FioDutyCycles) != 0 {
		ixp := job.id % len(FioDutyCycles)
		thinkTime = FioDutyCycles[ixp].ThinkTime
		thinkTimeBlocks = FioDutyCycles[ixp].ThinkTimeBlocks
	}

	RunIoSoakFio(
		job.podName,
		duration,
		thinkTime,
		thinkTimeBlocks,
		common.VolRawBlock,
		doneC,
		errC,
	)
}

func (job FioRawBlockSoakJob) getPodName() string {
	return job.podName
}

func MakeFioRawBlockJob(scName string, id int, duration time.Duration ) FioRawBlockSoakJob {
	nm := fmt.Sprintf("fio-rawblock-%s-%d", scName, id)
	return FioRawBlockSoakJob{
		volName: nm,
		scName:  scName,
		podName: nm,
		id:      id,
		duration: int(duration.Seconds()),
	}
}
