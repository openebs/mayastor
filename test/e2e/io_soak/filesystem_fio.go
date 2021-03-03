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
	volName string
	scName  string
	podName string
	id      int
}

func (job FioFsSoakJob) makeVolume() {
	common.MkPVC(job.volName, job.scName)
}

func (job FioFsSoakJob) removeVolume() {
	common.RmPVC(job.volName, job.scName)
}

func (job FioFsSoakJob) makeTestPod() (*coreV1.Pod, error) {
	pod, err := common.CreateFioPod(job.podName, job.volName)
	return pod, err
}

func (job FioFsSoakJob) removeTestPod() error {
	return common.DeletePod(job.podName)
}

func (job FioFsSoakJob) run(duration time.Duration, doneC chan<- string, errC chan<- error) {
	FioDutyCycles := e2e_config.GetConfig().IOSoakTest.FioDutyCycles
	ixp := job.id % len(FioDutyCycles)
	RunIoSoakFio(
		job.podName,
		duration,
		FioDutyCycles[ixp].ThinkTime,
		FioDutyCycles[ixp].ThinkTimeBlocks,
		false,
		doneC,
		errC,
	)
}

func (job FioFsSoakJob) getPodName() string {
	return job.podName
}

func (job FioFsSoakJob) getId() int {
	return job.id
}

func MakeFioFsJob(scName string, id int) FioFsSoakJob {
	nm := fmt.Sprintf("fio-filesystem-%s-%d", scName, id)
	return FioFsSoakJob{
		volName: nm,
		scName:  scName,
		podName: nm,
		id:      id,
	}
}
