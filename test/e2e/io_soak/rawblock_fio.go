package io_soak

import (
	"e2e-basic/common"

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
}

func (job FioRawBlockSoakJob) makeVolume() {
	common.MkRawBlockPVC(job.volName, job.scName)
}

func (job FioRawBlockSoakJob) removeVolume() {
	common.RmPVC(job.volName, job.scName)
}

func (job FioRawBlockSoakJob) makeTestPod() (*coreV1.Pod, error) {
	pod, err := common.CreateRawBlockFioPod(job.podName, job.volName)
	return pod, err
}

func (job FioRawBlockSoakJob) removeTestPod() error {
	return common.DeletePod(job.podName)
}

func (job FioRawBlockSoakJob) run(duration time.Duration, doneC chan<- string, errC chan<- error) {
	ixp := job.id % len(FioDutyCycles)
	RunIoSoakFio(
		job.podName,
		duration,
		FioDutyCycles[ixp].thinkTime,
		FioDutyCycles[ixp].thinkTimeBlocks,
		true,
		doneC,
		errC,
	)
}

func (job FioRawBlockSoakJob) getPodName() string {
	return job.podName
}

func (job FioRawBlockSoakJob) getId() int {
	return job.id
}

func MakeFioRawBlockJob(scName string, id int) FioRawBlockSoakJob {
	nm := fmt.Sprintf("fio-rawblock-%s-%d", scName, id)
	return FioRawBlockSoakJob{
		volName: nm,
		scName:  scName,
		podName: nm,
		id:      id,
	}
}
