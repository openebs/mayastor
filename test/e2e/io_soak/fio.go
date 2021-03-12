package io_soak

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"

	"fmt"
	"io/ioutil"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func GetThinkTime(idx int) int {
	thinkTime := 1 // 1 microsecond
	FioDutyCycles := e2e_config.GetConfig().IOSoakTest.FioDutyCycles
	if len(FioDutyCycles) != 0 {
		ixp := idx % len(FioDutyCycles)
		thinkTime = FioDutyCycles[ixp].ThinkTime
	}
	return thinkTime
}

func GetThinkTimeBlocks(idx int) int {
	thinkTimeBlocks := 1000 // 1 microsecond
	FioDutyCycles := e2e_config.GetConfig().IOSoakTest.FioDutyCycles
	if len(FioDutyCycles) != 0 {
		ixp := idx % len(FioDutyCycles)
		thinkTimeBlocks = FioDutyCycles[ixp].ThinkTimeBlocks
	}
	return thinkTimeBlocks
}

// see https://fio.readthedocs.io/en/latest/fio_doc.html#i-o-rate
// run fio in a loop of fixed duration to fulfill a larger duration,
// this to facilitate a relatively timely termination when an error
// occurs elsewhere.
// podName - name of the fio pod
// duration - time in seconds to run fio
// thinktime -  usecs, stall the job for the specified period of time after an I/O has completed before issuing the next
// thinktime_blocks - how many blocks to issue, before waiting thinktime usecs.
// rawBlock - false for filesystem volumes, true for raw block mounts.
func RunIoSoakFio(podName string, duration time.Duration, thinkTime int, thinkTimeBlocks int, volType common.VolumeType, doneC chan<- string, errC chan<- error) {
	secs := int(duration.Seconds())
	argThinkTime := fmt.Sprintf("--thinktime=%d", thinkTime)
	argThinkTimeBlocks := fmt.Sprintf("--thinktime_blocks=%d", thinkTimeBlocks)

	logf.Log.Info("Running fio",
		"pod", podName,
		"duration", duration,
		"thinktime", thinkTime,
		"thinktime_blocks", thinkTimeBlocks,
		"volType", volType,
	)

	fioFile := ""
	if volType == common.VolRawBlock {
		fioFile = common.FioBlockFilename
	} else {
		fioFile = common.FioFsFilename
	}

	for ix := 1; secs > 0; ix++ {
		runtime := e2e_config.GetConfig().IOSoakTest.FioFixedDuration
		if runtime > secs {
			runtime = secs
		}
		secs -= runtime

		logf.Log.Info("run fio ",
			"iteration", ix,
			"pod", podName,
			"duration", runtime,
			"thinktime", thinkTime,
			"thinktime_blocks", thinkTimeBlocks,
			"volType", volType,
			"fioFile", fioFile,
		)
		output, err := common.RunFio(podName, runtime, fioFile, argThinkTime, argThinkTimeBlocks)

		//TODO: for now shove the output into /tmp
		_ = ioutil.WriteFile("/tmp/"+podName+".out", output, 0644)
		//logf.Log.Info(string(output))
		if err != nil {
			logf.Log.Info("Abort running fio", "pod", podName, "error", err)
			errC <- err
			return
		}
	}
	logf.Log.Info("Finished running fio", "pod", podName, "duration", duration)
	doneC <- podName
}
