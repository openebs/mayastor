package io_soak

import (
	"e2e-basic/common/e2e_config"
)

// see https://fio.readthedocs.io/en/latest/fio_doc.html#i-o-rate
// thinktime -  usecs, stall the job for the specified period of time after an I/O has completed before issuing the next
// thinktime_blocks - how many blocks to issue, before waiting thinktime usecs.

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

var FioArgs = []string{
	"--name=benchtest",
	"--direct=1",
	"--rw=randrw",
	"--ioengine=libaio",
	"--bs=4k",
	"--iodepth=16",
	"--numjobs=1",
	"--verify=crc32",
	"--verify_fatal=1",
	"--verify_async=2",
	"--status-interval=51",
}
