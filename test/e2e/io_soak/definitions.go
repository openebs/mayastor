package io_soak

import (
	"e2e-basic/common"
	"k8s.io/api/core/v1"
	"time"
)

const NSDisrupt = common.NSE2EPrefix + "-iosoak-disrupt"

const NodeSelectorKey = "e2e-io-soak"
const NodeSelectorAppValue = "e2e-app"
const PodReadyTime = 5

var AppNodeSelector = map[string]string{
	NodeSelectorKey: NodeSelectorAppValue,
}

type IoSoakJob interface {
	makeVolume()
	makeTestPod(map[string]string) (*v1.Pod, error)
	removeTestPod() error
	removeVolume()
	run(time.Duration, chan<- string, chan<- error)
	getPodName() string
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
