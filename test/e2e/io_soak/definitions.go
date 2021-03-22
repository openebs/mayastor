package io_soak

import (
	"e2e-basic/common"
	"k8s.io/api/core/v1"
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
	getPodName() string
	describe() string
}
