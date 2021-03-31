package basic_test

import (
	"e2e-basic/common"
	"e2e-basic/common/e2e_config"
	"e2e-basic/common/locations"
	"time"

	"fmt"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func generateYamlFiles(imageTag string, mayastorNodes []string, e2eCfg *e2e_config.E2EConfig) {
	coresDirective := ""
	if e2eCfg.Cores != 0 {
		coresDirective = fmt.Sprintf("%s -c %d", coresDirective, e2eCfg.Cores)
	}

	poolDirectives := ""
	if len(e2eCfg.PoolDevice) != 0 {
		poolDevice := e2eCfg.PoolDevice
		for _, mayastorNode := range mayastorNodes {
			poolDirectives += fmt.Sprintf(" -p '%s,%s'", mayastorNode, poolDevice)
		}
	}

	registryDirective := ""
	if len(e2eCfg.Registry) != 0 {
		registryDirective = fmt.Sprintf(" -r '%s'", e2eCfg.Registry)
	}

	bashCmd := fmt.Sprintf(
		"%s/generate-deploy-yamls.sh -o %s -t '%s' %s %s %s test",
		locations.GetMayastorScriptsDir(),
		locations.GetGeneratedYamlsDir(),
		imageTag, registryDirective, coresDirective, poolDirectives,
	)
	cmd := exec.Command("bash", "-c", bashCmd)
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// create pools for the cluster
//
// TODO: Ideally there should be one way how to create pools without using
// two env variables to do a similar thing.
func createPools(e2eCfg *e2e_config.E2EConfig) {
	poolYamlFiles := e2eCfg.PoolYamlFiles
	poolDevice := e2eCfg.PoolDevice
	// TODO: It is an error if configuration specifies both
	//	- pool device
	//	- pool yaml files,
	// this simple code does not resolve that use case.
	if len(poolYamlFiles) != 0 {
		// Apply the list of externally defined pool yaml files
		// NO check is made on the status of pools
		for _, poolYaml := range poolYamlFiles {
			logf.Log.Info("applying ", "yaml", poolYaml)
			bashCmd := "kubectl apply -f " + poolYaml
			cmd := exec.Command("bash", "-c", bashCmd)
			out, err := cmd.CombinedOutput()
			Expect(err).ToNot(HaveOccurred(), "%s", out)
		}
	} else if len(poolDevice) != 0 {
		// Use the generated file to create pools as per the devices
		// NO check is made on the status of pools
		common.KubeCtlApplyYaml("pool.yaml", locations.GetGeneratedYamlsDir())
	} else {
		Expect(false).To(BeTrue(), "Neither pool yaml files nor pool device specified")
	}
}

// Install mayastor on the cluster under test.
// We deliberately call out to kubectl, rather than constructing the client-go
// objects, so that we can verify the local deploy yaml files are correct.
func installMayastor() {
	e2eCfg := e2e_config.GetConfig()

	Expect(e2eCfg.ImageTag).ToNot(BeEmpty(),
		"mayastor image tag not defined")
	Expect(e2eCfg.PoolDevice != "" || len(e2eCfg.PoolYamlFiles) != 0).To(BeTrue(),
		"configuration error pools are not defined.")
	Expect(e2eCfg.PoolDevice == "" || len(e2eCfg.PoolYamlFiles) == 0).To(BeTrue(),
		"Unable to resolve pool definitions if both pool device and pool yaml files are defined")

	imageTag := e2eCfg.ImageTag
	registry := e2eCfg.Registry

	nodes, err := common.GetNodeLocs()
	Expect(err).ToNot(HaveOccurred())

	var mayastorNodes []string
	numMayastorInstances := 0

	for _, node := range nodes {
		if node.MayastorNode && !node.MasterNode {
			mayastorNodes = append(mayastorNodes, node.NodeName)
			numMayastorInstances += 1
		}
	}
	Expect(numMayastorInstances).ToNot(Equal(0))

	logf.Log.Info("Install", "tag", imageTag, "registry", registry, "# of mayastor instances", numMayastorInstances)

	generateYamlFiles(imageTag, mayastorNodes, &e2eCfg)
	deployDir := locations.GetMayastorDeployDir()
	yamlsDir := locations.GetGeneratedYamlsDir()

	err = common.MkNamespace(common.NSMayastor)
	Expect(err).ToNot(HaveOccurred())
	common.KubeCtlApplyYaml("moac-rbac.yaml", yamlsDir)
	common.KubeCtlApplyYaml("mayastorpoolcrd.yaml", deployDir)
	common.KubeCtlApplyYaml("nats-deployment.yaml", yamlsDir)
	common.KubeCtlApplyYaml("csi-daemonset.yaml", yamlsDir)
	common.KubeCtlApplyYaml("moac-deployment.yaml", yamlsDir)
	common.KubeCtlApplyYaml("mayastor-daemonset.yaml", yamlsDir)

	ready, err := common.MayastorReady(2, 540)
	Expect(err).ToNot(HaveOccurred())
	Expect(ready).To(BeTrue())

	// Now create pools on all nodes.
	createPools(&e2eCfg)

	// Wait for pools to be online
	const timoSecs = 120
	const timoSleepSecs = 10
	for ix := 0; ix < timoSecs/timoSleepSecs; ix++ {
		time.Sleep(timoSleepSecs * time.Second)
		err = common.CheckAllPoolsAreOnline(); if err == nil {
			break
		}
	}
	Expect(err).To(BeNil(), "One or more pools are offline")
	// Mayastor has been installed and is now ready for use.
}

func TestInstallSuite(t *testing.T) {
	// Initialise test and set class and file names for reports
	common.InitTesting(t, "Basic Install Suite", "install")
}

var _ = Describe("Mayastor setup", func() {
	It("should install using yaml files", func() {
		installMayastor()
	})
})

var _ = BeforeSuite(func(done Done) {
	common.SetupTestEnv()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	common.TeardownTestEnvNoCleanup()
})
