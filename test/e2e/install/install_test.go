package basic_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"

	"e2e-basic/common/junit"

	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/deprecated/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var cfg *rest.Config
var k8sClient client.Client
var k8sManager ctrl.Manager
var testEnv *envtest.Environment

/// Enumerate the nodes in the k8s cluster and return
/// 1. the IP address of the master node (if one exists),
/// 2. the number of nodes labelled openebs.io/engine=mayastor
/// 3. the names of nodes labelled openebs.io/engine=mayastor
/// The assumption is that the test-registry is accessible via the IP addr of the master,
/// or any node in the cluster if the master noe does not exist
/// TODO Refine how we workout the address of the test-registry
func getTestClusterDetails() (string, string, int, []string, error) {
	var master = ""
	var nme = 0
	nodeList := coreV1.NodeList{}
	if (k8sClient.List(context.TODO(), &nodeList, &client.ListOptions{}) != nil) {
		return "", "", 0, nil, errors.New("failed to list nodes")
	}
	nodeIPs := make([]string, len(nodeList.Items))
	for ix, k8node := range nodeList.Items {
		for _, k8Addr := range k8node.Status.Addresses {
			if k8Addr.Type == coreV1.NodeInternalIP {
				nodeIPs[ix] = k8Addr.Address
				for label, value := range k8node.Labels {
					if label == "node-role.kubernetes.io/master" {
						master = k8Addr.Address
					}
					if label == "openebs.io/engine" && value == "mayastor" {
						nme++
					}
				}
			}
		}
	}

	// At least one node where mayastor can be deployed must exist
	if nme == 0 {
		return "", "", 0, nil, errors.New("no usable nodes found for the mayastor engine")
	}

	mayastorNodes := make([]string, nme)
	ix := 0
	for _, k8node := range nodeList.Items {
		for _, k8Addr := range k8node.Status.Addresses {
			if k8Addr.Type == coreV1.NodeHostName {
				for label, value := range k8node.Labels {
					if label == "openebs.io/engine" && value == "mayastor" {
						mayastorNodes[ix] = k8Addr.Address
						ix++
					}
				}
			}
		}
	}

	// Redundant check, but keep it anyway, we are writing a test after all.
	// We should have found at least one node!
	if len(nodeIPs) == 0 {
		return "", "", 0, nil, errors.New("no usable nodes found")
	}

	tag := os.Getenv("e2e_image_tag")
	if len(tag) == 0 {
		tag = "ci"
	}
	registry := os.Getenv("e2e_docker_registry")
	if len(registry) == 0 {
		// a registry was not specified
		// If there is master node, use its IP address as the registry IP address
		if len(master) != 0 {
			registry = master + ":30291"
		} else {
			/// Otherwise choose the IP address of first node in the list  as the registry IP address
			registry = nodeIPs[0] + ":30291"
		}
	}
	return tag, registry, nme, mayastorNodes, nil
}

// Encapsulate the logic to find where the deploy yamls are
func getDeployYamlDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Clean(filename + "/../../../../deploy")
}

// Create mayastor namespace
func createNamespace() {
	cmd := exec.Command("kubectl", "create", "namespace", "mayastor")
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// Helper for passing yaml from the deploy directory to kubectl
func applyDeployYaml(filename string) {
	cmd := exec.Command("kubectl", "apply", "-f", filename)
	cmd.Dir = getDeployYamlDir()
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// Encapsulate the logic to find where the templated yamls are
func getTemplateYamlDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Clean(filename + "/../deploy")
}

func generateYamls(imageTag string, registryAddress string) {
	bashcmd := fmt.Sprintf("../../../scripts/generate-deploy-yamls.sh -t ../../../test-yamls %s %s", imageTag, registryAddress)
	cmd := exec.Command("bash", "-c", bashcmd)
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// We expect this to fail a few times before it succeeds,
// so no throwing errors from here.
func mayastorReadyPodCount() int {
	var mayastorDaemonSet appsV1.DaemonSet
	if k8sClient.Get(context.TODO(), types.NamespacedName{Name: "mayastor", Namespace: "mayastor"}, &mayastorDaemonSet) != nil {
		fmt.Println("Failed to get mayastor DaemonSet")
		return -1
	}
	return int(mayastorDaemonSet.Status.NumberAvailable)
}

func moacReady() bool {
	var moacDeployment appsV1.Deployment
	if k8sClient.Get(context.TODO(), types.NamespacedName{Name: "moac", Namespace: "mayastor"}, &moacDeployment) != nil {
		logf.Log.Info("Failed to get MOAC deployment")
		return false
	}

	// { Remove/Reduce verbosity once we have fixed install test occasional failure.
	logf.Log.Info("moacDeployment.Status",
		"ObservedGeneration", moacDeployment.Status.ObservedGeneration,
		"Replicas", moacDeployment.Status.Replicas,
		"UpdatedReplicas", moacDeployment.Status.UpdatedReplicas,
		"ReadyReplicas", moacDeployment.Status.ReadyReplicas,
		"AvailableReplicas", moacDeployment.Status.AvailableReplicas,
		"UnavailableReplicas", moacDeployment.Status.UnavailableReplicas,
		"CollisionCount", moacDeployment.Status.CollisionCount)
	for ix, condition := range moacDeployment.Status.Conditions {
		logf.Log.Info("Condition", "ix", ix,
			"Status", condition.Status,
			"Type", condition.Type,
			"Message", condition.Message,
			"Reason", condition.Reason)
	}
	// }

	for _, condition := range moacDeployment.Status.Conditions {
		if condition.Type == appsV1.DeploymentAvailable {
			if condition.Status == coreV1.ConditionTrue {
				logf.Log.Info("MOAC is Available")
				return true
			}
		}
	}
	logf.Log.Info("MOAC is Not Available")
	return false
}

// create pools for the cluster
//
// TODO: Ideally there should be one way how to create pools without using
// two env variables to do a similar thing.
func createPools(mayastorNodes []string) {
	envPoolYamls := os.Getenv("e2e_pool_yaml_files")
	poolDevice := os.Getenv("e2e_pool_device")
	if len(envPoolYamls) != 0 {
		// Apply the list of externally defined pool yaml files
		// NO check is made on the status of pools
		poolYamlFiles := strings.Split(envPoolYamls, ",")
		for _, poolYaml := range poolYamlFiles {
			fmt.Println("applying ", poolYaml)
			bashcmd := "kubectl apply -f " + poolYaml
			cmd := exec.Command("bash", "-c", bashcmd)
			_, err := cmd.CombinedOutput()
			Expect(err).ToNot(HaveOccurred())
		}
	} else if len(poolDevice) != 0 {
		// Use the template file to create pools as per the devices
		// NO check is made on the status of pools
		for _, mayastorNode := range mayastorNodes {
			fmt.Println("creating pool on:", mayastorNode, " using device:", poolDevice)
			bashcmd := "NODE_NAME=" + mayastorNode + " POOL_DEVICE=" + poolDevice + " envsubst < " + "pool.yaml.template" + " | kubectl apply -f -"
			cmd := exec.Command("bash", "-c", bashcmd)
			cmd.Dir = getTemplateYamlDir()
			out, err := cmd.CombinedOutput()
			Expect(err).ToNot(HaveOccurred(), "%s", out)
		}
	} else {
		Expect(false).To(BeTrue(), "Neither e2e_pool_yaml_files nor e2e_pool_device specified")
	}
}

// Install mayastor on the cluster under test.
// We deliberately call out to kubectl, rather than constructing the client-go
// objects, so that we can verfiy the local deploy yamls are correct.
func installMayastor() {
	imageTag, registryAddress, numMayastorInstances, mayastorNodes, err := getTestClusterDetails()
	Expect(err).ToNot(HaveOccurred())
	Expect(numMayastorInstances).ToNot(Equal(0))

	fmt.Printf("tag %v, registry %v, # of mayastor instances=%v\n", imageTag, registryAddress, numMayastorInstances)

	// FIXME use absolute paths, do not depend on CWD
	createNamespace()
	applyDeployYaml("storage-class.yaml")
	applyDeployYaml("moac-rbac.yaml")
	applyDeployYaml("mayastorpoolcrd.yaml")
	applyDeployYaml("nats-deployment.yaml")
	generateYamls(imageTag, registryAddress)
	applyDeployYaml("../test-yamls/csi-daemonset.yaml")
	applyDeployYaml("../test-yamls/moac-deployment.yaml")
	applyDeployYaml("../test-yamls/mayastor-daemonset.yaml")

	// Given the yaml files and the environment described in the test readme,
	// we expect mayastor to be running on exactly numMayastorInstances nodes.
	Eventually(func() int {
		return mayastorReadyPodCount()
	},
		"180s", // timeout
		"1s",   // polling interval
	).Should(Equal(numMayastorInstances))

	// Wait for MOAC to be ready before creating the pools,
	Eventually(func() bool {
		return moacReady()
	},
		"360s", // timeout
		"2s",   // polling interval
	).Should(Equal(true))

	// Now create pools on all nodes.
	createPools(mayastorNodes)

	// Mayastor has been installed and is now ready for use.
}

func TestInstallSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(junit.ConstructJunitFileName("install-junit.xml"))
	RunSpecsWithDefaultAndCustomReporters(t, "Basic Install Suite",
		[]Reporter{junitReporter})
}

var _ = Describe("Mayastor setup", func() {
	It("should install using yamls", func() {
		installMayastor()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))

	By("bootstrapping test environment")
	useCluster := true
	testEnv = &envtest.Environment{
		UseExistingCluster:       &useCluster,
		AttachControlPlaneOutput: true,
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(cfg).ToNot(BeNil())

	k8sManager, err = ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
	})
	Expect(err).ToNot(HaveOccurred())

	go func() {
		err = k8sManager.Start(ctrl.SetupSignalHandler())
		Expect(err).ToNot(HaveOccurred())
	}()

	mgrSyncCtx, mgrSyncCtxCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer mgrSyncCtxCancel()
	if synced := k8sManager.GetCache().WaitForCacheSync(mgrSyncCtx); !synced {
		fmt.Println("Failed to sync")
	}

	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).ToNot(BeNil())

	close(done)
}, 60)

var _ = AfterSuite(func() {
	// NB This only tears down the local structures for talking to the cluster,
	// not the kubernetes cluster itself.
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})
