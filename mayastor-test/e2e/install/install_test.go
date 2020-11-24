package basic_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

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
func getTestClusterDetails() (string, int, []string, error) {
	var master = ""
	var nme = 0
	nodeList := coreV1.NodeList{}
	if (k8sClient.List(context.TODO(), &nodeList, &client.ListOptions{}) != nil) {
		return master, nme, nil, errors.New("failed to list nodes")
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
		return "", 0, nil, errors.New("no usable nodes found for the mayastor engine")
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
		return "", 0, nil, errors.New("no usable nodes found")
	}

	/// TODO Refine how we workout the address of the test-registry
	/// If there is master node, use its IP address as the registry IP address
	if len(master) != 0 {
		return master, nme, mayastorNodes, nil
	}
	/// Otherwise choose the IP address of first node in the list  as the registry IP address
	return nodeIPs[0], nme, mayastorNodes, nil
}

// Encapsulate the logic to find where the deploy yamls are
func getDeployYamlDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Clean(filename + "/../../../../deploy")
}

// Helper for passing yaml from the deploy directory to kubectl
func applyDeployYaml(filename string) {
	cmd := exec.Command("kubectl", "apply", "-f", filename)
	cmd.Dir = getDeployYamlDir()
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

// Encapsulate the logic to find where the templated yamls are
func getTemplateYamlDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Clean(filename + "/../deploy")
}

func makeImageName(registryAddress string, registryport string, imagename string, imageversion string) string {
	return registryAddress + ":" + registryport + "/mayadata/" + imagename + ":" + imageversion
}

func applyTemplatedYaml(filename string, imagename string, registryAddress string) {
	fullimagename := makeImageName(registryAddress, "30291", imagename, "ci")
	bashcmd := "IMAGE_NAME=" + fullimagename + " envsubst < " + filename + " | kubectl apply -f -"
	cmd := exec.Command("bash", "-c", bashcmd)
	cmd.Dir = getTemplateYamlDir()
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
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

func moacReadyPodCount() int {
	var moacDeployment appsV1.Deployment
	if k8sClient.Get(context.TODO(), types.NamespacedName{Name: "moac", Namespace: "mayastor"}, &moacDeployment) != nil {
		fmt.Println("Failed to get MOAC deployment")
		return -1
	}
	return int(moacDeployment.Status.AvailableReplicas)
}

// Install mayastor on the cluster under test.
// We deliberately call out to kubectl, rather than constructing the client-go
// objects, so that we can verfiy the local deploy yamls are correct.
func installMayastor() {
	registryAddress, numMayastorInstances, mayastorNodes, err := getTestClusterDetails()
	Expect(err).ToNot(HaveOccurred())
	Expect(numMayastorInstances).ToNot(Equal(0))

	fmt.Printf("registry address %v, number of mayastor instances=%v\n", registryAddress, numMayastorInstances)

	applyDeployYaml("namespace.yaml")
	applyDeployYaml("storage-class.yaml")
	applyDeployYaml("moac-rbac.yaml")
	applyDeployYaml("mayastorpoolcrd.yaml")
	applyDeployYaml("nats-deployment.yaml")
	applyTemplatedYaml("csi-daemonset.yaml.template", "mayastor-csi", registryAddress)
	applyTemplatedYaml("moac-deployment.yaml.template", "moac", registryAddress)
	applyTemplatedYaml("mayastor-daemonset.yaml.template", "mayastor", registryAddress)

	// Given the yamls and the environment described in the test readme,
	// we expect mayastor to be running on exactly 2 nodes.
	Eventually(mayastorReadyPodCount,
		"120s", // timeout
		"1s",   // polling interval
	).Should(Equal(numMayastorInstances))

	Eventually(moacReadyPodCount(),
		"60s", // timeout
		"1s",  // polling interval
	).Should(Equal(1))

	// Now create pools on all nodes.
	// Note the disk for use on each node has been set in deploy/pool.yaml
	// TODO make the pool disk configurable
	for _, mayastorNode := range mayastorNodes {
		bashcmd := "NODE_NAME=" + mayastorNode + " envsubst < " + "pool.yaml" + " | kubectl apply -f -"
		cmd := exec.Command("bash", "-c", bashcmd)
		cmd.Dir = getTemplateYamlDir()
		_, err := cmd.CombinedOutput()
		Expect(err).ToNot(HaveOccurred())
	}
}

func TestInstallSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Basic Install Suite")
}

var _ = Describe("Mayastor setup", func() {
	It("should install using yamls", func() {
		installMayastor()
	})
})

var _ = BeforeSuite(func(done Done) {
	logf.SetLogger(zap.LoggerTo(GinkgoWriter, true))

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
	if synced := k8sManager.GetCache().WaitForCacheSync(mgrSyncCtx.Done()); !synced {
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
