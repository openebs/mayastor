package basic_test

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
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

func makeImageName(registryaddress string, registryport string, imagename string, imageversion string) string {
	return registryaddress + ":" + registryport + "/mayadata/" + imagename + ":" + imageversion
}

func applyTemplatedYaml(filename string, imagename string) {
	fullimagename := makeImageName("172.18.8.101", "30291", imagename, "ci")
	bashcmd := "IMAGE_NAME=" + fullimagename + " envsubst < " + filename + " | kubectl apply -f -"
	cmd := exec.Command("bash", "-c", bashcmd)
	cmd.Dir = getTemplateYamlDir()
	_, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred())
}

// We expect this to fail a few times before it succeeds,
// so no throwing errors from here.
func mayastorReadyPodCount() int {
	var mayastorDaemonSet appsv1.DaemonSet
	if k8sClient.Get(context.TODO(), types.NamespacedName{Name: "mayastor", Namespace: "mayastor"}, &mayastorDaemonSet) != nil {
		fmt.Println("Failed to get mayastor DaemonSet")
		return -1
	}
	return int(mayastorDaemonSet.Status.NumberAvailable)
}

func moacReadyPodCount() int {
	var moacDeployment appsv1.Deployment
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
	applyDeployYaml("namespace.yaml")
	applyDeployYaml("storage-class.yaml")
	applyDeployYaml("moac-rbac.yaml")
	applyDeployYaml("mayastorpoolcrd.yaml")
	applyDeployYaml("nats-deployment.yaml")
	applyTemplatedYaml("csi-daemonset.yaml.template", "mayastor-csi")
	applyTemplatedYaml("moac-deployment.yaml.template", "moac")
	applyTemplatedYaml("mayastor-daemonset.yaml.template", "mayastor")

	// Given the yamls and the environment described in the test readme,
	// we expect mayastor to be running on exactly 3 nodes.
	Eventually(mayastorReadyPodCount,
		"120s", // timeout
		"1s",   // polling interval
	).Should(Equal(3))

	Eventually(moacReadyPodCount(),
		"60s", // timeout
		"1s",  // polling interval
	).Should(Equal(1))

	// Now create pools on all nodes.
	// Note the disk for use on each node has been set in deploy/pool.yaml
	nodeList := corev1.NodeList{}
	if (k8sClient.List(context.TODO(), &nodeList, &client.ListOptions{}) != nil) {
		fmt.Println("Failed to list Nodes, pools not created")
		return
	}
	for _, k8node := range nodeList.Items {
		bashcmd := "NODE_NAME=" + k8node.Name + " envsubst < " + "pool.yaml" + " | kubectl apply -f -"
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
