package common

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"e2e-basic/common/loki"
	"e2e-basic/common/reporter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/deprecated/scheme"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type TestEnvironment struct {
	Cfg           *rest.Config
	K8sClient     client.Client
	KubeInt       kubernetes.Interface
	K8sManager    *ctrl.Manager
	TestEnv       *envtest.Environment
	DynamicClient dynamic.Interface
}

var gTestEnv TestEnvironment

// Initialise testing and setup class name + report filename.
func InitTesting(t *testing.T, classname string, reportname string) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, classname, reporter.GetReporters(reportname))
	loki.SendLokiMarker("Start of test " + classname)
}

func SetupTestEnv() {
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter)))
	By("bootstrapping test environment")
	var err error

	useCluster := true
	testEnv := &envtest.Environment{
		UseExistingCluster:       &useCluster,
		AttachControlPlaneOutput: true,
	}

	cfg, err := testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(cfg).ToNot(BeNil())

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		// We do not consume prometheus metrics.
		MetricsBindAddress: "0",
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

	k8sClient := k8sManager.GetClient()
	Expect(k8sClient).ToNot(BeNil())

	restConfig := config.GetConfigOrDie()
	Expect(restConfig).ToNot(BeNil())

	kubeInt := kubernetes.NewForConfigOrDie(restConfig)
	Expect(kubeInt).ToNot(BeNil())

	dynamicClient := dynamic.NewForConfigOrDie(restConfig)
	Expect(dynamicClient).ToNot(BeNil())

	gTestEnv = TestEnvironment{
		Cfg:           cfg,
		K8sClient:     k8sClient,
		KubeInt:       kubeInt,
		K8sManager:    &k8sManager,
		TestEnv:       testEnv,
		DynamicClient: dynamicClient,
	}
}

func TeardownTestEnvNoCleanup() {
	err := gTestEnv.TestEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
}

func TeardownTestEnv() {
	AfterSuiteCleanup()
	TeardownTestEnvNoCleanup()
}

// placeholder function for now
// To aid postmortem analysis for the most common CI use case
// namely cluster is retained aon failure, we do nothing
// For other situations behaviour should be configurable
func AfterSuiteCleanup() {
	logf.Log.Info("AfterSuiteCleanup")
}

// Fit for purpose checks
// - No pods
// - No PVCs
// - No PVs
// - No MSVs
// - Mayastor pods are all healthy
// - All mayastor pools are online
// - TODO: mayastor pools usage is 0
func ResourceCheck() error {
	var errorMsg = ""

	pods, err := CheckForTestPods()
	if err != nil {
		errorMsg += fmt.Sprintf("%s %v", errorMsg, err)
	}
	if pods {
		errorMsg += " found Pods"
	}

	pvcs, err := CheckForPVCs()
	if err != nil {
		errorMsg += fmt.Sprintf("%s %v", errorMsg, err)
	}
	if pvcs {
		errorMsg += " found PersistentVolumeClaims"
	}

	pvs, err := CheckForPVs()
	if err != nil {
		errorMsg += fmt.Sprintf("%s %v", errorMsg, err)
	}
	if pvs {
		errorMsg += " found PersistentVolumes"
	}

	// Mayastor volumes
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
	msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metaV1.ListOptions{})
	if err != nil {
		errorMsg += fmt.Sprintf("%s %v", errorMsg, err)
	}
	if len(msvs.Items) != 0 {
		errorMsg += " found MayastorVolumes"
	}

	// Check that Mayastor pods are healthy no restarts or fails.
	err = CheckTestPodsHealth(NSMayastor)
	if err != nil {
		errorMsg += fmt.Sprintf("%s %v", errorMsg, err)
	}

	scs, err := CheckForStorageClasses()
	if err != nil {
		errorMsg += fmt.Sprintf("%s %v", errorMsg, err)
	}
	if scs {
		errorMsg += " found storage classes using mayastor "
	}

	err = CheckAllPoolsAreOnline()
	if err != nil {
		errorMsg += fmt.Sprintf("%s %v", errorMsg, err)
		logf.Log.Info("BeforeEachCheck: not all pools are online")
	}

	// TODO Check pools usage is 0

	if len(errorMsg) != 0 {
		return errors.New(errorMsg)
	}
	return nil
}

// The before and after each check are very similar, however functionally
//	BeforeEachCheck asserts that the state of mayastor resources is fit for the test to run
//  AfterEachCheck asserts that the state of mayastor resources has been restored.
func BeforeEachCheck() error {
	logf.Log.Info("BeforeEachCheck")
	err := ResourceCheck()
	if err != nil {
		logf.Log.Info("BeforeEachCheck failed", "error", err)
	}
	return err
}

func AfterEachCheck() error {
	logf.Log.Info("AfterEachCheck")
	err := ResourceCheck()
	if err != nil {
		logf.Log.Info("AfterEachCheck failed", "error", err)
	}
	return err
}
