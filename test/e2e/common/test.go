package common

import (
	"context"
	"errors"
	"fmt"
	"time"

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

func SetupTestEnv() {
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

// Check that no PVs, PVCs and MSVs are still extant.
// Returns an error if resources exists.
func AfterEachCheck() error {
	var errorMsg = ""

	logf.Log.Info("AfterEachCheck")

	// Phase 1 to delete dangling resources
	pvcs, _ := gTestEnv.KubeInt.CoreV1().PersistentVolumeClaims("default").List(context.TODO(), metaV1.ListOptions{})
	if len(pvcs.Items) != 0 {
		errorMsg += " found leftover PersistentVolumeClaims"
		logf.Log.Info("AfterEachCheck: found leftover PersistentVolumeClaims, test fails.")
	}

	pvs, _ := gTestEnv.KubeInt.CoreV1().PersistentVolumes().List(context.TODO(), metaV1.ListOptions{})
	if len(pvs.Items) != 0 {
		errorMsg += " found leftover PersistentVolumes"
		logf.Log.Info("AfterEachCheck: found leftover PersistentVolumes, test fails.")
	}

	// Mayastor volumes
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
	msvs, _ := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metaV1.ListOptions{})
	if len(msvs.Items) != 0 {
		errorMsg += " found leftover MayastorVolumes"
		logf.Log.Info("AfterEachCheck: found leftover MayastorVolumes, test fails.")
	}

	// Check that Mayastor pods are healthy no restarts or fails.
	err := CheckPods(NSMayastor)
	if err != nil {
		errorMsg = fmt.Sprintf("%s %v", errorMsg, err)
	}

	if len(errorMsg) != 0 {
		return errors.New(errorMsg)
	}
	return nil
}
