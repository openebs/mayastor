package common

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/deprecated/scheme"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
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
	useCluster := true
	testEnv := &envtest.Environment{
		UseExistingCluster:       &useCluster,
		AttachControlPlaneOutput: true,
	}

	var err error
	cfg, err := testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(cfg).ToNot(BeNil())

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
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

func TeardownTestEnv() {
	err := gTestEnv.TestEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
}
