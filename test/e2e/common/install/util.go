package install

import (
	"os/exec"
	"path"
	"runtime"
	"sync"

	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var once sync.Once
var rootPath string

//  directories of interest
const DeployDir = "deploy"
const ScriptsDir = "scripts"
const YamlsDir = "artifacts/test-yamls"

func getRootPath() string {
	once.Do(func() {
		_, filename, _, _ := runtime.Caller(0)
		rootPath = path.Clean(filename + "/../../../../../")
		logf.Log.Info("Repo", "rootPath", rootPath)
	})
	return rootPath
}

// Helper for passing yaml from the specified directory to kubectl
func ApplyYaml(filename string, dir string) {
	cmd := exec.Command("kubectl", "apply", "-f", filename)
	root := getRootPath()
	cmd.Dir = path.Clean(root + "/" + dir)
	logf.Log.Info("kubectl apply ", "yaml file", filename, "path", cmd.Dir)
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

// Helper for passing yaml from the specified directory to kubectl
func UnapplyYaml(filename string, dir string) {
	cmd := exec.Command("kubectl", "delete", "-f", filename)
	root := getRootPath()
	cmd.Dir = path.Clean(root + "/" + dir)
	logf.Log.Info("kubectl delete ", "yaml file", filename, "path", cmd.Dir)
	out, err := cmd.CombinedOutput()
	Expect(err).ToNot(HaveOccurred(), "%s", out)
}

func GetPathInRepo(dir string) string {
	return path.Clean(getRootPath() + "/" + dir)
}