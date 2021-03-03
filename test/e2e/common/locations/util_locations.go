package locations

import (
	"os"
	"path"
	"runtime"
	"sync"

	. "github.com/onsi/gomega"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var once sync.Once
var topDir string

func init() {
	once.Do(func() {
		value, ok := os.LookupEnv("e2e_top_dir")
		if !ok {
			_, filename, _, _ := runtime.Caller(0)
			topDir = path.Clean(filename + "/../../../../")
		} else {
			topDir = value
		}
		logf.Log.Info("Repo", "top directory", topDir)
	})
}

func locationExists(path string) string {
	_, err := os.Stat(path)
	Expect(err).To(BeNil(), "%s", err)
	return path
}

func GetDeployDir() string {
	return locationExists(path.Clean(topDir + "/deploy"))
}

func GetScriptsDir() string {
	return locationExists(path.Clean(topDir + "/scripts"))
}

func GetArtifactsDir() string {
	return path.Clean(topDir + "/artifacts")
}

// This is a generate directory, so may not exist yet.
func GetGeneratedYamlsDir() string {
	return path.Clean(topDir + "/artifacts/install/yamls")
}
