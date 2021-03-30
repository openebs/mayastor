package locations

// For now the relative paths are hardcoded, there may be a case to make this
// more generic and data driven.

import (
	"e2e-basic/common/e2e_config"
	"os"
	"path"

	. "github.com/onsi/gomega"
)

func locationExists(path string) string {
	_, err := os.Stat(path)
	Expect(err).To(BeNil(), "%s", err)
	return path
}

func GetMayastorDeployDir() string {
	return locationExists(path.Clean(e2e_config.GetConfig().MayastorRootDir + "/deploy"))
}

func GetMayastorScriptsDir() string {
	return locationExists(path.Clean(e2e_config.GetConfig().MayastorRootDir + "/scripts"))
}

// This is a generate directory, so may not exist yet.
func GetGeneratedYamlsDir() string {
	return path.Clean(e2e_config.GetConfig().E2eRootDir + "/artifacts/install/yamls")
}
