package e2e_config

import (
	"fmt"
	"github.com/ilyakaznacheev/cleanenv"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"sync"
)

// E2EConfig is a application configuration structure
type E2EConfig struct {
	// Operational parameters
	Cores         int      `yaml:"cores,omitempty"`
	Registry      string   `yaml:"registry" env:"e2e_docker_registry" env-default:"ci-registry.mayastor-ci.mayadata.io"`
	ImageTag      string   `yaml:"imageTag" env:"e2e_image_tag" env-default:"ci"`
	PoolDevice    string   `yaml:"poolDevice" env:"e2e_pool_device"`
	PoolYamlFiles []string `yaml:"poolYamlFiles" env:"e2e_pool_yaml_files"`
	// Individual Test parameters
	PVCStress struct {
		CdCycles   int `yaml:"cdCycles" env-default:"100"`
		CrudCycles int `yaml:"crudCycles" env-default:"20"`
	}
	IOSoakTest struct {
		Replicas         int      `yaml:"replicas" env-default:"2"`
		Duration         string   `yaml:"duration" env-default:"30m"`
		LoadFactor       int      `yaml:"loadFactor" env-default:"10"`
		Protocols        []string `yaml:"protocols" env-default:"nvmf,iscsi"`
		FioFixedDuration int      `yaml:"fioFixedDuration" env-default:"60"`
		FioDutyCycles    []struct {
			ThinkTime       int `yaml:"thinkTime"`
			ThinkTimeBlocks int `yaml:"thinkTimeBlocks"`
		} `yaml:"fioDutyCycles"`
	} `yaml:"ioSoakTest"`
	CSI struct {
		SmallClaimSize string `yaml:"smallClaimSize" env-default:"50Mi"`
		LargeClaimSize string `yaml:"largeClaimSize" env-default:"500Mi"`
	}
	Uninstall struct {
		Cleanup int `yaml:"cleanup" env:"e2e_uninstall_cleanup"`
	} `yaml:"uninstall"`
	// Run configuration
	ReportsDir string `yaml:"reportsDir" env:"e2e_reports_dir"`
}

var once sync.Once
var e2eConfig E2EConfig

// This works because *ALL* tests source directories are 1 level deep.
const configDir = "../configurations"

func configFileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else if os.IsNotExist(err) {
		fmt.Printf("Configuration file %s does not exist\n", path)
	} else {
		fmt.Printf("Configuration file %s is not accessible\n", path)
	}
	return false
}

// This function is called early from junit and various bits have not been initialised yet
// so we cannot use logf or Expect instead we use fmt.Print... and panic.
func GetConfig() E2EConfig {
	var err error
	once.Do(func() {
		// We absorb the complexity of locating the configuration file here
		// so that scripts invoking the tests can be simpler
		// - if OS envvar e2e_config is not defined the config file is defaulted to ci_e2e_config
		// - if OS envvar e2e_config is defined
		//		- if it is a path to a file then that file is used as the config file
		//		- else try to use a file of the same name in the configuration directory
		configFile := fmt.Sprintf("%s/ci_e2e_config.yaml", configDir)
		// A configuration file *MUST* be provided.
		value, ok := os.LookupEnv("e2e_config_file")
		if ok {
			if configFileExists(value) {
				configFile = value
			} else {
				configFile = fmt.Sprintf("%s/%s", configDir, value)
			}
		}
		fmt.Printf("Using configuration file %s\n", configFile)
		err = cleanenv.ReadConfig(configFile, &e2eConfig)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}

		cfgBytes, _ := yaml.Marshal(e2eConfig)
		_ = ioutil.WriteFile("../../../artifacts/e2e_config.used.yaml", cfgBytes, 0444)
	})

	return e2eConfig
}
