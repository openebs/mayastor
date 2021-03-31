package e2e_config

import (
	"fmt"
	"github.com/ilyakaznacheev/cleanenv"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

// E2EConfig is a application configuration structure
type E2EConfig struct {
	ConfigName string `yaml:"configName"`
	// Generic configuration files used for CI and automation should not define MayastorRootDir and E2eRootDir
	MayastorRootDir string `yaml:"mayastorRootDir" env:"e2e_mayastor_root_dir"`
	E2eRootDir      string `yaml:"e2eRootDir" env:"e2e_root_dir"`
	// Operational parameters
	Cores int `yaml:"cores,omitempty"`
	// Registry from where mayastor images are retrieved
	Registry string `yaml:"registry" env:"e2e_docker_registry" env-default:"ci-registry.mayastor-ci.mayadata.io"`
	// Registry from where CI testing images are retrieved
	CIRegistry    string   `yaml:"ciRegistry" env:"e2e_ci_docker_registry" env-default:"ci-registry.mayastor-ci.mayadata.io"`
	ImageTag      string   `yaml:"imageTag" env:"e2e_image_tag" env-default:"ci"`
	PoolDevice    string   `yaml:"poolDevice" env:"e2e_pool_device"`
	PoolYamlFiles []string `yaml:"poolYamlFiles" env:"e2e_pool_yaml_files"`
	// Individual Test parameters
	PVCStress struct {
		Replicas   int `yaml:"replicas" env-default:"1"`
		CdCycles   int `yaml:"cdCycles" env-default:"100"`
		CrudCycles int `yaml:"crudCycles" env-default:"20"`
	} `yaml:"pvcStress"`
	IOSoakTest struct {
		Replicas int    `yaml:"replicas" env-default:"2"`
		Duration string `yaml:"duration" env-default:"10m"`
		// Number of volumes for each mayastor instance
		// volumes for disruptor pods are allocated from within this "pool"
		LoadFactor int      `yaml:"loadFactor" env-default:"20"`
		Protocols  []string `yaml:"protocols" env-default:"nvmf"`
		// FioStartDelay units are seconds
		FioStartDelay int `yaml:"fioStartDelay" env-default:"60"`
		Disrupt       struct {
			// Number of disruptor pods.
			PodCount int `yaml:"podCount" env-default:"3"`
			// FaultAfter units are seconds
			FaultAfter int `yaml:"faultAfter" env-default:"45"`
		} `yaml:"disrupt"`
		FioDutyCycles []struct {
			// ThinkTime units are microseconds
			ThinkTime       int `yaml:"thinkTime"`
			ThinkTimeBlocks int `yaml:"thinkTimeBlocks"`
		} `yaml:"fioDutyCycles"`
	} `yaml:"ioSoakTest"`
	CSI struct {
		Replicas       int    `yaml:"replicas" env-default:"1"`
		SmallClaimSize string `yaml:"smallClaimSize" env-default:"50Mi"`
		LargeClaimSize string `yaml:"largeClaimSize" env-default:"500Mi"`
	} `yaml:"csi"`
	Uninstall struct {
		Cleanup int `yaml:"cleanup" env:"e2e_uninstall_cleanup"`
	} `yaml:"uninstall"`
	BasicVolumeIO struct {
		Replicas int `yaml:"replicas" env-default:"1"`
		// FioTimeout is in seconds
		FioTimeout int `yaml:"fioTimeout" env-default:"120"`
		// VolSizeMb Units are MiB
		VolSizeMb int `yaml:"volSizeMb" env-default:"1024"`
		// FsVolSizeMb Units are MiB
		FsVolSizeMb int `yaml:"fsVolSizeMb" env-default:"900"`
	} `yaml:"basicVolumeIO"`
	MultipleVolumesPodIO struct {
		VolumeCount          int `yaml:"volumeCount" env-default:"2"`
		MultipleReplicaCount int `yaml:"replicas" env-default:"2"`
	} `yaml:"multiVolumesPodIO"`
	// Run configuration
	ReportsDir string `yaml:"reportsDir" env:"e2e_reports_dir"`
}

var once sync.Once
var e2eConfig E2EConfig

// Relative path to the configuration directory WRT e2e root
const configDir = "/configurations"
const defaultConfigFileRelPath = configDir + "/ci_e2e_config.yaml"

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
	e2eRootDir, okE2eRootDir := os.LookupEnv("e2e_root_dir")
	// The configuration overrides the e2eRootDir setting,
	// this makes it possible to use a configuration file written out
	// previously to replicate a test run configuration.
	once.Do(func() {
		var configFile string
		// We absorb the complexity of locating the configuration file here
		// so that scripts invoking the tests can be simpler
		// - if OS envvar e2e_config is defined
		//		- if it is a path to a file then that file is used as the config file
		//		- else try to use a file of the same name in the configuration directory
		// - Otherwise the config file is defaulted to ci_e2e_config
		// A configuration file *MUST* be specified.
		value, ok := os.LookupEnv("e2e_config_file")
		if ok {
			if configFileExists(value) {
				configFile = value
			} else {
				if !okE2eRootDir {
					panic("E2E root directory not defined - define via e2e_root_dir environment variable")
				}
				configFile = path.Clean(e2eRootDir + configDir + "/" + value)
			}
		} else {
			if !okE2eRootDir {
				panic("E2E root directory not defined - define via e2e_root_dir environment variable")
			}
			configFile = path.Clean(e2eRootDir + defaultConfigFileRelPath)
		}
		fmt.Printf("Using configuration file %s\n", configFile)
		err = cleanenv.ReadConfig(configFile, &e2eConfig)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}

		// There are complications because there are 2 possible sources for truth for the e2e root directory
		// 1. the environment variable
		// 2. the configuration file
		// If only one is defined, we use the defined value,
		// We need to resolve in a well defined manner when
		// a. neither are defined (panic)
		// b. both are defined, (environment variable overrides configuration setting)
		if !okE2eRootDir {
			if e2eConfig.E2eRootDir == "" {
				panic("E2E root directory is not specified.")
			}
		} else {
			if e2eRootDir != e2eConfig.E2eRootDir {
				fmt.Printf("overriding configuration e2e root dir from %s to %s", e2eConfig.E2eRootDir, e2eRootDir)
			}
			e2eConfig.E2eRootDir = e2eRootDir
		}

		// MayastorRootDir is either set from the environment var mayastor_root_dir
		// or is pre-configured in the configuration file.
		// It *cannot* be empty
		if e2eConfig.MayastorRootDir == "" {
			panic("Configuration error unspecified mayastor directory")
		}

		cfgBytes, _ := yaml.Marshal(e2eConfig)
		cfgUsedFile := path.Clean(e2eConfig.MayastorRootDir + "/artifacts/e2e_config-" + e2eConfig.ConfigName + "-used.yaml")
		_ = ioutil.WriteFile(cfgUsedFile, cfgBytes, 0644)
	})

	return e2eConfig
}
