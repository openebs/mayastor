package reporter

import (
	"e2e-basic/common/e2e_config"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
)

func GetReporters(name string) []Reporter {
	cfg := e2e_config.GetConfig()

	if cfg.ReportsDir == "" {
		panic("reportDir not defined - define via e2e_reports_dir environment variable")
	}
	testGroupPrefix := "e2e."
	xmlFileSpec := cfg.ReportsDir + "/" + testGroupPrefix + name + "-junit.xml"
	junitReporter := reporters.NewJUnitReporter(xmlFileSpec)
	return []Reporter{junitReporter}
}
