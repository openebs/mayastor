package reporter

import (
	"os"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
)

func GetReporters(name string) []Reporter {
	reportDir := os.Getenv("e2e_reports_dir")
	if reportDir == "" {
		panic("reportDir not defined - define via e2e_reports_dir environment variable")
	}
	testGroupPrefix := "e2e."
	xmlFileSpec := reportDir + "/" + testGroupPrefix + name + "-junit.xml"
	junitReporter := reporters.NewJUnitReporter(xmlFileSpec)
	return []Reporter{junitReporter}
}
