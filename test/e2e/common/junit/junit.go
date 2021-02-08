package junit

import (
	"os"
)

func ConstructJunitFileName(name string) string {
	reportDir := os.Getenv("e2e_reports_dir")
	testGroupPrefix := "e2e."
	return reportDir + "/" + testGroupPrefix + name
}
