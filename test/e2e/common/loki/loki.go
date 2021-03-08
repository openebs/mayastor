package loki

import (
	"bytes"
	"e2e-basic/common/e2e_config"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func SendLokiMarker(text string) error {
	logf.Log.Info("Sending Loki report", "text", text)

	apiUser := os.Getenv("grafana_api_user")
	apiPw := os.Getenv("grafana_api_pw")
	buildNumber := os.Getenv("e2e_build_number")
	imageTag := e2e_config.GetConfig().ImageTag
	timestamp := strconv.FormatInt(time.Now().UnixNano(), 10)

	if apiUser != "" && apiPw != "" && buildNumber != "" {
		logentryJSON := `
		{
			"streams": [
				{
					"stream": {
						"run": "` + buildNumber + `",
						"version": "` + imageTag + `",
						"app": "marker"
					},
					"values": [
						["` + timestamp + `","` + text + `"]
					]
				}
			]
		}`
		compactedBuffer := new(bytes.Buffer)
		err := json.Compact(compactedBuffer, []byte(logentryJSON))
		if err != nil {
			return fmt.Errorf("Failed to compact Loki request %v", err)
		}
		req, err := http.NewRequest("POST", "https://logs-prod-us-central1.grafana.net/loki/api/v1/push", compactedBuffer)
		if err != nil {
			return fmt.Errorf("Failed to create Loki marker request %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth(apiUser, apiPw)

		client := &http.Client{}
		_, err = client.Do(req)
		if err != nil {
			return fmt.Errorf("Failed to send Loki marker %v", err)
		}
	} else if apiUser != "" || apiPw != "" || buildNumber != "" { // all should be defined or none
		errorStr := "Invalid combination of environment variables"
		if apiUser == "" {
			errorStr += ", user is not defined"
		}
		if apiPw == "" {
			errorStr += ", password is not defined"
		}
		if buildNumber == "" {
			errorStr += ", build number is not defined"
		}
		return fmt.Errorf(errorStr)
	}
	return nil
}
