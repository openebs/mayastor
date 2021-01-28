{{/* Enforce trailing slash to mayastorImagesPrefix or leave empty */}}
{{- define "mayastorImagesPrefix" -}}
{{- if .Values.mayastorImagesRepo }}
{{- printf "%s/" (.Values.mayastorImagesRepo | trimSuffix "/") }}
{{- else }}
{{- "" }}
{{- end }}
{{- end }}

{{/* Generate CPU list specification based on CPU count (-l param of mayastor) */}}
{{- define "mayastorCpuSpec" -}}
{{- range $i, $e := until (int .Values.mayastorCpuCount) }}
{{- if gt $i 0 }}
{{- printf "," }}
{{- end }}
{{- printf "%d" (add $i 1) }}
{{- end }}
{{- end }}
