{{/* Enforce trailing slash to mayastorImagesPrefix or leave empty */}}
{{- define "mayastorImagesPrefix" -}}
{{- if .Values.mayastorImagesRepo }}
{{- printf "%s/" (.Values.mayastorImagesRepo | trimSuffix "/") }}
{{- else }}
{{- "" }}
{{- end }}
{{- end }}
