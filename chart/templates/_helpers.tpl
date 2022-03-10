{{/* Enforce trailing slash to mayastorImagesPrefix or leave empty */}}
{{- define "mayastorImagesPrefix" -}}
{{- if .Values.mayastorImagesRegistry }}
{{- printf "%s/" (.Values.mayastorImagesRegistry | trimSuffix "/") }}
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

{{/* Generate the etcd endpoint that should be used by mayastor */}}
{{- define "etcdEndpoint" -}}
    {{- if or .Values.etcd.enabled (not .Values.etcdEndpoint) }}
        {{- printf "mayastor-etcd" }}
    {{- else }}
        {{- .Values.etcdEndpoint }}
    {{- end }}
{{- end }}
