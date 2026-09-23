{{- define "sandbox-operator.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "sandbox-operator.apiserver.labels" -}}
{{ include "sandbox-operator.labels" . }}
app.kubernetes.io/name: sandbox-apiserver
app.kubernetes.io/component: aggregated-apiserver
{{- end }}

{{- define "sandbox-operator.apiserver.selectorLabels" -}}
app.kubernetes.io/name: sandbox-apiserver
{{- end }}

{{- define "sandbox-operator.envdProxy.labels" -}}
{{ include "sandbox-operator.labels" . }}
app.kubernetes.io/name: sandbox-envd-proxy
app.kubernetes.io/component: data-plane-proxy
{{- end }}

{{- define "sandbox-operator.envdProxy.selectorLabels" -}}
app.kubernetes.io/name: sandbox-envd-proxy
{{- end }}

{{- define "sandbox-operator.image" -}}
{{- printf "%s:%s" .repository (required "an image tag is required" .tag) -}}
{{- end }}

{{/* Both binaries spell a sandbox host as {port}-{sandboxID}.{domain}, so the proxy follows the e2b domain unless overridden. */}}
{{- define "sandbox-operator.envdProxy.domain" -}}
{{- default .Values.apiserver.e2b.domain .Values.envdProxy.domain -}}
{{- end }}

{{/* The proxy is not an access boundary and a key's sandboxes live in the key's namespace, so it resolves across every namespace unless narrowed. */}}
{{- define "sandbox-operator.envdProxy.namespace" -}}
{{- .Values.envdProxy.namespace -}}
{{- end }}
