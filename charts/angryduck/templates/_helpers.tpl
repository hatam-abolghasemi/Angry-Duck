{{- define "angryduck.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "angryduck.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "angryduck.selectorLabels" -}}
app.kubernetes.io/name: {{ include "angryduck.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "angryduck.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "angryduck.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "angryduck.controllerName" -}}
{{- printf "%s-controller" (include "angryduck.fullname" .) -}}
{{- end -}}

{{- define "angryduck.workerName" -}}
{{- printf "%s-worker" (include "angryduck.fullname" .) -}}
{{- end -}}

{{- define "angryduck.sharedTokenSecret" -}}
{{- default (printf "%s-token" (include "angryduck.fullname" .)) .Values.sharedToken.existingSecret -}}
{{- end -}}

{{- define "angryduck.webhookTokenSecret" -}}
{{- default (printf "%s-webhook-token" (include "angryduck.fullname" .)) .Values.webhookToken.existingSecret -}}
{{- end -}}

{{- define "angryduck.registryCredentialsSecret" -}}
{{- default (printf "%s-registry-credentials" (include "angryduck.fullname" .)) .Values.registryCredentials.existingSecret -}}
{{- end -}}

{{- define "angryduck.hasRegistryCredentials" -}}
{{- if or .Values.registryCredentials.existingSecret .Values.registryCredentials.dockerconfigjson -}}true{{- end -}}
{{- end -}}

{{- define "angryduck.image" -}}
{{- printf "%s:%s" .image.repository (default .chart.AppVersion .image.tag) -}}
{{- end -}}

{{- define "angryduck.token" -}}
{{- $secret := lookup "v1" "Secret" .namespace .name -}}
{{- if and $secret $secret.data (index $secret.data "token") -}}
{{- index $secret.data "token" -}}
{{- else -}}
{{- randAlphaNum 64 | b64enc -}}
{{- end -}}
{{- end -}}
