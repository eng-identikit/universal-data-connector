{{- define "udc.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "udc.labels" -}}
app.kubernetes.io/name: udc
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "udc.selector" -}}
app.kubernetes.io/name: udc
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "udc.secretName" -}}
{{- if .Values.auth.existingSecret -}}{{ .Values.auth.existingSecret }}{{- else -}}{{ include "udc.fullname" . }}-secrets{{- end -}}
{{- end -}}

{{- define "udc.dbHost" -}}
{{- if .Values.database.bundled -}}{{ include "udc.fullname" . }}-db{{- else -}}{{ required "database.host is required when database.bundled=false" .Values.database.host }}{{- end -}}
{{- end -}}

{{- define "udc.natsUrl" -}}
{{- if .Values.nats.bundled -}}nats://{{ include "udc.fullname" . }}-nats:4222{{- else -}}{{ required "nats.url is required when nats.bundled=false" .Values.nats.url }}{{- end -}}
{{- end -}}

{{- define "udc.mqttUrl" -}}
{{- if .Values.mqtt.bundled -}}mqtt://{{ include "udc.fullname" . }}-mqtt:1883{{- else -}}{{ .Values.mqtt.url }}{{- end -}}
{{- end -}}

{{- define "udc.redisUrl" -}}
{{- if .Values.redis.bundled -}}redis://{{ include "udc.fullname" . }}-redis:6379{{- else -}}{{ required "redis.url is required when redis.bundled=false" .Values.redis.url }}{{- end -}}
{{- end -}}

{{/* Value of an existing secret key, so generated secrets survive upgrades */}}
{{- define "udc.secretValue" -}}
{{- $existing := lookup "v1" "Secret" .ctx.Release.Namespace (printf "%s-secrets" (include "udc.fullname" .ctx)) -}}
{{- if .value -}}{{ .value }}{{- else if and $existing (index $existing.data .key) -}}{{ index $existing.data .key | b64dec }}{{- else -}}{{ randAlphaNum 32 }}{{- end -}}
{{- end -}}
