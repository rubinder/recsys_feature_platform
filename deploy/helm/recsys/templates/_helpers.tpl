{{- define "recsys.redisEnv" -}}
{{- if .Values.redis.enabled }}
- {name: REDIS_CONNECTION, value: "{{ .Release.Name }}-redis:6379"}
{{- else if .Values.redis.externalSecret }}
- name: REDIS_CONNECTION
  valueFrom: {secretKeyRef: {name: {{ .Values.redis.externalSecret | quote }}, key: connection}}
{{- else }}
- {name: REDIS_CONNECTION, value: {{ required "redis.external or redis.externalSecret is required when redis.enabled=false" .Values.redis.external | quote }}}
{{- end }}
{{- end -}}
{{- define "recsys.env" -}}
{{ include "recsys.redisEnv" . }}
- {name: RECSYS_ROOT, value: /app}
- {name: DEFAULT_VARIANT, value: {{ .Values.api.defaultVariant | quote }}}
- {name: FEATURE_CACHE_TTL_S, value: {{ .Values.api.featureCacheTtlS | quote }}}
{{- if .Values.api.modelVariants }}
- {name: MODEL_VARIANTS, value: {{ .Values.api.modelVariants | quote }}}
{{- end }}
{{- if .Values.hostRoot }}
- {name: MLFLOW_TRACKING_URI, value: "sqlite:///{{ .Values.hostRoot }}/mlruns/mlflow.db"}
{{- else }}
- {name: MLFLOW_TRACKING_URI, value: "sqlite:////tmp/mlflow.db"}
{{- end }}
{{- if .Values.cloud.enabled }}
- {name: RECSYS_PROFILE, value: cloud}
- {name: RECSYS_SF_PRIVATE_KEY_PATH, value: /secrets/snowflake/feast_rsa_key.p8}
{{- end }}
{{- end -}}
{{- define "recsys.envFrom" -}}
{{- if .Values.cloud.enabled }}
envFrom:
  - configMapRef: {name: {{ .Values.cloud.configMap | quote }}}
{{- end }}
{{- end -}}
