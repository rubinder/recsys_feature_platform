{{- define "recsys.redis" -}}
{{- if .Values.redis.enabled -}}{{ .Release.Name }}-redis:6379{{- else -}}{{ required "redis.external is required when redis.enabled=false" .Values.redis.external }}{{- end -}}
{{- end -}}
{{- define "recsys.env" -}}
- {name: REDIS_CONNECTION, value: {{ include "recsys.redis" . | quote }}}
- {name: RECSYS_ROOT, value: /app}
- {name: DEFAULT_VARIANT, value: {{ .Values.api.defaultVariant | quote }}}
- {name: FEATURE_CACHE_TTL_S, value: {{ .Values.api.featureCacheTtlS | quote }}}
{{- if .Values.hostRoot }}
- {name: MLFLOW_TRACKING_URI, value: "sqlite:///{{ .Values.hostRoot }}/mlruns/mlflow.db"}
{{- end }}
{{- end -}}
{{- define "recsys.volumes" -}}
{{- if .Values.hostRoot }}
volumes:
  - {name: data, hostPath: {path: "{{ .Values.hostRoot }}/data", type: Directory}}
  - {name: mlruns, hostPath: {path: "{{ .Values.hostRoot }}/mlruns", type: Directory}}
{{- end }}
{{- end -}}
{{- define "recsys.mounts" -}}
{{- if .Values.hostRoot }}
volumeMounts:
  - {name: data, mountPath: /app/data}
  - {name: mlruns, mountPath: "{{ .Values.hostRoot }}/mlruns"}
{{- end }}
{{- end -}}
