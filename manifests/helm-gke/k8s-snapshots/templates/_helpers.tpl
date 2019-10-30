{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "k8s-snapshots.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "k8s-snapshots.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Default list of labels to apply to each spec.
*/}}
{{- define "k8s-snapshots.release_labels" -}}
generator: helm
date: {{ now | htmlDate }}
heritage: {{ .Release.Service }}
release: {{ .Release.Name | quote }}
chart: "{{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}"
version: {{ .Chart.AppVersion | quote }}
app: {{ .Chart.Name | quote }}
tag: {{ .Values.image.tag | quote }}
{{ toYaml .Values.labels }}
{{- end -}}

{{/*
Create an image registry URI.
*/}}
{{- define "k8s-snapshots.image_name" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Chart.Name .Values.image.tag | quote -}}
{{- end -}}
