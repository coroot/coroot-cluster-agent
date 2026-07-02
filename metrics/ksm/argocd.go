package ksm

import (
	crs "k8s.io/kube-state-metrics/v2/pkg/customresourcestate"
	"k8s.io/kube-state-metrics/v2/pkg/metric"
)

var resourceLabels = map[string][]string{
	"resource_group":     {"group"},
	"resource_kind":      {"kind"},
	"resource_namespace": {"namespace"},
	"resource_name":      {"name"},
}

func merge(base map[string][]string, extra map[string][]string) map[string][]string {
	out := make(map[string][]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func argocd() []crs.Resource {
	commonLabels := crs.Labels{LabelsFromPath: map[string][]string{
		"uid":       {"metadata", "uid"},
		"name":      {"metadata", "name"},
		"namespace": {"metadata", "namespace"},
	}}
	metricNamePrefix := "argocd"
	return []crs.Resource{
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "argoproj.io", Version: "v1alpha1", Kind: "Application"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			// Many fields are absent on healthy/idle apps (no operationState until the
			// first sync; resources that don't report health). KSM logs every nil path
			// as an error; route those above default verbosity to avoid log spam.
			ErrorLogV: 4,
			Metrics: []crs.Generator{
				info("application_info", []string{}, map[string][]string{
					"project":         {"spec", "project"},
					"source_type":     {"status", "sourceType"},
					"repo":            {"spec", "source", "repoURL"},
					"path":            {"spec", "source", "path"},
					"chart":           {"spec", "source", "chart"},
					"target_revision": {"spec", "source", "targetRevision"},
					"dest_server":     {"spec", "destination", "server"},
					"dest_name":       {"spec", "destination", "name"},
					"dest_namespace":  {"spec", "destination", "namespace"},
					"revision":        {"status", "sync", "revision"},
				}),
				info("application_sync_status", []string{"status", "sync"}, map[string][]string{
					"sync_status": {"status"},
				}),
				info("application_health_status", []string{"status", "health"}, map[string][]string{
					"health_status": {"status"},
				}),
				info("application_operation_status", []string{"status", "operationState"}, map[string][]string{
					"operation_phase": {"phase"},
				}),
				{
					Name: "application_operation_finished_timestamp_seconds",
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{Path: []string{"status", "operationState"}},
							ValueFrom:  []string{"finishedAt"},
						},
					},
				},
				info("application_resource_status", []string{"status", "operationState", "syncResult", "resources"},
					merge(resourceLabels, map[string][]string{"status": {"status"}})),
				info("application_resource_info", []string{"status", "resources"}, resourceLabels),
				info("application_resource_sync_status", []string{"status", "resources"},
					merge(resourceLabels, map[string][]string{"sync_status": {"status"}})),
				info("application_resource_health_status", []string{"status", "resources"},
					merge(resourceLabels, map[string][]string{"health_status": {"health", "status"}})),
			},
		},
	}
}
