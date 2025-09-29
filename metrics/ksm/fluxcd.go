package ksm

import (
	crs "k8s.io/kube-state-metrics/v2/pkg/customresourcestate"
	"k8s.io/kube-state-metrics/v2/pkg/metric"
)

func fluxcd() []crs.Resource {
	commonLabels := crs.Labels{LabelsFromPath: map[string][]string{
		"uid":       {"metadata", "uid"},
		"name":      {"metadata", "name"},
		"namespace": {"metadata", "namespace"},
	}}
	metricNamePrefix := "fluxcd"
	res := []crs.Resource{
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "source.toolkit.fluxcd.io", Version: "v1", Kind: "GitRepository"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			Metrics: []crs.Generator{
				{
					Name: "git_repository_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"spec"},
								LabelsFromPath: map[string][]string{
									"suspended": {"suspend"},
									"url":       {"url"},
									"interval":  {"interval"},
								},
							},
						},
					},
				},
				{
					Name: "git_repository_status",
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "conditions"},
								LabelsFromPath: map[string][]string{
									"type":   {"type"},
									"reason": {"reason"},
								},
							},
							ValueFrom: []string{"status"},
						},
					},
				},
			},
		},
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "source.toolkit.fluxcd.io", Version: "v1", Kind: "OCIRepository"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			Metrics: []crs.Generator{
				{
					Name: "oci_repository_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"spec"},
								LabelsFromPath: map[string][]string{
									"suspended": {"suspend"},
									"url":       {"url"},
									"interval":  {"interval"},
								},
							},
						},
					},
				},
				{
					Name: "oci_repository_status",
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "conditions"},
								LabelsFromPath: map[string][]string{
									"type":   {"type"},
									"reason": {"reason"},
								},
							},
							ValueFrom: []string{"status"},
						},
					},
				},
			},
		},
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "source.toolkit.fluxcd.io", Version: "v1", Kind: "HelmRepository"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			Metrics: []crs.Generator{
				{
					Name: "helm_repository_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"spec"},
								LabelsFromPath: map[string][]string{
									"suspended": {"suspend"},
									"url":       {"url"},
									"interval":  {"interval"},
								},
							},
						},
					},
				},
				{
					Name: "helm_repository_status",
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "conditions"},
								LabelsFromPath: map[string][]string{
									"type":   {"type"},
									"reason": {"reason"},
								},
							},
							ValueFrom: []string{"status"},
						},
					},
				},
			},
		},
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "helm.toolkit.fluxcd.io", Version: "v2", Kind: "HelmRelease"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			Metrics: []crs.Generator{
				{
					Name: "helm_release_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"spec"},
								LabelsFromPath: map[string][]string{
									"suspended":           {"suspend"},
									"interval":            {"interval"},
									"target_namespace":    {"targetNamespace"},
									"source_kind":         {"chart", "spec", "sourceRef", "kind"},
									"source_name":         {"chart", "spec", "sourceRef", "name"},
									"source_namespace":    {"chart", "spec", "sourceRef", "namespace"},
									"chart":               {"chart", "spec", "chart"},
									"version":             {"chart", "spec", "version"},
									"chart_ref_kind":      {"chartRef", "kind"},
									"chart_ref_name":      {"chartRef", "name"},
									"chart_ref_namespace": {"chartRef", "namespace"},
								},
							},
						},
					},
				},
				{
					Name:   "helm_release_status",
					Labels: crs.Labels{},
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "conditions"},
								LabelsFromPath: map[string][]string{
									"type":   {"type"},
									"reason": {"reason"},
								},
							},
							ValueFrom: []string{"status"},
						},
					},
				},
			},
		},
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "source.toolkit.fluxcd.io", Version: "v1", Kind: "HelmChart"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			Metrics: []crs.Generator{
				{
					Name: "helm_chart_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"spec"},
								LabelsFromPath: map[string][]string{
									"chart":            {"chart"},
									"version":          {"version"},
									"source_kind":      {"sourceRef", "kind"},
									"source_name":      {"sourceRef", "name"},
									"source_namespace": {"sourceRef", "namespace"},
									"interval":         {"interval"},
									"suspended":        {"suspend"},
								},
							},
						},
					},
				},
				{
					Name: "helm_chart_status",
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "conditions"},
								LabelsFromPath: map[string][]string{
									"type":   {"type"},
									"reason": {"reason"},
								},
							},
							ValueFrom: []string{"status"},
						},
					},
				},
			},
		},
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			Metrics: []crs.Generator{
				{
					Name: "kustomization_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{},
								LabelsFromPath: map[string][]string{
									"suspended":               {"spec", "suspend"},
									"interval":                {"spec", "interval"},
									"path":                    {"spec", "path"},
									"source_kind":             {"spec", "sourceRef", "kind"},
									"source_name":             {"spec", "sourceRef", "name"},
									"source_namespace":        {"spec", "sourceRef", "namespace"},
									"target_namespace":        {"spec", "targetNamespace"},
									"last_applied_revision":   {"status", "lastAppliedRevision"},
									"last_attempted_revision": {"status", "lastAttemptedRevision"},
								},
							},
						},
					},
				},
				{
					Name: "kustomization_status",
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "conditions"},
								LabelsFromPath: map[string][]string{
									"type":   {"type"},
									"reason": {"reason"},
								},
							},
							ValueFrom: []string{"status"},
						},
					},
				},
				{
					Name: "kustomization_inventory_entry_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "inventory", "entries"},
								LabelsFromPath: map[string][]string{
									"entry_id": {"id"},
								},
							},
						},
					},
				},
				{
					Name: "kustomization_dependency_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"spec", "dependsOn"},
								LabelsFromPath: map[string][]string{
									"depends_on_name":      {"name"},
									"depends_on_namespace": {"namespace"},
								},
							},
						},
					},
				},
			},
		},
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "fluxcd.controlplane.io", Version: "v1", Kind: "ResourceSet"},
			MetricNamePrefix: &metricNamePrefix,
			Labels:           commonLabels,
			Metrics: []crs.Generator{
				{
					Name: "resourceset_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{},
								LabelsFromPath: map[string][]string{
									"last_applied_revision": {"status", "lastAppliedRevision"},
								},
							},
						},
					},
				},
				{
					Name: "resourceset_status",
					Each: crs.Metric{
						Type: metric.Gauge,
						Gauge: &crs.MetricGauge{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "conditions"},
								LabelsFromPath: map[string][]string{
									"type":   {"type"},
									"reason": {"reason"},
								},
							},
							ValueFrom: []string{"status"},
						},
					},
				},
				{
					Name: "resourceset_inventory_entry_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"status", "inventory", "entries"},
								LabelsFromPath: map[string][]string{
									"entry_id": {"id"},
								},
							},
						},
					},
				},
				{
					Name: "resourceset_dependency_info",
					Each: crs.Metric{
						Type: metric.Info,
						Info: &crs.MetricInfo{
							MetricMeta: crs.MetricMeta{
								Path: []string{"spec", "dependsOn"},
								LabelsFromPath: map[string][]string{
									"depends_on_kind":      {"kind"},
									"depends_on_name":      {"name"},
									"depends_on_namespace": {"namespace"},
								},
							},
						},
					},
				},
			},
		},
	}
	return res
}
