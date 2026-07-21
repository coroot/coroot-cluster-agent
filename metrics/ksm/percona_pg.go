package ksm

import (
	crs "k8s.io/kube-state-metrics/v2/pkg/customresourcestate"
)

func perconaPG() []crs.Resource {
	operator := map[string]string{"operator": "percona"}
	commonLabels := crs.Labels{
		CommonLabels: operator,
		LabelsFromPath: map[string][]string{
			"uid":       {"metadata", "uid"},
			"name":      {"metadata", "name"},
			"namespace": {"metadata", "namespace"},
		},
	}
	prefix := "pg"
	return []crs.Resource{
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "pgv2.percona.com", Version: "v2", Kind: "PerconaPGCluster"},
			MetricNamePrefix: &prefix,
			Labels:           commonLabels,
			ErrorLogV:        4,
			Metrics: []crs.Generator{
				infoConst("backup_target_info", []string{"spec", "backups", "pgbackrest", "repos"}, map[string][]string{
					"method":          {"name"},
					"s3_bucket":       {"s3", "bucket"},
					"s3_endpoint":     {"s3", "endpoint"},
					"gcs_bucket":      {"gcs", "bucket"},
					"azure_container": {"azure", "container"},
					"schedule":        {"schedules", "full"},
				}, map[string]string{"type": "object-storage"}),
				info("cluster_status", []string{"status", "conditions"}, map[string][]string{
					"type":   {"type"},
					"status": {"status"},
					"reason": {"reason"},
				}),
			},
		},
		{
			GroupVersionKind: crs.GroupVersionKind{Group: "pgv2.percona.com", Version: "v2", Kind: "PerconaPGBackup"},
			MetricNamePrefix: &prefix,
			Labels:           commonLabels,
			ErrorLogV:        4,
			Metrics: []crs.Generator{
				info("backup_info", []string{}, map[string][]string{
					"cluster": {"spec", "pgCluster"},
					"method":  {"status", "repo", "name"},
					"kind":    {"status", "backupType"},
					"path":    {"status", "destination"},
				}),
				info("backup_status", []string{}, map[string][]string{
					"status": {"status", "state"},
				}),
				gaugeTimestamp("backup_completed_timestamp_seconds", []string{"status"}, "completed"),
			},
		},
	}
}
