package ksm

import (
	crs "k8s.io/kube-state-metrics/v2/pkg/customresourcestate"
)

func cnpg() []crs.Resource {
	operator := map[string]string{"operator": "cnpg"}
	commonLabels := crs.Labels{
		CommonLabels: operator,
		LabelsFromPath: map[string][]string{
			"uid":       {"metadata", "uid"},
			"name":      {"metadata", "name"},
			"namespace": {"metadata", "namespace"},
		},
	}
	prefix := "pg"
	gvk := func(kind string) crs.GroupVersionKind {
		return crs.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: kind}
	}
	return []crs.Resource{
		{
			GroupVersionKind: gvk("Cluster"),
			MetricNamePrefix: &prefix,
			Labels:           commonLabels,
			ErrorLogV:        4,
			Metrics: []crs.Generator{
				infoConst("backup_target_info", []string{"spec", "backup"}, map[string][]string{
					"path":             {"barmanObjectStore", "destinationPath"},
					"endpoint":         {"barmanObjectStore", "endpointURL"},
					"retention_policy": {"retentionPolicy"},
				}, map[string]string{"method": "barmanObjectStore", "type": "object-storage"}),
				info("cluster_status", []string{"status", "conditions"}, map[string][]string{
					"type":   {"type"},
					"status": {"status"},
					"reason": {"reason"},
				}),
				gaugeTimestampByMethod("backup_last_successful_timestamp_seconds", []string{"status", "lastSuccessfulBackupByMethod"}),
				gaugeTimestampByMethod("backup_first_recoverability_point_timestamp_seconds", []string{"status", "firstRecoverabilityPointByMethod"}),
				gaugeTimestamp("backup_last_failed_timestamp_seconds", []string{"status"}, "lastFailedBackup"),
			},
		},
		{
			GroupVersionKind: gvk("Backup"),
			MetricNamePrefix: &prefix,
			Labels:           commonLabels,
			ErrorLogV:        4,
			Metrics: []crs.Generator{
				info("backup_info", []string{}, map[string][]string{
					"cluster": {"spec", "cluster", "name"},
					"method":  {"status", "method"},
				}),
				info("backup_status", []string{}, map[string][]string{
					"status": {"status", "phase"},
				}),
				gaugeTimestamp("backup_completed_timestamp_seconds", []string{"status"}, "stoppedAt"),
			},
		},
		{
			GroupVersionKind: gvk("ScheduledBackup"),
			MetricNamePrefix: &prefix,
			Labels: crs.Labels{
				CommonLabels: operator,
				LabelsFromPath: map[string][]string{
					"uid":       {"metadata", "uid"},
					"name":      {"metadata", "name"},
					"namespace": {"metadata", "namespace"},
					"cluster":   {"spec", "cluster", "name"},
				},
			},
			ErrorLogV: 4,
			Metrics: []crs.Generator{
				info("backup_schedule_info", []string{"spec"}, map[string][]string{
					"schedule": {"schedule"},
				}),
				gaugeTimestamp("backup_next_scheduled_timestamp_seconds", []string{"status"}, "nextScheduleTime"),
			},
		},
	}
}
