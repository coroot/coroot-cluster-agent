package ksm

import (
	crs "k8s.io/kube-state-metrics/v2/pkg/customresourcestate"
	"k8s.io/kube-state-metrics/v2/pkg/metric"
)

func info(name string, path []string, labels map[string][]string) crs.Generator {
	return crs.Generator{
		Name: name,
		Each: crs.Metric{
			Type: metric.Info,
			Info: &crs.MetricInfo{
				MetricMeta: crs.MetricMeta{Path: path, LabelsFromPath: labels},
			},
		},
	}
}

func infoConst(name string, path []string, labels map[string][]string, constLabels map[string]string) crs.Generator {
	g := info(name, path, labels)
	g.Labels = crs.Labels{CommonLabels: constLabels}
	return g
}

func gaugeTimestamp(name string, path []string, valueFrom string) crs.Generator {
	return crs.Generator{
		Name: name,
		Each: crs.Metric{
			Type: metric.Gauge,
			Gauge: &crs.MetricGauge{
				MetricMeta: crs.MetricMeta{Path: path},
				ValueFrom:  []string{valueFrom},
			},
		},
	}
}

func gaugeTimestampByMethod(name string, path []string) crs.Generator {
	return crs.Generator{
		Name: name,
		Each: crs.Metric{
			Type: metric.Gauge,
			Gauge: &crs.MetricGauge{
				MetricMeta:   crs.MetricMeta{Path: path},
				LabelFromKey: "method",
			},
		},
	}
}
