package ksm

import (
	"context"

	"github.com/coroot/coroot-cluster-agent/common"
	"gopkg.in/yaml.v3"
	"k8s.io/klog"
	"k8s.io/kube-state-metrics/v2/pkg/app"
	crs "k8s.io/kube-state-metrics/v2/pkg/customresourcestate"
	"k8s.io/kube-state-metrics/v2/pkg/options"
)

type KSM struct {
	opts *options.Options
	stop context.CancelFunc
}

func NewKSM(listenAddr string) (*KSM, error) {
	host, port, err := common.SplitHostPort(listenAddr)
	if err != nil {
		return nil, err
	}

	opts := &options.Options{
		Host:          host,
		TelemetryHost: host,
		Port:          port,
		TelemetryPort: port + 1,
		TotalShards:   1,
		Namespaces:    options.DefaultNamespaces,
		Resources: options.ResourceSet{
			"namespaces":             struct{}{},
			"nodes":                  struct{}{},
			"daemonsets":             struct{}{},
			"deployments":            struct{}{},
			"replicasets":            struct{}{},
			"statefulsets":           struct{}{},
			"cronjobs":               struct{}{},
			"jobs":                   struct{}{},
			"persistentvolumeclaims": struct{}{},
			"persistentvolumes":      struct{}{},
			"pods":                   struct{}{},
			"services":               struct{}{},
			"endpoints":              struct{}{},
			"storageclasses":         struct{}{},
			"volumeattachments":      struct{}{},
		},
		MetricAllowlist: options.MetricSet{},
		MetricDenylist:  options.MetricSet{},
		MetricOptInList: options.MetricSet{},
		LabelsAllowList: options.LabelsAllowList{
			"pods": {"*"},
		},
		AnnotationsAllowList: options.LabelsAllowList{
			"*": {
				"coroot.com/application-category",
				"coroot.com/custom-application-name",
				"coroot.com/slo-availability-objective",
				"coroot.com/slo-latency-objective",
				"coroot.com/slo-latency-threshold",
			},
		},
		CustomResourceConfig: customResourceConfig(),
	}

	return &KSM{opts: opts}, nil
}

func (ksm *KSM) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	ksm.stop = cancel
	err := app.RunKubeStateMetrics(ctx, ksm.opts)
	if err != nil {
		klog.Errorln(err)
		cancel()
	}
}

func (ksm *KSM) Stop() {
	ksm.stop()
}

func customResourceConfig() string {
	cfg := crs.Metrics{
		Spec: crs.MetricsSpec{Resources: fluxcd()},
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		klog.Errorln("can't marshal custom resource config:", err)
		return ""
	}
	return string(data)
}
