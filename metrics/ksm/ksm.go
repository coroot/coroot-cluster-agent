package ksm

import (
	"context"
	"fmt"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"gopkg.in/yaml.v3"
	"k8s.io/klog"
	"k8s.io/kube-state-metrics/v2/pkg/app"
	crs "k8s.io/kube-state-metrics/v2/pkg/customresourcestate"
	"k8s.io/kube-state-metrics/v2/pkg/options"
)

const RetryInterval = 10 * time.Second

type KSM struct {
	opts *options.Options
	ctx  context.Context
	stop context.CancelFunc
}

func NewKSM(listenAddr string, minAge time.Duration) (*KSM, error) {
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
		MinAge:        minAge,
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

	ctx, cancel := context.WithCancel(context.Background())
	return &KSM{opts: opts, ctx: ctx, stop: cancel}, nil
}

func (ksm *KSM) Start() {
	for {
		err := app.RunKubeStateMetrics(ksm.ctx, ksm.opts)
		if ksm.ctx.Err() != nil {
			return
		}
		if err == nil {
			err = fmt.Errorf("exited unexpectedly")
		}
		klog.Errorf("kube-state-metrics failed: %s, retrying in %s", err, RetryInterval)
		select {
		case <-ksm.ctx.Done():
			return
		case <-time.After(RetryInterval):
		}
	}
}

func (ksm *KSM) Stop() {
	ksm.stop()
}

func customResourceConfig() string {
	resources := append(fluxcd(), argocd()...)
	resources = append(resources, cnpg()...)
	resources = append(resources, perconaPG()...)
	cfg := crs.Metrics{
		Spec: crs.MetricsSpec{Resources: resources},
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		klog.Errorln("can't marshal custom resource config:", err)
		return ""
	}
	return string(data)
}
