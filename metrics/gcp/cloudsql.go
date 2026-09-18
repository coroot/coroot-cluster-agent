package gcp

import (
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/prometheus/client_golang/prometheus"
	sqladmin "google.golang.org/api/sqladmin/v1"
	"k8s.io/klog"
)

func (d *Discoverer) discoverCloudSQL() {
	var instances []*sqladmin.DatabaseInstance
	ctx, cancel := d.apiContext()
	defer cancel()
	err := d.sqlClient.Instances.List(d.project).Pages(ctx, func(page *sqladmin.InstancesListResponse) error {
		for _, instance := range page.Items {
			if d.region != "" && instance.Region != d.region {
				continue
			}
			instances = append(instances, instance)
		}
		return nil
	})
	if err != nil {
		d.registerError(err)
		return
	}
	byName := map[string]*sqladmin.DatabaseInstance{}
	for _, instance := range instances {
		byName[instance.Name] = instance
	}
	seen := map[string]bool{}
	for _, instance := range instances {
		filters, labels := d.cfg.CloudSQLLabelFilters, userLabels(instance)
		if !common.LabelsMatched(filters, labels) && !common.LabelsMatched(filters, userLabels(byName[replicaPrimary(instance)])) {
			klog.Infof("Cloud SQL instance %s (labels: %s) was skipped according to the label-based filters: %s", instance.Name, labels, filters)
			continue
		}
		id := d.project + "/" + instance.Name
		seen[id] = true
		if d.sqlCollectors[id] == nil {
			klog.Infoln("new Cloud SQL instance found:", id)
			c := NewCloudSQLCollector(d, instance)
			if err := prometheus.WrapRegistererWith(cloudSQLLabels(id), d.reg).Register(c); err != nil {
				klog.Error(err)
				continue
			}
			d.sqlCollectors[id] = c
		}
		d.sqlCollectors[id].update(instance)
	}
	for id, c := range d.sqlCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(cloudSQLLabels(id), d.reg).Unregister(c)
			delete(d.sqlCollectors, id)
			c.Stop()
		}
	}
}

func replicaPrimary(i *sqladmin.DatabaseInstance) string {
	_, primary, _ := strings.Cut(i.MasterInstanceName, ":")
	return primary
}

func userLabels(i *sqladmin.DatabaseInstance) map[string]string {
	if i == nil || i.Settings == nil {
		return nil
	}
	return i.Settings.UserLabels
}

type CloudSQLCollector struct {
	discoverer *Discoverer
	instance   *sqladmin.DatabaseInstance
	logs       *LogReader
}

func NewCloudSQLCollector(discoverer *Discoverer, instance *sqladmin.DatabaseInstance) *CloudSQLCollector {
	c := &CloudSQLCollector{discoverer: discoverer, instance: instance}
	switch engine, _ := cloudSQLEngine(instance.DatabaseVersion); engine {
	case "postgres", "mysql":
		c.logs = NewLogReader(discoverer, instance.Name, *flags.CollectGCPLogs)
	}
	return c
}

func (c *CloudSQLCollector) update(instance *sqladmin.DatabaseInstance) {
	c.instance = instance
}

func (c *CloudSQLCollector) Stop() {
	if c.logs != nil {
		c.logs.Stop()
	}
}

func (c *CloudSQLCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("gcp_cloudsql_collector", "", nil, nil)
}

func (c *CloudSQLCollector) address() (string, string) {
	var private, public string
	for _, ip := range c.instance.IpAddresses {
		switch ip.Type {
		case "PRIVATE":
			private = ip.IpAddress
		case "PRIMARY":
			public = ip.IpAddress
		}
	}
	ip := private
	if ip == "" {
		ip = public
	}
	engine, _ := cloudSQLEngine(c.instance.DatabaseVersion)
	port := ""
	switch engine {
	case "postgres":
		port = "5432"
	case "mysql":
		port = "3306"
	case "sqlserver":
		port = "1433"
	}
	return ip, port
}

func (c *CloudSQLCollector) Collect(ch chan<- prometheus.Metric) {
	i := c.instance
	if i == nil {
		return
	}
	ch <- common.Gauge(dCloudSQLStatus, 1, i.State)
	engine, version := cloudSQLEngine(i.DatabaseVersion)
	ip, port := c.address()
	var tier, availability string
	if i.Settings != nil {
		tier = i.Settings.Tier
		availability = i.Settings.AvailabilityType
	}
	ch <- common.Gauge(dCloudSQLInfo, 1,
		c.discoverer.project, i.Region, i.GceZone, ip, port, engine, version, tier, availability, i.ConnectionName,
		i.InstanceType, replicaPrimary(i),
	)
	c.discoverer.monitoring.collect(i.Name, ch)
	if c.logs != nil {
		for _, lc := range c.logs.Counters() {
			ch <- common.Counter(dCloudSQLLogMessages, float64(lc.Messages), lc.Level.String(), lc.Hash, lc.Sample)
		}
	}
}

func cloudSQLEngine(databaseVersion string) (string, string) {
	engine, version, _ := strings.Cut(strings.ToLower(databaseVersion), "_")
	return engine, strings.ReplaceAll(version, "_", ".")
}
