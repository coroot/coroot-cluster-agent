package gcp

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/googleapi"
	monitoring "google.golang.org/api/monitoring/v3"
)

var (
	dCloudSQLCpuUsage      = common.Desc("gcp_cloudsql_cpu_usage_percent", "CPU utilization, percent")
	dCloudSQLCpuCores      = common.Desc("gcp_cloudsql_cpu_cores", "Number of vCPUs reserved for the instance")
	dCloudSQLCpuUsageCores = common.Desc("gcp_cloudsql_cpu_usage_cores", "CPU usage of the database process, cores")
	dCloudSQLMemoryTotal   = common.Desc("gcp_cloudsql_memory_total_bytes", "Memory quota")
	dCloudSQLMemoryUsed    = common.Desc("gcp_cloudsql_memory_used_bytes", "Memory usage of the database process, including its buffers and cache")
	dCloudSQLMemory        = common.Desc("gcp_cloudsql_memory_components_percent", "Memory quota split into usage, cache and free, percent", "component")
	dCloudSQLDiskTotal     = common.Desc("gcp_cloudsql_disk_total_bytes", "Disk quota")
	dCloudSQLDiskUsed      = common.Desc("gcp_cloudsql_disk_used_bytes", "Disk usage")
	dCloudSQLNetworkBytes  = common.Desc("gcp_cloudsql_network_bytes_per_second", "Network throughput", "direction")
	dCloudSQLIOOps         = common.Desc("gcp_cloudsql_io_ops_per_second", "Disk I/O operations per second", "operation")
	dCloudSQLIOBytes       = common.Desc("gcp_cloudsql_io_bytes_per_second", "Disk I/O throughput", "operation")

	dMemorystoreCpuUsage      = common.Desc("gcp_memorystore_cpu_usage_percent", "CPU utilization, percent")
	dMemorystoreCpuUsageCores = common.Desc("gcp_memorystore_cpu_usage_cores", "CPU usage of the engine process, cores")
	dMemorystoreMemoryUsed    = common.Desc("gcp_memorystore_memory_used_bytes", "Memory used by the engine")
	dMemorystoreNetworkBytes  = common.Desc("gcp_memorystore_network_bytes_per_second", "Network throughput", "direction")

	cloudSQL = monitoringProduct{prefix: "cloudsql.googleapis.com/database/", resource: "cloudsql_database", id: func(l map[string]string) string {
		_, instance, _ := strings.Cut(l["database_id"], ":") // "<project>:<instance>"
		return instance
	}}
	memorystoreRedis = monitoringProduct{prefix: "redis.googleapis.com/", resource: "redis_instance", id: func(l map[string]string) string {
		region, name, _ := parseResourceName(l["instance_id"])
		return l["project_id"] + "/" + region + "/" + name
	}}
	memorystoreValkey = monitoringProduct{prefix: "memorystore.googleapis.com/instance/", resource: "memorystore.googleapis.com/Instance", id: func(l map[string]string) string {
		return l["project_id"] + "/" + l["location"] + "/" + l["instance_id"]
	}}
	memorystoreMemcached = monitoringProduct{prefix: "memcache.googleapis.com/node/", resource: "memcache_node", id: func(l map[string]string) string {
		region := l["location"] // a zone: <region>-<letter>
		if i := strings.LastIndex(region, "-"); i > 0 {
			region = region[:i]
		}
		return l["project_id"] + "/" + region + "/" + l["instance_id"] + "/" + l["node_id"]
	}}

	primaryOnly = `metric.labels.role="primary"` // the replicas of Redis and Valkey report their own series

	monitoringMetrics = []monitoringMetric{
		{product: cloudSQL, name: "cpu/utilization", desc: dCloudSQLCpuUsage, scale: 100},
		{product: cloudSQL, name: "cpu/reserved_cores", desc: dCloudSQLCpuCores},
		{product: cloudSQL, name: "cpu/usage_time", desc: dCloudSQLCpuUsageCores, rate: true},
		{product: cloudSQL, name: "memory/components", desc: dCloudSQLMemory, labelKey: "component"},
		{product: cloudSQL, name: "memory/quota", desc: dCloudSQLMemoryTotal},
		{product: cloudSQL, name: "memory/total_usage", desc: dCloudSQLMemoryUsed},
		{product: cloudSQL, name: "disk/quota", desc: dCloudSQLDiskTotal},
		{product: cloudSQL, name: "disk/bytes_used", desc: dCloudSQLDiskUsed},
		{product: cloudSQL, name: "network/received_bytes_count", desc: dCloudSQLNetworkBytes, rate: true, label: "rx"},
		{product: cloudSQL, name: "network/sent_bytes_count", desc: dCloudSQLNetworkBytes, rate: true, label: "tx"},
		{product: cloudSQL, name: "disk/read_ops_count", desc: dCloudSQLIOOps, rate: true, label: "read"},
		{product: cloudSQL, name: "disk/write_ops_count", desc: dCloudSQLIOOps, rate: true, label: "write"},
		{product: cloudSQL, name: "disk/read_bytes_count", desc: dCloudSQLIOBytes, rate: true, label: "read"},
		{product: cloudSQL, name: "disk/write_bytes_count", desc: dCloudSQLIOBytes, rate: true, label: "write"},

		{product: memorystoreRedis, name: "stats/cpu_utilization", desc: dMemorystoreCpuUsageCores, rate: true, filter: primaryOnly},
		{product: memorystoreRedis, name: "stats/memory/usage", desc: dMemorystoreMemoryUsed, filter: primaryOnly},
		{product: memorystoreRedis, name: "stats/network_traffic", desc: dMemorystoreNetworkBytes, rate: true, label: "rx", filter: primaryOnly + ` AND metric.labels.direction="in"`},
		{product: memorystoreRedis, name: "stats/network_traffic", desc: dMemorystoreNetworkBytes, rate: true, label: "tx", filter: primaryOnly + ` AND metric.labels.direction="out"`},

		{product: memorystoreValkey, name: "cpu/average_utilization", desc: dMemorystoreCpuUsage, scale: 100, filter: primaryOnly},
		{product: memorystoreValkey, name: "memory/total_used_memory", desc: dMemorystoreMemoryUsed},

		{product: memorystoreMemcached, name: "cpu/usage_time", desc: dMemorystoreCpuUsageCores, rate: true},
		{product: memorystoreMemcached, name: "cache_memory", desc: dMemorystoreMemoryUsed, filter: `metric.labels.used="1"`},
		{product: memorystoreMemcached, name: "received_bytes_count", desc: dMemorystoreNetworkBytes, rate: true, label: "rx"},
		{product: memorystoreMemcached, name: "sent_bytes_count", desc: dMemorystoreNetworkBytes, rate: true, label: "tx"},
	}
)

type monitoringProduct struct {
	prefix   string
	resource string
	id       func(map[string]string) string
}

type monitoringMetric struct {
	product  monitoringProduct
	name     string
	desc     *prometheus.Desc
	scale    float64
	rate     bool
	label    string
	labelKey string
	filter   string
}

type monitoringValue struct {
	desc  *prometheus.Desc
	value float64
	label string
}

type Monitoring struct {
	client  *monitoring.Service
	project string

	lock   sync.RWMutex
	values map[string][]monitoringValue
}

func NewMonitoring(client *monitoring.Service, project string) *Monitoring {
	return &Monitoring{client: client, project: project, values: map[string][]monitoringValue{}}
}

func (m *Monitoring) refresh(d *Discoverer) {
	now := time.Now()
	values := map[string][]monitoringValue{}
	for _, metric := range monitoringMetrics {
		filter := fmt.Sprintf(`metric.type="%s%s" AND resource.type="%s"`, metric.product.prefix, metric.name, metric.product.resource)
		if metric.filter != "" {
			filter += " AND " + metric.filter
		}
		aligner := "ALIGN_MEAN"
		if metric.rate {
			aligner = "ALIGN_RATE"
		}
		call := m.client.Projects.TimeSeries.List("projects/" + m.project).
			Filter(filter).
			IntervalStartTime(now.Add(-5 * time.Minute).Format(time.RFC3339)).
			IntervalEndTime(now.Format(time.RFC3339)).
			AggregationAlignmentPeriod("60s").
			AggregationPerSeriesAligner(aligner)
		ctx, cancel := d.apiContext()
		err := call.Pages(ctx, func(page *monitoring.ListTimeSeriesResponse) error {
			for _, ts := range page.TimeSeries {
				if ts.Resource == nil || len(ts.Points) == 0 {
					continue
				}
				id := metric.product.id(ts.Resource.Labels)
				v := pointValue(ts.Points[0]) // points are returned newest first
				if metric.scale != 0 {
					v *= metric.scale
				}
				label := metric.label
				if metric.labelKey != "" && ts.Metric != nil {
					label = strings.ToLower(ts.Metric.Labels[metric.labelKey])
				}
				merged := false
				for i := range values[id] {
					if mv := &values[id][i]; mv.desc == metric.desc && mv.label == label {
						mv.value += v
						merged = true
						break
					}
				}
				if !merged {
					values[id] = append(values[id], monitoringValue{desc: metric.desc, value: v, label: label})
				}
			}
			return nil
		})
		cancel()
		if err != nil {
			var apiErr *googleapi.Error
			if errors.As(err, &apiErr) && apiErr.Code == 404 { // the metric doesn't exist in this project
				continue
			}
			d.registerError(err)
		}
	}
	m.lock.Lock()
	m.values = values
	m.lock.Unlock()
}

func (m *Monitoring) collect(id string, ch chan<- prometheus.Metric) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, v := range m.values[id] {
		if v.label != "" {
			ch <- common.Gauge(v.desc, v.value, v.label)
		} else {
			ch <- common.Gauge(v.desc, v.value)
		}
	}
}

func pointValue(p *monitoring.Point) float64 {
	if p == nil || p.Value == nil {
		return 0
	}
	switch {
	case p.Value.DoubleValue != nil:
		return *p.Value.DoubleValue
	case p.Value.Int64Value != nil:
		return float64(*p.Value.Int64Value)
	}
	return 0
}
