package oci

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dDBCpuUsage      = common.Desc("oci_db_cpu_usage_percent", "CPU utilization, percent")
	dDBCpuUsageCores = common.Desc("oci_db_cpu_usage_cores", "CPU usage, vCPUs (2 per OCPU)")
	dDBCpuCoresUsed  = dDBCpuCores // MySQL reports the allocated OCPUs through Monitoring
	dDBMemoryUsed    = common.Desc("oci_db_memory_used_bytes", "Memory usage")
	dDBMemoryUsage   = common.Desc("oci_db_memory_usage_percent", "Memory utilization, percent")
	dDBDiskTotal     = common.Desc("oci_db_disk_total_bytes", "Storage allocated")
	dDBDiskUsed      = common.Desc("oci_db_disk_used_bytes", "Storage used")
	dDBNetworkBytes  = common.Desc("oci_db_network_bytes_per_second", "Network throughput", "direction")
	dDBIOOps         = common.Desc("oci_db_io_ops_per_second", "Disk I/O operations per second", "operation")
	dDBIOBytes       = common.Desc("oci_db_io_bytes_per_second", "Disk I/O throughput", "operation")
	dDBIOLatency     = common.Desc("oci_db_io_latency_seconds", "Average disk I/O latency (PostgreSQL only)", "operation")

	dCacheCpuUsage     = common.Desc("oci_cache_cpu_usage_percent", "CPU utilization, percent")
	dCacheMemoryUsed   = common.Desc("oci_cache_memory_used_bytes", "Memory used by the engine")
	dCacheNetworkBytes = common.Desc("oci_cache_network_bytes_per_second", "Network throughput", "direction")

	monitoringMetrics = []monitoringMetric{
		// MySQL HeatWave: the DB system and read replica series are selected, the HeatWave nodes carry their own resourceType
		{namespace: "oci_mysql_database", name: "CPUUtilization", filter: `resourceType =~ "mysql|replica"`, desc: dDBCpuUsage},
		{namespace: "oci_mysql_database", name: "OCPUsAllocated", filter: `resourceType =~ "mysql|replica"`, desc: dDBCpuCoresUsed, scale: 2},
		{namespace: "oci_mysql_database", name: "OCPUsUsed", filter: `resourceType =~ "mysql|replica"`, desc: dDBCpuUsageCores, scale: 2},
		{namespace: "oci_mysql_database", name: "MemoryAllocated", filter: `resourceType =~ "mysql|replica"`, desc: dDBMemoryTotal, scale: gb},
		{namespace: "oci_mysql_database", name: "MemoryUsed", filter: `resourceType =~ "mysql|replica"`, desc: dDBMemoryUsed, scale: gb},
		{namespace: "oci_mysql_database", name: "StorageAllocated", desc: dDBDiskTotal, scale: gb},
		{namespace: "oci_mysql_database", name: "StorageUsed", desc: dDBDiskUsed, scale: gb},
		{namespace: "oci_mysql_database", name: "NetworkReceiveBytes", filter: `resourceType =~ "mysql|replica"`, desc: dDBNetworkBytes, rate: true, label: "rx"},
		{namespace: "oci_mysql_database", name: "NetworkTransmitBytes", filter: `resourceType =~ "mysql|replica"`, desc: dDBNetworkBytes, rate: true, label: "tx"},
		{namespace: "oci_mysql_database", name: "DbVolumeReadOperations", filter: `resourceType =~ "mysql|replica"`, desc: dDBIOOps, rate: true, label: "read"},
		{namespace: "oci_mysql_database", name: "DbVolumeWriteOperations", filter: `resourceType =~ "mysql|replica"`, desc: dDBIOOps, rate: true, label: "write"},
		{namespace: "oci_mysql_database", name: "DbVolumeReadBytes", filter: `resourceType =~ "mysql|replica"`, desc: dDBIOBytes, rate: true, label: "read"},
		{namespace: "oci_mysql_database", name: "DbVolumeWriteBytes", filter: `resourceType =~ "mysql|replica"`, desc: dDBIOBytes, rate: true, label: "write"},
		// PostgreSQL: memory and vCPUs come from the API, Monitoring reports utilization
		{namespace: "oci_postgresql", name: "CpuUtilization", desc: dDBCpuUsage},
		{namespace: "oci_postgresql", name: "MemoryUtilization", desc: dDBMemoryUsage},
		{namespace: "oci_postgresql", name: "UsedStorage", desc: dDBDiskUsed, scale: 1e9},
		{namespace: "oci_postgresql", name: "ReadIops", desc: dDBIOOps, label: "read"},
		{namespace: "oci_postgresql", name: "WriteIops", desc: dDBIOOps, label: "write"},
		{namespace: "oci_postgresql", name: "ReadThroughput", desc: dDBIOBytes, label: "read", scale: 1024},
		{namespace: "oci_postgresql", name: "WriteThroughput", desc: dDBIOBytes, label: "write", scale: 1024},
		{namespace: "oci_postgresql", name: "ReadLatency", desc: dDBIOLatency, label: "read", scale: 1e-3},
		{namespace: "oci_postgresql", name: "WriteLatency", desc: dDBIOLatency, label: "write", scale: 1e-3},
		// OCI Cache
		{namespace: "oci_redis", name: "CPUUtilization", desc: dCacheCpuUsage},
		{namespace: "oci_redis", name: "UsedMemory", desc: dCacheMemoryUsed},
		{namespace: "oci_redis", name: "NetworkBytesIn", desc: dCacheNetworkBytes, label: "rx"},
		{namespace: "oci_redis", name: "NetworkBytesOut", desc: dCacheNetworkBytes, label: "tx"},
	}
)

const monitoringTimeout = 60 * time.Second

type monitoringMetric struct {
	namespace string
	name      string
	filter    string
	desc      *prometheus.Desc
	scale     float64
	rate      bool
	label     string
}

type monitoringValue struct {
	desc  *prometheus.Desc
	value float64
	label string
}

type Monitoring struct {
	client       monitoring.MonitoringClient
	compartments []string

	lock   sync.RWMutex
	values map[string][]monitoringValue
}

func NewMonitoring(client monitoring.MonitoringClient, compartments []string) *Monitoring {
	return &Monitoring{client: client, compartments: compartments, values: map[string][]monitoringValue{}}
}

func (m *Monitoring) refresh(d *Discoverer) {
	now := time.Now()
	values := map[string][]monitoringValue{}
	var lock sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for _, compartment := range m.compartments {
		for _, metric := range monitoringMetrics {
			wg.Add(1)
			sem <- struct{}{}
			go func(compartment string, metric monitoringMetric) {
				defer wg.Done()
				defer func() { <-sem }()
				query := metric.name + "[1m]"
				if metric.filter != "" {
					query += "{" + metric.filter + "}"
				}
				if metric.rate {
					query += ".rate()"
				} else {
					query += ".mean()"
				}
				req := monitoring.SummarizeMetricsDataRequest{
					CompartmentId:   ocicommon.String(compartment),
					RequestMetadata: retry(),
					SummarizeMetricsDataDetails: monitoring.SummarizeMetricsDataDetails{
						Namespace: ocicommon.String(metric.namespace),
						Query:     ocicommon.String(query),
						StartTime: &ocicommon.SDKTime{Time: now.Add(-5 * time.Minute)},
						EndTime:   &ocicommon.SDKTime{Time: now},
					},
				}
				ctx, cancel := context.WithTimeout(d.ctx, monitoringTimeout)
				resp, err := m.client.SummarizeMetricsData(ctx, req)
				cancel()
				if err != nil {
					d.registerError(fmt.Errorf("%s/%s: %w", metric.namespace, metric.name, err))
					return
				}
				lock.Lock()
				defer lock.Unlock()
				for _, ts := range resp.Items {
					id := ts.Dimensions["dbInstanceId"]
					if id == "" {
						id = ts.Dimensions["resourceId"]
					}
					if id == "" || len(ts.AggregatedDatapoints) == 0 {
						continue
					}
					last := ts.AggregatedDatapoints[len(ts.AggregatedDatapoints)-1]
					if last.Value == nil {
						continue
					}
					v := *last.Value
					if metric.scale != 0 {
						v *= metric.scale
					}
					merged := false
					for i := range values[id] {
						if mv := &values[id][i]; mv.desc == metric.desc && mv.label == metric.label {
							mv.value += v
							merged = true
							break
						}
					}
					if !merged {
						values[id] = append(values[id], monitoringValue{desc: metric.desc, value: v, label: metric.label})
					}
				}
			}(compartment, metric)
		}
	}
	wg.Wait()
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
