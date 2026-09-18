package oci

import (
	"strconv"
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/oracle/oci-go-sdk/v65/redis"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

type cacheInfo struct {
	id, name        string
	compartment     string
	region          string
	engine, version string
	nodeCount       int
	nodeMemoryGb    float64
	host, port      string
	state           string
	tags            map[string]string
}

type CacheCollector struct {
	discoverer *Discoverer
	info       cacheInfo
	logs       *LogReader
}

func (c *CacheCollector) Stop() {
	c.logs.Stop()
}

func (c *CacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("oci_cache_collector", "", nil, nil)
}

func (c *CacheCollector) Collect(ch chan<- prometheus.Metric) {
	i := c.info
	ch <- common.Gauge(dCacheStatus, 1, i.state)
	ch <- common.Gauge(dCacheInfo, 1,
		i.name, i.compartment, i.region, i.host, i.port, i.engine, i.version, strconv.Itoa(i.nodeCount), strconv.FormatFloat(i.nodeMemoryGb, 'f', -1, 64),
	)
	if i.nodeMemoryGb > 0 {
		ch <- common.Gauge(dCacheMemoryTotal, i.nodeMemoryGb*gb)
	}
	c.discoverer.monitoring.collect(i.id, ch)
	for _, lc := range c.logs.Counters() {
		ch <- common.Counter(dCacheLogMessages, float64(lc.Messages), lc.Level.String(), lc.Hash, lc.Sample)
	}
}

func (d *Discoverer) discoverCaches() {
	var found []cacheInfo
	var failed bool
	for _, compartment := range d.compartments {
		res, err := d.listCaches(compartment)
		if err != nil {
			d.registerError(err) // reported with the discovery summary
			failed = true
		}
		found = append(found, res...)
	}
	seen := map[string]bool{}
	for _, info := range found {
		if !common.LabelsMatched(d.cfg.CacheTagFilters, info.tags) {
			klog.Infof("OCI Cache cluster %s (tags: %s) was skipped according to the tag-based filters: %s", info.name, info.tags, d.cfg.CacheTagFilters)
			continue
		}
		seen[info.id] = true
		c := d.cacheCollectors[info.id]
		if c == nil {
			klog.Infoln("new OCI Cache cluster found:", info.name)
			c = &CacheCollector{discoverer: d, info: info}
			c.logs = NewLogReader(d, info.id, "", cacheLogService(info.id), "ocicache:"+info.name, *flags.CollectOCILogs)
			if err := prometheus.WrapRegistererWith(cacheLabels(info.id), d.reg).Register(c); err != nil {
				klog.Error(err)
				c.Stop()
				continue
			}
			d.cacheCollectors[info.id] = c
		}
		c.info = info
	}
	if failed { // a compartment couldn't be listed: keep its collectors rather than dropping and re-adding them
		return
	}
	for id, c := range d.cacheCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(cacheLabels(id), d.reg).Unregister(c)
			c.Stop()
			delete(d.cacheCollectors, id)
		}
	}
}

func (d *Discoverer) listCaches(compartment string) ([]cacheInfo, error) {
	var res []cacheInfo
	req := redis.ListRedisClustersRequest{CompartmentId: &compartment, RequestMetadata: retry()}
	for {
		ctx, cancel := d.apiContext()
		resp, err := d.cacheClient.ListRedisClusters(ctx, req)
		cancel()
		if err != nil {
			return res, err
		}
		for _, s := range resp.Items {
			if s.LifecycleState == redis.RedisClusterLifecycleStateDeleted {
				continue
			}
			// software versions are REDIS_7_0, VALKEY_7_2, ...
			engine, version, _ := strings.Cut(strings.ToLower(string(s.SoftwareVersion)), "_")
			info := cacheInfo{
				id:          str(s.Id),
				name:        str(s.DisplayName),
				compartment: str(s.CompartmentId),
				region:      d.region,
				engine:      engine,
				version:     strings.ReplaceAll(version, "_", "."),
				host:        str(s.PrimaryEndpointIpAddress),
				port:        "6379",
				state:       string(s.LifecycleState),
				tags:        s.FreeformTags,
			}
			if s.NodeCount != nil {
				info.nodeCount = *s.NodeCount
			}
			if s.NodeMemoryInGBs != nil {
				info.nodeMemoryGb = float64(*s.NodeMemoryInGBs)
			}
			res = append(res, info)
		}
		if resp.OpcNextPage == nil {
			break
		}
		req.Page = resp.OpcNextPage
	}
	return res, nil
}
