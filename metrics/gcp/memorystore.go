package gcp

import (
	"strconv"
	"strings"

	"cloud.google.com/go/memorystore/apiv1/memorystorepb"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"
	memcache "google.golang.org/api/memcache/v1"
	redis "google.golang.org/api/redis/v1"
	"k8s.io/klog"
)

type memorystoreInfo struct {
	id                                  string
	project, region, zone               string
	instance                            string
	engine, version, tier, memorySizeGb string
	host, port                          string
	state                               string
	cpuCores                            float64 // 0 if the API doesn't return it (only Memcached does)
	memoryBytes                         float64
	labels                              map[string]string
}

type MemorystoreCollector struct {
	discoverer *Discoverer
	info       memorystoreInfo
}

func (c *MemorystoreCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("gcp_memorystore_collector", "", nil, nil)
}

func (c *MemorystoreCollector) Collect(ch chan<- prometheus.Metric) {
	i := c.info
	ch <- common.Gauge(dMemorystoreStatus, 1, i.state)
	ch <- common.Gauge(dMemorystoreInfo, 1,
		i.project, i.region, i.zone, i.host, i.port, i.engine, i.version, i.tier, i.memorySizeGb, i.instance,
	)
	if i.cpuCores > 0 {
		ch <- common.Gauge(dMemorystoreCpuCores, i.cpuCores)
	}
	if i.memoryBytes > 0 {
		ch <- common.Gauge(dMemorystoreMemoryTotal, i.memoryBytes)
	}
	c.discoverer.monitoring.collect(i.id, ch)
}

const gb = 1 << 30

func (d *Discoverer) discoverMemorystore() {
	var found []memorystoreInfo
	var failed bool
	for _, list := range []func() ([]memorystoreInfo, error){d.listRedis, d.listMemcached, d.listValkey} {
		res, err := list()
		if err != nil {
			d.registerError(err)
			failed = true
		}
		found = append(found, res...)
	}

	seen := map[string]bool{}
	for _, info := range found {
		if !common.LabelsMatched(d.cfg.MemorystoreLabelFilters, info.labels) {
			klog.Infof("Memorystore instance %s (labels: %s) was skipped according to the label-based filters: %s", info.id, info.labels, d.cfg.MemorystoreLabelFilters)
			continue
		}
		seen[info.id] = true
		c := d.redisCollectors[info.id]
		if c == nil {
			klog.Infoln("new Memorystore instance found:", info.id)
			c = &MemorystoreCollector{discoverer: d}
			if err := prometheus.WrapRegistererWith(memorystoreLabels(info.id), d.reg).Register(c); err != nil {
				klog.Error(err)
				continue
			}
			d.redisCollectors[info.id] = c
		}
		c.info = info
	}
	if failed { // a product couldn't be listed: keep its collectors rather than dropping and re-adding them
		return
	}
	for id, c := range d.redisCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(memorystoreLabels(id), d.reg).Unregister(c)
			delete(d.redisCollectors, id)
		}
	}
}

func (d *Discoverer) location() string {
	if d.region != "" {
		return d.region
	}
	return "-"
}

// projects/<project>/locations/<region>/instances/<name>
func parseResourceName(name string) (region, instance string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 {
		return "", "", false
	}
	return parts[3], parts[5], true
}

func (d *Discoverer) listRedis() ([]memorystoreInfo, error) {
	var res []memorystoreInfo
	parent := "projects/" + d.project + "/locations/" + d.location()
	ctx, cancel := d.apiContext()
	defer cancel()
	err := d.redisClient.Projects.Locations.Instances.List(parent).Pages(ctx, func(page *redis.ListInstancesResponse) error {
		for _, i := range page.Instances {
			region, name, ok := parseResourceName(i.Name)
			if !ok {
				continue
			}
			res = append(res, memorystoreInfo{
				id:           d.project + "/" + region + "/" + name,
				project:      d.project,
				region:       region,
				zone:         i.CurrentLocationId,
				instance:     name,
				engine:       "redis",
				version:      strings.ReplaceAll(strings.TrimPrefix(strings.ToLower(i.RedisVersion), "redis_"), "_", "."),
				tier:         strings.ToLower(i.Tier),
				memorySizeGb: strconv.FormatInt(i.MemorySizeGb, 10),
				host:         i.Host,
				port:         strconv.FormatInt(i.Port, 10),
				state:        i.State,
				memoryBytes:  float64(i.MemorySizeGb) * gb,
				labels:       i.Labels,
			})
		}
		return nil
	})
	return res, err
}

func (d *Discoverer) listMemcached() ([]memorystoreInfo, error) {
	var res []memorystoreInfo
	parent := "projects/" + d.project + "/locations/" + d.location()
	ctx, cancel := d.apiContext()
	defer cancel()
	err := d.memcacheClient.Projects.Locations.Instances.List(parent).Pages(ctx, func(page *memcache.ListInstancesResponse) error {
		for _, i := range page.Instances {
			region, name, ok := parseResourceName(i.Name)
			if !ok {
				continue
			}
			var memoryGb string
			var cpuCores, memoryBytes float64
			if i.NodeConfig != nil {
				memoryGb = strconv.FormatFloat(float64(i.NodeConfig.MemorySizeMb)/1024, 'f', -1, 64)
				cpuCores = float64(i.NodeConfig.CpuCount)
				memoryBytes = float64(i.NodeConfig.MemorySizeMb) * (1 << 20)
			}
			for _, n := range i.MemcacheNodes {
				res = append(res, memorystoreInfo{
					id:           d.project + "/" + region + "/" + name + "/" + n.NodeId,
					project:      d.project,
					region:       region,
					zone:         n.Zone,
					instance:     name,
					engine:       "memcached",
					version:      strings.ReplaceAll(strings.TrimPrefix(strings.ToLower(i.MemcacheVersion), "memcache_"), "_", "."),
					memorySizeGb: memoryGb,
					host:         n.Host,
					port:         strconv.FormatInt(n.Port, 10),
					state:        n.State,
					cpuCores:     cpuCores,
					memoryBytes:  memoryBytes,
					labels:       i.Labels,
				})
			}
		}
		return nil
	})
	return res, err
}

func (d *Discoverer) listValkey() ([]memorystoreInfo, error) {
	var res []memorystoreInfo
	ctx, cancel := d.apiContext()
	defer cancel()
	it := d.valkeyClient.ListInstances(ctx, &memorystorepb.ListInstancesRequest{Parent: "projects/" + d.project + "/locations/" + d.location()})
	for {
		i, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return res, err
		}
		region, name, ok := parseResourceName(i.GetName())
		if !ok {
			continue
		}
		var host, port string
		for _, ep := range i.GetEndpoints() {
			for _, c := range ep.GetConnections() {
				if a := c.GetPscAutoConnection(); a.GetConnectionType() == memorystorepb.ConnectionType_CONNECTION_TYPE_DISCOVERY {
					host, port = a.GetIpAddress(), strconv.Itoa(int(a.GetPort()))
				} else if m := c.GetPscConnection(); m.GetConnectionType() == memorystorepb.ConnectionType_CONNECTION_TYPE_DISCOVERY {
					host, port = m.GetIpAddress(), strconv.Itoa(int(m.GetPort()))
				}
			}
		}
		res = append(res, memorystoreInfo{
			id:           d.project + "/" + region + "/" + name,
			project:      d.project,
			region:       region,
			instance:     name,
			engine:       "valkey",
			version:      strings.ReplaceAll(strings.TrimPrefix(strings.ToLower(i.GetEngineVersion()), "valkey_"), "_", "."),
			tier:         strings.ReplaceAll(strings.ToLower(i.GetNodeType().String()), "_", "-"), // as in the docs and gcloud: shared-core-nano
			memorySizeGb: strconv.FormatFloat(i.GetNodeConfig().GetSizeGb(), 'f', -1, 64),
			host:         host,
			port:         port,
			state:        i.GetState().String(),
			memoryBytes:  i.GetNodeConfig().GetSizeGb() * gb,
			labels:       i.GetLabels(),
		})
	}
	return res, nil
}
