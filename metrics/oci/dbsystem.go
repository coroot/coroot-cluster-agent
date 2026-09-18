package oci

import (
	"regexp"
	"strconv"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/logparser"
	"github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/oracle/oci-go-sdk/v65/psql"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

type dbInfo struct {
	id, name        string
	compartment     string
	region, ad      string
	engine, version string
	shape           string
	highlyAvailable bool
	host, port      string
	state           string
	cpuCores        float64
	memoryBytes     float64
	tags            map[string]string
	primary         string
	monitoringID    string
	logResource     string
	logSubject      string
}

type DBCollector struct {
	discoverer *Discoverer
	info       dbInfo
	logs       *LogReader
}

func (c *DBCollector) Stop() {
	if c.logs != nil {
		c.logs.Stop()
	}
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("oci_db_collector", "", nil, nil)
}

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	i := c.info
	ch <- common.Gauge(dDBStatus, 1, i.state)
	ch <- common.Gauge(dDBInfo, 1,
		i.name, i.compartment, i.region, i.ad, i.host, i.port, i.engine, i.version, i.shape, strconv.FormatBool(i.highlyAvailable), i.primary,
	)
	if i.cpuCores > 0 {
		ch <- common.Gauge(dDBCpuCores, i.cpuCores)
	}
	if i.memoryBytes > 0 {
		ch <- common.Gauge(dDBMemoryTotal, i.memoryBytes)
	}
	c.discoverer.monitoring.collect(i.monitoringID, ch)
	if i.monitoringID != i.id { // the series of the DB system itself go to the primary
		c.discoverer.monitoring.collect(i.id, ch)
	}
	var counters []logparser.LogCounter
	if c.logs != nil {
		counters = c.logs.Counters()
	} else if c.discoverer.logCounters != nil {
		counters = c.discoverer.logCounters(i.name)
	}
	for _, lc := range counters {
		ch <- common.Counter(dDBLogMessages, float64(lc.Messages), lc.Level.String(), lc.Hash, lc.Sample)
	}
}

const gb = 1 << 30

func (d *Discoverer) discoverDBSystems() {
	var found []dbInfo
	var failed bool
	for _, compartment := range d.compartments {
		for _, list := range []func(string) ([]dbInfo, error){d.listMySQL, d.listPostgreSQL} {
			res, err := list(compartment)
			if err != nil {
				d.registerError(err)
				failed = true
			}
			found = append(found, res...)
		}
	}
	tagsByName := map[string]map[string]string{}
	for _, info := range found {
		tagsByName[info.name] = info.tags
	}
	seen := map[string]bool{}
	for _, info := range found {
		if info.monitoringID == "" {
			info.monitoringID = info.id
		}
		if info.logResource == "" {
			info.logResource = info.id
		}
		if !common.LabelsMatched(d.cfg.DBTagFilters, info.tags) && (info.primary == "" || !common.LabelsMatched(d.cfg.DBTagFilters, tagsByName[info.primary])) {
			klog.Infof("OCI DB system %s (tags: %s) was skipped according to the tag-based filters: %s", info.name, info.tags, d.cfg.DBTagFilters)
			continue
		}
		seen[info.id] = true
		c := d.dbCollectors[info.id]
		if c == nil {
			klog.Infoln("new OCI DB system found:", info.name)
			c = &DBCollector{discoverer: d, info: info}
			if info.engine == "postgres" { // MySQL HeatWave doesn't publish its logs to OCI Logging
				c.logs = NewLogReader(d, info.logResource, info.logSubject, dbLogService(info.id), "ocidb:"+info.name, *flags.CollectOCILogs)
			}
			if err := prometheus.WrapRegistererWith(dbLabels(info.id), d.reg).Register(c); err != nil {
				klog.Error(err)
				c.Stop()
				continue
			}
			d.dbCollectors[info.id] = c
		} else if c.logs != nil && info.logSubject != "" && info.logSubject != c.info.logSubject {
			c.Stop()
			c.logs = NewLogReader(d, info.logResource, info.logSubject, dbLogService(info.id), "ocidb:"+info.name, *flags.CollectOCILogs)
		}
		c.info = info
	}
	if failed { // a product couldn't be listed: keep its collectors rather than dropping and re-adding them
		return
	}
	for id, c := range d.dbCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(dbLabels(id), d.reg).Unregister(c)
			c.Stop()
			delete(d.dbCollectors, id)
		}
	}
}

func (d *Discoverer) listMySQL(compartment string) ([]dbInfo, error) {
	var res []dbInfo
	systems := map[string]string{}
	req := mysql.ListDbSystemsRequest{CompartmentId: &compartment, RequestMetadata: retry()}
	for {
		ctx, cancel := d.apiContext()
		resp, err := d.mysqlClient.ListDbSystems(ctx, req)
		cancel()
		if err != nil {
			return res, err
		}
		for _, s := range resp.Items {
			if s.LifecycleState == mysql.DbSystemLifecycleStateDeleted {
				continue
			}
			systems[str(s.Id)] = str(s.DisplayName)
			info := dbInfo{
				id:              str(s.Id),
				name:            str(s.DisplayName),
				compartment:     str(s.CompartmentId),
				region:          d.region,
				ad:              availabilityDomain(str(s.AvailabilityDomain), d.region),
				engine:          "mysql",
				version:         str(s.MysqlVersion),
				shape:           str(s.ShapeName),
				highlyAvailable: s.IsHighlyAvailable != nil && *s.IsHighlyAvailable,
				state:           string(s.LifecycleState),
				tags:            s.FreeformTags,
			}
			for _, e := range s.Endpoints {
				if e.ResourceType == mysql.DbSystemEndpointResourceTypeDbsystem && e.IpAddress != nil && e.Port != nil {
					info.host, info.port = *e.IpAddress, strconv.Itoa(*e.Port)
					break
				}
			}
			res = append(res, info)
		}
		if resp.OpcNextPage == nil {
			break
		}
		req.Page = resp.OpcNextPage
	}
	replicas := mysql.ListReplicasRequest{CompartmentId: &compartment, RequestMetadata: retry()}
	for {
		ctx, cancel := d.apiContext()
		resp, err := d.mysqlReplicasClient.ListReplicas(ctx, replicas)
		cancel()
		if err != nil {
			return res, err
		}
		for _, r := range resp.Items {
			if r.LifecycleState == mysql.ReplicaSummaryLifecycleStateDeleted {
				continue
			}
			info := dbInfo{
				id:          str(r.Id),
				name:        str(r.DisplayName),
				compartment: str(r.CompartmentId),
				region:      d.region,
				ad:          availabilityDomain(str(r.AvailabilityDomain), d.region),
				engine:      "mysql",
				version:     str(r.MysqlVersion),
				shape:       str(r.ShapeName),
				state:       string(r.LifecycleState),
				tags:        r.FreeformTags,
				primary:     systems[str(r.DbSystemId)],
				host:        str(r.IpAddress),
			}
			if r.Port != nil {
				info.port = strconv.Itoa(*r.Port)
			}
			res = append(res, info)
		}
		if resp.OpcNextPage == nil {
			break
		}
		replicas.Page = resp.OpcNextPage
	}
	return res, nil
}

func (d *Discoverer) listPostgreSQL(compartment string) ([]dbInfo, error) {
	var res []dbInfo
	req := psql.ListDbSystemsRequest{CompartmentId: &compartment, RequestMetadata: retry()}
	for {
		ctx, cancel := d.apiContext()
		resp, err := d.psqlClient.ListDbSystems(ctx, req)
		cancel()
		if err != nil {
			return res, err
		}
		for _, s := range resp.Items {
			if s.LifecycleState == psql.DbSystemLifecycleStateDeleted {
				continue
			}
			info := dbInfo{
				id:          str(s.Id),
				name:        str(s.DisplayName),
				compartment: str(s.CompartmentId),
				region:      d.region,
				engine:      "postgres",
				version:     str(s.DbVersion),
				shape:       str(s.Shape),
				state:       string(s.LifecycleState),
				tags:        s.FreeformTags,
			}
			if s.InstanceOcpuCount != nil {
				info.cpuCores = float64(*s.InstanceOcpuCount) * 2
			}
			if s.InstanceMemorySizeInGBs != nil {
				info.memoryBytes = float64(*s.InstanceMemorySizeInGBs) * gb
			}
			if s.InstanceCount != nil && *s.InstanceCount > 1 {
				info.highlyAvailable = true
			}
			ctx, cancel := d.apiContext()
			system, err := d.psqlClient.GetDbSystem(ctx, psql.GetDbSystemRequest{DbSystemId: s.Id, RequestMetadata: retry()})
			cancel()
			if err != nil {
				d.registerError(err)
			}
			ctx, cancel = d.apiContext()
			primary, err := d.psqlClient.GetPrimaryDbInstance(ctx, psql.GetPrimaryDbInstanceRequest{DbSystemId: s.Id, RequestMetadata: retry()})
			cancel()
			if err != nil {
				d.registerError(err)
			}
			ctx, cancel = d.apiContext()
			details, err := d.psqlClient.GetConnectionDetails(ctx, psql.GetConnectionDetailsRequest{DbSystemId: s.Id, RequestMetadata: retry()})
			cancel()
			if err != nil {
				d.registerError(err)
			} else if details.PrimaryDbEndpoint != nil {
				info.host, info.port = endpoint(details.PrimaryDbEndpoint)
			}
			endpoints := map[string]*psql.Endpoint{}
			for _, e := range details.InstanceEndpoints {
				endpoints[str(e.DbInstanceId)] = e.Endpoint
			}
			var replicas []dbInfo
			if primary.DbInstanceId == nil {
				system.Instances = nil
			}
			for _, i := range system.Instances {
				if str(i.Id) == str(primary.DbInstanceId) {
					info.ad = availabilityDomain(str(i.AvailabilityDomain), d.region)
					info.monitoringID, info.logResource, info.logSubject = str(i.Id), info.id, str(i.Id)
					continue
				}
				replica := info
				replica.id, replica.monitoringID, replica.primary = str(i.Id), str(i.Id), info.name
				replica.logResource, replica.logSubject = info.id, str(i.Id)
				replica.name = str(i.DisplayName)
				replica.ad = availabilityDomain(str(i.AvailabilityDomain), d.region)
				replica.state = string(i.LifecycleState)
				replica.host, replica.port = "", ""
				if ep := endpoints[str(i.Id)]; ep != nil {
					replica.host, replica.port = endpoint(ep)
				}
				replicas = append(replicas, replica)
			}
			res = append(append(res, info), replicas...)
		}
		if resp.OpcNextPage == nil {
			break
		}
		req.Page = resp.OpcNextPage
	}
	return res, nil
}

func endpoint(ep *psql.Endpoint) (host, port string) {
	host = str(ep.IpAddress)
	if host == "" {
		host = str(ep.Fqdn)
	}
	if ep.Port != nil {
		port = strconv.Itoa(*ep.Port)
	}
	return host, port
}

var adSuffix = regexp.MustCompile(`-AD-(\d+)$`)

func availabilityDomain(ad, region string) string {
	if m := adSuffix.FindStringSubmatch(ad); m != nil {
		return region + "-ad-" + m[1]
	}
	return ""
}
