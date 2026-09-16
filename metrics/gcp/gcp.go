package gcp

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/compute/metadata"
	memorystore "cloud.google.com/go/memorystore/apiv1"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	logging "google.golang.org/api/logging/v2"
	memcache "google.golang.org/api/memcache/v1"
	monitoring "google.golang.org/api/monitoring/v3"
	"google.golang.org/api/option"
	redis "google.golang.org/api/redis/v1"
	sqladmin "google.golang.org/api/sqladmin/v1"
	"k8s.io/klog"
)

const (
	discoveryInterval = time.Minute
	apiTimeout        = 30 * time.Second
)

var (
	dError = common.Desc("gcp_discovery_error", "GCP discovery error", "error")

	dCloudSQLInfo = common.Desc("gcp_cloudsql_info", "Cloud SQL instance info",
		"project", "region", "zone", "ipv4", "port", "engine", "engine_version", "tier", "availability_type", "connection_name",
		"instance_type", "primary_instance",
	)
	dCloudSQLStatus      = common.Desc("gcp_cloudsql_status", "Status of the Cloud SQL instance", "status")
	dCloudSQLLogMessages = common.Desc("gcp_cloudsql_log_messages_total", "Number of messages in the instance's logs grouped by the automatically extracted repeated pattern", "level", "pattern_hash", "sample")

	dMemorystoreInfo = common.Desc("gcp_memorystore_info", "Memorystore instance info",
		"project", "region", "zone", "ipv4", "port", "engine", "engine_version", "tier", "memory_size_gb", "instance",
	)
	dMemorystoreStatus      = common.Desc("gcp_memorystore_status", "Status of the Memorystore instance", "status")
	dMemorystoreCpuCores    = common.Desc("gcp_memorystore_cpu_cores", "Number of vCPUs of the node")
	dMemorystoreMemoryTotal = common.Desc("gcp_memorystore_memory_total_bytes", "Memory capacity of the node")
)

type Discoverer struct {
	cfg     *config.GCPConfig
	k8s     *k8s.K8S
	project string
	region  string // the configured region, the cluster's one by default, or "" for all regions
	ctx     context.Context
	reg     prometheus.Registerer
	stop    chan struct{}

	sqlClient        *sqladmin.Service
	redisClient      *redis.Service
	memcacheClient   *memcache.Service
	valkeyClient     *memorystore.Client
	monitoringClient *monitoring.Service
	loggingClient    *logging.Service
	monitoring       *Monitoring

	errors      map[string]bool
	errorsLock  sync.RWMutex
	lastSummary string

	sqlCollectors   map[string]*CloudSQLCollector
	redisCollectors map[string]*MemorystoreCollector

	endpointsLock        sync.RWMutex
	cloudsqlEndpoints    map[string]common.Endpoint
	cloudsqlReplicas     map[string][]string
	memorystoreEndpoints map[string][]common.Endpoint
}

func (d *Discoverer) CloudSQLEndpoint(name string) (common.Endpoint, bool) {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	e, ok := d.cloudsqlEndpoints[name]
	return e, ok
}

func (d *Discoverer) CloudSQLReplicas(primary string) []string {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.cloudsqlReplicas[primary]
}

func (d *Discoverer) MemorystoreEndpoints(name string) []common.Endpoint {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.memorystoreEndpoints[name]
}

func NewDiscoverer(cfg *config.GCPConfig, k8s *k8s.K8S, reg prometheus.Registerer) (*Discoverer, error) {
	ctx := context.Background()
	d := &Discoverer{
		cfg:    cfg,
		k8s:    k8s,
		ctx:    ctx,
		reg:    reg,
		stop:   make(chan struct{}),
		errors: map[string]bool{},

		sqlCollectors:   map[string]*CloudSQLCollector{},
		redisCollectors: map[string]*MemorystoreCollector{},
	}
	if err := d.init(); err != nil {
		return nil, err
	}
	if err := reg.Register(d); err != nil {
		return nil, err
	}
	go func() {
		d.discover()
		t := time.NewTicker(discoveryInterval)
		defer t.Stop()
		for {
			select {
			case <-d.stop:
				return
			case <-t.C:
				d.discover()
			}
		}
	}()
	return d, nil
}

func (d *Discoverer) Config() *config.GCPConfig {
	return d.cfg
}

func (d *Discoverer) init() error {
	cfg := d.cfg
	var creds *google.Credentials
	var err error
	if cfg.CredentialsJSON != "" {
		creds, err = google.CredentialsFromJSONWithTypeAndParams(d.ctx, []byte(cfg.CredentialsJSON), google.ServiceAccount, google.CredentialsParams{Scopes: []string{sqladmin.CloudPlatformScope}})
	} else {
		creds, err = google.FindDefaultCredentials(d.ctx, sqladmin.CloudPlatformScope)
	}
	if err != nil {
		return fmt.Errorf("GCP integration: failed to obtain credentials: %w", err)
	}
	project := cfg.ProjectID
	if project == "" {
		project = creds.ProjectID
	}
	if project == "" && metadata.OnGCE() {
		project, _ = metadata.ProjectIDWithContext(d.ctx)
	}
	if project == "" {
		return fmt.Errorf("GCP integration: the project is not configured and cannot be detected (set projectId)")
	}
	region := cfg.Region
	switch region {
	case "":
		region = d.clusterRegion()
	case "all":
		region = ""
	}
	sqlClient, err := sqladmin.NewService(d.ctx, option.WithCredentials(creds))
	if err != nil {
		return err
	}
	redisClient, err := redis.NewService(d.ctx, option.WithCredentials(creds))
	if err != nil {
		return err
	}
	memcacheClient, err := memcache.NewService(d.ctx, option.WithCredentials(creds))
	if err != nil {
		return err
	}
	valkeyClient, err := memorystore.NewRESTClient(d.ctx, option.WithCredentials(creds))
	if err != nil {
		return err
	}
	monitoringClient, err := monitoring.NewService(d.ctx, option.WithCredentials(creds))
	if err != nil {
		return err
	}
	loggingClient, err := logging.NewService(d.ctx, option.WithCredentials(creds))
	if err != nil {
		return err
	}
	d.project = project
	d.region = region
	d.sqlClient = sqlClient
	d.redisClient = redisClient
	d.memcacheClient = memcacheClient
	d.valkeyClient = valkeyClient
	d.monitoringClient = monitoringClient
	d.loggingClient = loggingClient
	d.monitoring = NewMonitoring(monitoringClient, project)
	source := "application default credentials"
	if cfg.CredentialsJSON != "" {
		source = "service account key"
	} else if metadata.OnGCE() {
		source = "metadata server (workload identity / service account of the VM)"
	}
	klog.Infof("GCP integration: project=%s, region=%s, credentials=%s", project, d.regionScope(), source)
	return nil
}

func (d *Discoverer) apiContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(d.ctx, apiTimeout)
}

func (d *Discoverer) clusterRegion() string {
	if region, _ := d.k8s.GetNodeRegion(d.ctx); region != "" {
		return region
	}
	if zone, err := metadata.ZoneWithContext(d.ctx); err == nil { // <region>-<letter>
		if i := strings.LastIndex(zone, "-"); i > 0 {
			return zone[:i]
		}
	}
	return ""
}

func (d *Discoverer) regionScope() string {
	if d.region == "" {
		return "all"
	}
	return d.region
}

func (d *Discoverer) Stop() {
	d.stop <- struct{}{}
	for id, c := range d.sqlCollectors {
		prometheus.WrapRegistererWith(cloudSQLLabels(id), d.reg).Unregister(c)
		c.Stop()
	}
	for id, c := range d.redisCollectors {
		prometheus.WrapRegistererWith(memorystoreLabels(id), d.reg).Unregister(c)
	}
	d.reg.Unregister(d)
	if d.valkeyClient != nil {
		_ = d.valkeyClient.Close()
	}
}

func (d *Discoverer) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("gcp_discoverer", "", nil, nil)
}

func (d *Discoverer) Collect(ch chan<- prometheus.Metric) {
	d.errorsLock.RLock()
	defer d.errorsLock.RUnlock()
	if len(d.errors) == 0 {
		ch <- common.Gauge(dError, 0, "")
		return
	}
	for e := range d.errors {
		ch <- common.Gauge(dError, 1, e)
	}
}

func (d *Discoverer) registerError(err error) {
	msg := err.Error()
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		msg = apiErr.Message
	}
	d.errorsLock.Lock()
	d.errors[msg] = true
	d.errorsLock.Unlock()
}

func (d *Discoverer) discover() {
	d.errorsLock.Lock()
	d.errors = map[string]bool{}
	d.errorsLock.Unlock()
	d.discoverCloudSQL()
	d.discoverMemorystore()
	d.publishEndpoints()
	if len(d.sqlCollectors) > 0 || len(d.redisCollectors) > 0 {
		d.monitoring.refresh(d)
	}

	d.errorsLock.RLock()
	errs := maps.Keys(d.errors)
	d.errorsLock.RUnlock()
	summary := fmt.Sprintf("GCP discovery (project=%s, region=%s): %d Cloud SQL instances, %d Memorystore instances", d.project, d.regionScope(), len(d.sqlCollectors), len(d.redisCollectors))
	switch {
	case len(errs) > 0:
		klog.Errorf("%s, errors: %s", summary, strings.Join(errs, "; "))
	case summary != d.lastSummary:
		klog.Infoln(summary)
	}
	d.lastSummary = summary
}

func (d *Discoverer) publishEndpoints() {
	sql := map[string]common.Endpoint{}
	replicas := map[string][]string{}
	for _, c := range d.sqlCollectors {
		if ip, port := c.address(); ip != "" {
			sql[c.instance.Name] = common.Endpoint{Host: ip, Port: port}
		}
		if _, primary, ok := strings.Cut(c.instance.MasterInstanceName, ":"); ok && primary != "" {
			replicas[primary] = append(replicas[primary], c.instance.Name)
		}
	}
	rd := map[string][]common.Endpoint{}
	for _, c := range d.redisCollectors {
		if c.info.host != "" {
			rd[c.info.instance] = append(rd[c.info.instance], common.Endpoint{Host: c.info.host, Port: c.info.port})
		}
	}
	d.endpointsLock.Lock()
	d.cloudsqlEndpoints = sql
	d.cloudsqlReplicas = replicas
	d.memorystoreEndpoints = rd
	d.endpointsLock.Unlock()
}

func cloudSQLLabels(id string) prometheus.Labels {
	return prometheus.Labels{"cloudsql_instance_id": id}
}

func memorystoreLabels(id string) prometheus.Labels {
	return prometheus.Labels{"memorystore_instance_id": id}
}

func labelsMatched(filters, labels map[string]string) bool {
	for name, desired := range filters {
		if matched, _ := filepath.Match(desired, labels[name]); !matched {
			return false
		}
	}
	return true
}
