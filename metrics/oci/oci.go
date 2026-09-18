package oci

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/logparser"
	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/oracle/oci-go-sdk/v65/loggingsearch"
	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/oracle/oci-go-sdk/v65/psql"
	"github.com/oracle/oci-go-sdk/v65/redis"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"k8s.io/klog"
)

const (
	discoveryInterval = time.Minute
	apiTimeout        = 30 * time.Second
)

var (
	dError = common.Desc("oci_discovery_error", "OCI discovery error", "error")

	dDBInfo = common.Desc("oci_db_info", "OCI DB system info (MySQL HeatWave or Database with PostgreSQL)",
		"name", "compartment", "region", "availability_domain", "ipv4", "port", "engine", "engine_version", "shape", "high_availability", "primary",
	)
	dDBStatus      = common.Desc("oci_db_status", "Status of the DB system", "status")
	dDBCpuCores    = common.Desc("oci_db_cpu_cores", "Number of vCPUs of the DB system (2 per OCPU)")
	dDBMemoryTotal = common.Desc("oci_db_memory_total_bytes", "Memory of the DB system")
	dDBLogMessages = common.Desc("oci_db_log_messages_total", "Number of messages in the DB system's log grouped by the automatically extracted repeated pattern", "level", "pattern_hash", "sample")

	dCacheInfo = common.Desc("oci_cache_info", "OCI Cache cluster info",
		"name", "compartment", "region", "ipv4", "port", "engine", "engine_version", "node_count", "node_memory_gb",
	)
	dCacheStatus      = common.Desc("oci_cache_status", "Status of the cache cluster", "status")
	dCacheMemoryTotal = common.Desc("oci_cache_memory_total_bytes", "Memory of a cache node")
	dCacheLogMessages = common.Desc("oci_cache_log_messages_total", "Number of messages in the cache cluster's engine log grouped by the automatically extracted repeated pattern", "level", "pattern_hash", "sample")
)

type Discoverer struct {
	cfg          *config.OCIConfig
	k8s          *k8s.K8S
	compartments []string
	region       string
	ctx          context.Context
	reg          prometheus.Registerer
	stop         chan struct{}

	mysqlClient         mysql.DbSystemClient
	mysqlReplicasClient mysql.ReplicasClient
	psqlClient          psql.PostgresqlClient
	cacheClient         redis.RedisClusterClient
	monitoringClient    monitoring.MonitoringClient
	loggingClient       logging.LoggingManagementClient
	logSearchClient     loggingsearch.LogSearchClient
	monitoring          *Monitoring

	errors      map[string]bool
	errorsLock  sync.RWMutex
	lastSummary string

	dbCollectors    map[string]*DBCollector
	cacheCollectors map[string]*CacheCollector

	endpointsLock  sync.RWMutex
	dbEndpoints    map[string]common.Endpoint
	dbReplicas     map[string][]string
	dbLogServices  map[string]string
	cacheEndpoints map[string]common.Endpoint

	serviceLogsLock sync.RWMutex
	serviceLogs     map[string]string

	logCounters func(name string) []logparser.LogCounter // the log pattern counters of the DB systems whose logs are read by their database collectors (MySQL), by display name
}

func (d *Discoverer) DBEndpoint(name string) (common.Endpoint, bool) {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	e, ok := d.dbEndpoints[name]
	return e, ok
}

func (d *Discoverer) DBLogService(name string) string {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.dbLogServices[name]
}

func (d *Discoverer) DBReplicas(primary string) []string {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.dbReplicas[primary]
}

func (d *Discoverer) CacheEndpoint(name string) (common.Endpoint, bool) {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	e, ok := d.cacheEndpoints[name]
	return e, ok
}

func NewDiscoverer(cfg *config.OCIConfig, k8s *k8s.K8S, reg prometheus.Registerer, logCounters func(name string) []logparser.LogCounter) (*Discoverer, error) {
	d := &Discoverer{
		cfg:             cfg,
		k8s:             k8s,
		ctx:             context.Background(),
		reg:             reg,
		stop:            make(chan struct{}),
		errors:          map[string]bool{},
		dbCollectors:    map[string]*DBCollector{},
		cacheCollectors: map[string]*CacheCollector{},
		logCounters:     logCounters,
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

func (d *Discoverer) Config() *config.OCIConfig {
	return d.cfg
}

func (d *Discoverer) init() error {
	cfg := d.cfg
	region := cfg.Region
	if region == "" {
		region, _ = d.k8s.GetNodeRegion(d.ctx)
	}
	if region != "" {
		region = string(ocicommon.StringToRegion(region)) // OKE labels the nodes with the region key (e.g. "iad")
	}
	var provider ocicommon.ConfigurationProvider
	var source string
	var err error
	switch {
	case cfg.UserID != "":
		source = "API key"
		provider = ocicommon.NewRawConfigurationProvider(cfg.TenancyID, cfg.UserID, region, cfg.Fingerprint, cfg.PrivateKey, nil)
	case os.Getenv("KUBERNETES_SERVICE_HOST") != "":
		source = "OKE workload identity"
		if os.Getenv(auth.ResourcePrincipalVersionEnvVar) == "" {
			_ = os.Setenv(auth.ResourcePrincipalVersionEnvVar, auth.ResourcePrincipalVersion2_2)
		}
		if os.Getenv(auth.ResourcePrincipalRegionEnvVar) == "" && region != "" {
			_ = os.Setenv(auth.ResourcePrincipalRegionEnvVar, region)
		}
		provider, err = auth.OkeWorkloadIdentityConfigurationProvider()
	default:
		source = "instance principal"
		provider, err = auth.InstancePrincipalConfigurationProvider()
	}
	if err != nil {
		return fmt.Errorf("OCI integration: failed to obtain credentials: %w", err)
	}
	if region == "" {
		region, _ = provider.Region()
	}
	if region == "" {
		return fmt.Errorf("OCI integration: the region is not configured and cannot be detected (set region)")
	}
	compartments := cfg.CompartmentIDs
	if len(compartments) == 0 { // the compartment of the cluster: a claim of the resource principal token
		if holder, ok := provider.(auth.ClaimHolder); ok {
			if claim, _ := holder.GetClaim(auth.CompartmentOCIDClaimKey); claim != nil {
				if compartment, ok := claim.(string); ok && compartment != "" {
					compartments = []string{compartment}
				}
			}
		}
	}
	if len(compartments) == 0 {
		return fmt.Errorf("OCI integration: the compartments are not configured and cannot be detected (set compartmentIds)")
	}
	if d.mysqlClient, err = mysql.NewDbSystemClientWithConfigurationProvider(provider); err != nil {
		return err
	}
	if d.mysqlReplicasClient, err = mysql.NewReplicasClientWithConfigurationProvider(provider); err != nil {
		return err
	}
	if d.psqlClient, err = psql.NewPostgresqlClientWithConfigurationProvider(provider); err != nil {
		return err
	}
	if d.cacheClient, err = redis.NewRedisClusterClientWithConfigurationProvider(provider); err != nil {
		return err
	}
	if d.monitoringClient, err = monitoring.NewMonitoringClientWithConfigurationProvider(provider); err != nil {
		return err
	}
	if d.loggingClient, err = logging.NewLoggingManagementClientWithConfigurationProvider(provider); err != nil {
		return err
	}
	if d.logSearchClient, err = loggingsearch.NewLogSearchClientWithConfigurationProvider(provider); err != nil {
		return err
	}
	for _, c := range []interface{ SetRegion(string) }{&d.mysqlClient, &d.mysqlReplicasClient, &d.psqlClient, &d.cacheClient, &d.monitoringClient, &d.loggingClient, &d.logSearchClient} {
		c.SetRegion(region)
	}
	d.compartments, d.region = compartments, region
	d.monitoring = NewMonitoring(d.monitoringClient, compartments)
	klog.Infof("OCI integration: compartments=%s, region=%s, credentials=%s", strings.Join(compartments, ","), region, source)
	return nil
}

func (d *Discoverer) apiContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(d.ctx, apiTimeout)
}

func retry() ocicommon.RequestMetadata {
	policy := ocicommon.DefaultRetryPolicyWithoutEventualConsistency()
	return ocicommon.RequestMetadata{RetryPolicy: &policy}
}

func (d *Discoverer) Stop() {
	d.stop <- struct{}{}
	for id, c := range d.dbCollectors {
		prometheus.WrapRegistererWith(dbLabels(id), d.reg).Unregister(c)
		c.Stop()
	}
	for id, c := range d.cacheCollectors {
		prometheus.WrapRegistererWith(cacheLabels(id), d.reg).Unregister(c)
		c.Stop()
	}
	d.reg.Unregister(d)
}

func (d *Discoverer) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("oci_discoverer", "", nil, nil)
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
	var svcErr ocicommon.ServiceError
	if errors.As(err, &svcErr) {
		msg = svcErr.GetMessage()
	}
	d.errorsLock.Lock()
	d.errors[msg] = true
	d.errorsLock.Unlock()
}

func (d *Discoverer) discover() {
	d.discoverDBSystems()
	d.discoverCaches()
	d.publishEndpoints()
	if len(d.dbCollectors) > 0 || len(d.cacheCollectors) > 0 {
		d.monitoring.refresh(d)
		d.discoverServiceLogs()
	}

	d.errorsLock.Lock()
	errs := maps.Keys(d.errors)
	d.errors = map[string]bool{}
	d.errorsLock.Unlock()
	summary := fmt.Sprintf("OCI discovery (compartments=%s, region=%s): %d DB systems, %d cache clusters", strings.Join(d.compartments, ","), d.region, len(d.dbCollectors), len(d.cacheCollectors))
	switch {
	case len(errs) > 0:
		klog.Errorf("%s, errors: %s", summary, strings.Join(errs, "; "))
	case summary != d.lastSummary:
		klog.Infoln(summary)
	}
	d.lastSummary = summary
}

func (d *Discoverer) publishEndpoints() {
	dbs := map[string]common.Endpoint{}
	replicas := map[string][]string{}
	logServices := map[string]string{}
	for _, c := range d.dbCollectors {
		if c.info.host != "" {
			dbs[c.info.name] = common.Endpoint{Host: c.info.host, Port: c.info.port}
		}
		if c.info.primary != "" {
			replicas[c.info.primary] = append(replicas[c.info.primary], c.info.name)
		}
		if c.logs == nil && *flags.CollectOCILogs {
			logServices[c.info.name] = dbLogService(c.info.id)
		}
	}
	caches := map[string]common.Endpoint{}
	for _, c := range d.cacheCollectors {
		if c.info.host != "" {
			caches[c.info.name] = common.Endpoint{Host: c.info.host, Port: c.info.port}
		}
	}
	d.endpointsLock.Lock()
	d.dbEndpoints = dbs
	d.dbReplicas = replicas
	d.dbLogServices = logServices
	d.cacheEndpoints = caches
	d.endpointsLock.Unlock()
}

func dbLogService(id string) string    { return "/oci/db/" + id }
func cacheLogService(id string) string { return "/oci/cache/" + id }

func dbLabels(id string) prometheus.Labels {
	return prometheus.Labels{"oci_db_id": id}
}

func cacheLabels(id string) prometheus.Labels {
	return prometheus.Labels{"oci_cache_id": id}
}

func str(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
