package metrics

import (
	"context"
	"errors"
	"net"
	"sort"

	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics/aws"
	"github.com/coroot/coroot-cluster-agent/metrics/gcp"
	"github.com/coroot/coroot-cluster-agent/metrics/ksm"
	"github.com/coroot/coroot-cluster-agent/metrics/mysql"
	"github.com/coroot/coroot-cluster-agent/metrics/oci"
	"github.com/coroot/coroot-cluster-agent/schema/emitter"
	"github.com/coroot/logger"
	"github.com/coroot/logparser"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/maps"
	"k8s.io/klog"
)

const (
	ExportersRecheckInterval = 10 * time.Second
)

type Metrics struct {
	endpoint       *url.URL
	apiKey         string
	listenAddr     string
	ksmAddr        string
	scrapeInterval time.Duration
	scrapeTimeout  time.Duration
	walDir         string

	reg *prometheus.Registry

	targets     map[string]*Target
	targetsLock sync.Mutex

	aws          *aws.Discoverer
	gcp          *gcp.Discoverer
	oci          *oci.Discoverer
	cloudErrors  map[string]string
	k8s          *k8s.K8S
	static       *config.Static
	k8sPodEvents <-chan k8s.PodEvent
	ksm          *ksm.KSM

	changeEmitter *emitter.ChangeEmitter
}

func NewMetrics(k8s *k8s.K8S, static *config.Static) (*Metrics, error) {
	if *flags.MetricsScrapeInterval == 0 {
		klog.Infoln("scrape interval is not set, disabling the scraper")
		return nil, nil
	}

	ms := &Metrics{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/metrics"),
		apiKey:         *flags.APIKey,
		listenAddr:     *flags.ListenAddress,
		scrapeInterval: *flags.MetricsScrapeInterval,
		scrapeTimeout:  *flags.MetricsScrapeTimeout,
		walDir:         *flags.MetricsWALDir,
		reg:            prometheus.NewRegistry(),
		targets:        map[string]*Target{},
		k8s:            k8s,
		static:         static,
	}

	var err error
	ksmAddr := *flags.KubeStateMetricsListenAddress
	if ksmAddr != "" && k8s != nil {
		ms.ksmAddr = ksmAddr
		ms.ksm, err = ksm.NewKSM(ksmAddr, *flags.KubeStateMetricsMinAge)
		if err != nil {
			return nil, err
		}
	}

	if *flags.TrackDatabaseChanges {
		ms.changeEmitter, err = emitter.NewChangeEmitter()
		if err != nil {
			return nil, err
		}
	}

	klog.Infof("endpoint: %s, scrape interval: %s", ms.endpoint, ms.scrapeInterval)

	return ms, nil
}

func (ms *Metrics) Start() error {
	go ms.discoverFromPods()
	go ms.startExporters()
	if ms.ksm != nil {
		go ms.ksm.Start()
	}
	return ms.runScraper()
}

func (ms *Metrics) Stop() {
	if ms.ksm != nil {
		ms.ksm.Stop()
	}
}

func (ms *Metrics) ListenConfigUpdates(updates <-chan config.Config) {
	go func() {
		for cfg := range updates {
			var targets []*Target
			for _, i := range cfg.ApplicationInstrumentation {
				targets = append(targets, TargetFromConfig(i))
			}
			if ms.static != nil && ms.static.AWS != nil {
				cfg.AWSConfig = ms.static.AWS
			}
			ms.updateAWS(cfg.AWSConfig)
			if ms.static != nil {
				ms.updateGCP(ms.static.GCP)
				ms.updateOCI(ms.static.OCI)
				targets = append(targets, ms.resolveDatabases(ms.static.Databases)...)
			}
			ms.discoverFromConfig(targets)
		}
	}()
}

func (ms *Metrics) ListenPodEvents(events <-chan k8s.PodEvent) {
	ms.k8sPodEvents = events
}

func (ms *Metrics) HttpHandler() http.Handler {
	return promhttp.HandlerFor(ms.reg, promhttp.HandlerOpts{})
}

func (ms *Metrics) addTarget(target *Target) {
	klog.Infof("new target: %s", target)
	ms.targetsLock.Lock()
	defer ms.targetsLock.Unlock()
	ms.targets[target.Addr] = target
}

func (ms *Metrics) delTarget(target *Target) {
	klog.Infof("removing target: %s", target)
	ms.targetsLock.Lock()
	t := ms.targets[target.Addr]
	delete(ms.targets, target.Addr)
	ms.targetsLock.Unlock()
	if t != nil {
		t.StopExporter(ms.reg)
	}
}

func (ms *Metrics) startExporters() {
	for range time.Tick(ExportersRecheckInterval) {
		ms.targetsLock.Lock()
		var targets []*Target
		for _, t := range ms.targets {
			if !t.IsExporterStarted() {
				targets = append(targets, t)
			}
		}
		ms.targetsLock.Unlock()

		if len(targets) == 0 {
			continue
		}

		type secretId struct {
			namespace, name string
		}
		id2Keys := map[secretId][]string{}
		for _, t := range targets {
			if s := t.CredentialsSecret; s.Name != "" {
				var keys []string
				if s.UsernameKey != "" {
					keys = append(keys, s.UsernameKey)
				}
				if s.PasswordKey != "" {
					keys = append(keys, s.PasswordKey)
				}
				if len(keys) > 0 {
					id2Keys[secretId{namespace: s.Namespace, name: s.Name}] = keys
				}
			}
			if s := t.TLSSecret; s.Name != "" {
				id := secretId{namespace: s.Namespace, name: s.Name}
				for _, key := range []string{s.CAKey, s.CertKey, s.KeyKey} {
					if key != "" {
						id2Keys[id] = append(id2Keys[id], key)
					}
				}
			}
		}
		var err error
		secrets := map[secretId]map[string]string{}
		var isSecretsForbidden bool
		for id, keys := range id2Keys {
			secrets[id], err = ms.k8s.GetSecret(id.namespace, id.name, keys...)
			if err != nil {
				if errors.Is(err, k8s.ErrForbidden) {
					isSecretsForbidden = true
					break
				}
				if errors.Is(err, k8s.ErrNotFound) {
					continue
				}
				klog.Errorf("failed to get secret '%s': %s", id.name, err)
				continue
			}
		}

		if isSecretsForbidden {
			klog.Errorln("Cannot retrieve secrets: access forbidden. Update Coroot Operator to proceed.")
		}

		for _, t := range targets {
			credentials := t.Credentials
			if s := t.CredentialsSecret; s.Name != "" {
				kv := secrets[secretId{namespace: s.Namespace, name: s.Name}]
				switch {
				case isSecretsForbidden:
					t.logger.Errorf("failed to start exporter: secret '%s' forbidden", s.Name)
					continue
				case kv == nil:
					t.logger.Errorf("failed to start exporter: secret '%s' not found", s.Name)
					continue
				default:
					if username := kv[s.UsernameKey]; username != "" {
						credentials.Username = username
					}
					if password := kv[s.PasswordKey]; password != "" {
						credentials.Password = password
					}
				}
			}
			var tlsCreds common.TLSCredentials
			if s := t.TLSSecret; s.Name != "" {
				kv := secrets[secretId{namespace: s.Namespace, name: s.Name}]
				switch {
				case isSecretsForbidden:
					t.logger.Errorf("failed to start exporter: secret '%s' forbidden", s.Name)
					continue
				case kv == nil:
					t.logger.Errorf("failed to start exporter: TLS secret '%s' not found", s.Name)
					continue
				default:
					if (s.CertKey != "") != (s.KeyKey != "") {
						t.logger.Errorf("failed to start exporter: TLS secret '%s': the cert and key keys must be set together", s.Name)
						continue
					}
					if s.CAKey == "" && s.CertKey == "" {
						t.logger.Errorf("failed to start exporter: TLS secret '%s': no keys specified (set the ca-key and/or cert-key/key-key annotations)", s.Name)
						continue
					}
					if s.CAKey != "" {
						tlsCreds.CA = kv[s.CAKey]
					}
					if s.CertKey != "" {
						tlsCreds.Cert = kv[s.CertKey]
						tlsCreds.Key = kv[s.KeyKey]
					}
					if tlsCreds.CA == "" && (tlsCreds.Cert == "" || tlsCreds.Key == "") {
						t.logger.Errorf("failed to start exporter: TLS secret '%s' does not contain the specified keys", s.Name)
						continue
					}
				}
			}
			if err := t.StartExporter(ms.reg, credentials, tlsCreds, ms.scrapeInterval, ms.scrapeTimeout, ms.changeEmitter, *flags.MaxTablesPerDatabase, *flags.TrackDatabaseSizes, *flags.TrackDatabaseBloat, *flags.ExcludeDatabases); err != nil {
				t.logger.Errorf("failed to start exporter: %s", err)
				continue
			}
		}
	}
}

func (ms *Metrics) updateAWS(cfg *config.AWSConfig) {
	switch {
	case cfg == nil && ms.aws == nil:
	case cfg == nil && ms.aws != nil:
		ms.aws.Stop()
		ms.aws = nil
	case cfg != nil && ms.aws == nil:
		d, err := aws.NewDiscoverer(cfg, ms.k8s, ms.reg)
		if ms.logCloudError("aws", err) {
			ms.aws = d
		}
	default:
		err := ms.aws.Update(cfg)
		if err != nil {
			klog.Errorln(err)
			ms.aws.Stop()
			ms.aws = nil
		}
	}
}

func (ms *Metrics) updateGCP(cfg *config.GCPConfig) {
	if ms.gcp != nil && (cfg == nil || !ms.gcp.Config().Equal(cfg)) { // recreated on a change: rare, and simpler than reconfiguring
		ms.gcp.Stop()
		ms.gcp = nil
	}
	if cfg != nil && ms.gcp == nil {
		if d, err := gcp.NewDiscoverer(cfg, ms.k8s, ms.reg); ms.logCloudError("gcp", err) {
			ms.gcp = d
		}
	}
}

func (ms *Metrics) updateOCI(cfg *config.OCIConfig) {
	if ms.oci != nil && (cfg == nil || !ms.oci.Config().Equal(cfg)) {
		ms.oci.Stop()
		ms.oci = nil
	}
	if cfg != nil && ms.oci == nil {
		if d, err := oci.NewDiscoverer(cfg, ms.k8s, ms.reg, ms.ociLogCounters); ms.logCloudError("oci", err) {
			ms.oci = d
		}
	}
}

func (ms *Metrics) logCloudError(cloud string, err error) bool {
	if ms.cloudErrors == nil {
		ms.cloudErrors = map[string]string{}
	}
	if err == nil {
		delete(ms.cloudErrors, cloud)
		return true
	}
	if ms.cloudErrors[cloud] != err.Error() {
		klog.Errorln(err)
		ms.cloudErrors[cloud] = err.Error()
	}
	return false
}

func (ms *Metrics) discoverFromPods() {
	for e := range ms.k8sPodEvents {
		switch e.Type {
		case k8s.PodEventTypeAdd, k8s.PodEventTypeChange:
			target := TargetFromPod(e.Pod)
			old := TargetFromPod(e.Old)
			if target == nil {
				if old != nil {
					ms.delTarget(old)
				}
				continue
			}
			if old != nil && old.Addr != target.Addr { // e.g. the pod IP has changed
				ms.delTarget(old)
			}
			ms.targetsLock.Lock()
			t := ms.targets[target.Addr]
			ms.targetsLock.Unlock()
			switch {
			case t == nil:
				ms.addTarget(target)
			case t.Equal(target):
				continue
			default:
				ms.delTarget(t)
				ms.addTarget(target)
			}
		case k8s.PodEventTypeDelete:
			target := TargetFromPod(e.Pod)
			if target == nil {
				continue
			}
			ms.delTarget(target)
		}
	}
}

func (ms *Metrics) resolveDatabases(databases []config.Database) []*Target {
	var res []*Target
	for _, d := range databases {
		var endpoints []common.Endpoint
		var description string
		switch {
		case d.RDS != "":
			description = "rds:" + d.RDS
			if ms.aws == nil {
				klog.Warningf("%s: the AWS integration is not configured, skipping", description)
				continue
			}
			e, ok := ms.aws.RDSEndpoint(d.RDS)
			if !ok {
				klog.Warningf("%s: the RDS instance is not discovered (yet), skipping", description)
				continue
			}
			endpoints = []common.Endpoint{e}
			for _, replica := range ms.aws.RDSReplicas(d.RDS) {
				if e, ok := ms.aws.RDSEndpoint(replica); ok {
					res = append(res, ms.databaseTargets(d, "rds:"+replica, []common.Endpoint{e})...)
				}
			}
		case d.Elasticache != "":
			description = "elasticache:" + d.Elasticache
			if ms.aws == nil {
				klog.Warningf("%s: the AWS integration is not configured, skipping", description)
				continue
			}
			endpoints = ms.aws.ElastiCacheEndpoints(d.Elasticache)
			if len(endpoints) == 0 {
				klog.Warningf("%s: the ElastiCache cluster is not discovered (yet), skipping", description)
				continue
			}
		case d.CloudSQL != "":
			description = "cloudsql:" + d.CloudSQL
			if ms.gcp == nil {
				klog.Warningf("%s: the GCP integration is not configured, skipping", description)
				continue
			}
			e, ok := ms.gcp.CloudSQLEndpoint(d.CloudSQL)
			if !ok {
				klog.Warningf("%s: the Cloud SQL instance is not discovered (yet), skipping", description)
				continue
			}
			endpoints = []common.Endpoint{e}
			for _, replica := range ms.gcp.CloudSQLReplicas(d.CloudSQL) {
				if e, ok := ms.gcp.CloudSQLEndpoint(replica); ok {
					res = append(res, ms.databaseTargets(d, "cloudsql:"+replica, []common.Endpoint{e})...)
				}
			}
		case d.Memorystore != "":
			description = "memorystore:" + d.Memorystore
			if ms.gcp == nil {
				klog.Warningf("%s: the GCP integration is not configured, skipping", description)
				continue
			}
			endpoints = ms.gcp.MemorystoreEndpoints(d.Memorystore)
			if len(endpoints) == 0 {
				klog.Warningf("%s: the Memorystore instance is not discovered (yet), skipping", description)
				continue
			}
		case d.OCIDB != "":
			description = "ocidb:" + d.OCIDB
			if ms.oci == nil {
				klog.Warningf("%s: the OCI integration is not configured, skipping", description)
				continue
			}
			if _, ok := ms.oci.DBEndpoint(d.OCIDB); !ok {
				klog.Warningf("%s: the DB system is not discovered (yet), skipping", description)
				continue
			}
			for _, name := range append([]string{d.OCIDB}, ms.oci.DBReplicas(d.OCIDB)...) { // read replicas and standby instances share the credentials
				if e, ok := ms.oci.DBEndpoint(name); ok {
					targets := ms.databaseTargets(d, "ocidb:"+name, []common.Endpoint{e})
					for _, t := range targets {
						t.LogService = ms.oci.DBLogService(name)
					}
					res = append(res, targets...)
				}
			}
			continue
		case d.OCICache != "":
			description = "ocicache:" + d.OCICache
			if ms.oci == nil {
				klog.Warningf("%s: the OCI integration is not configured, skipping", description)
				continue
			}
			e, ok := ms.oci.CacheEndpoint(d.OCICache)
			if !ok {
				klog.Warningf("%s: the cache cluster is not discovered (yet), skipping", description)
				continue
			}
			endpoints = []common.Endpoint{e}
		default:
			description = d.Host
			endpoints = []common.Endpoint{{Host: d.Host, Port: d.Port}}
		}
		res = append(res, ms.databaseTargets(d, description, endpoints)...)
	}
	return res
}

func (ms *Metrics) ociLogCounters(name string) []logparser.LogCounter {
	ms.targetsLock.Lock()
	defer ms.targetsLock.Unlock()
	for _, t := range ms.targets {
		if t.Description != "ocidb:"+name {
			continue
		}
		if c, ok := t.collector().(*mysql.Collector); ok {
			return c.ErrorLogCounters()
		}
	}
	return nil
}

func (ms *Metrics) databaseTargets(d config.Database, description string, endpoints []common.Endpoint) []*Target {
	var res []*Target
	for _, e := range endpoints {
		port := e.Port
		if d.Port != "" {
			port = d.Port
		}
		for _, ip := range resolveHost(e.Host) {
			res = append(res, TargetFromConfig(config.ApplicationInstrumentation{
				Type:        d.Type,
				Host:        ip,
				Port:        port,
				Credentials: d.Credentials,
				Params:      d.Params,
				Instance:    description,
			}))
		}
	}
	return res
}

func resolveHost(host string) []string {
	if ip := net.ParseIP(host); ip != nil {
		return []string{host}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		klog.Warningf("failed to resolve %s: %s", host, err)
		return nil
	}
	ips := make([]string, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.IP.String())
	}
	sort.Strings(ips)
	return ips
}

func (ms *Metrics) discoverFromConfig(targets []*Target) {
	actual := map[string]bool{}
	for _, target := range targets {
		actual[target.Addr] = true
		ms.targetsLock.Lock()
		t := ms.targets[target.Addr]
		ms.targetsLock.Unlock()
		switch {
		case t == nil:
			ms.addTarget(target)
		case t.DiscoveredFromPodAnnotations:
			continue
		case t.Equal(target):
			continue
		default:
			ms.delTarget(t)
			ms.addTarget(target)
		}
	}
	ms.targetsLock.Lock()
	existing := maps.Values(ms.targets)
	ms.targetsLock.Unlock()
	for _, t := range existing {
		if !actual[t.Addr] && !t.DiscoveredFromPodAnnotations {
			ms.delTarget(t)
		}
	}
}

type promLogger struct {
	l logger.Logger
}

func (l *promLogger) Log(keyvals ...interface{}) error {
	l.l.Info(keyvals...)
	return nil
}
