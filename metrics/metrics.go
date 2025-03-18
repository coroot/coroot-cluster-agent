package metrics

import (
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics/aws"
	"github.com/coroot/logger"
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

	aws *aws.Discoverer

	k8s *k8s.K8S

	k8sPodEvents <-chan k8s.PodEvent
}

func NewMetrics(k8s *k8s.K8S) *Metrics {
	if *flags.MetricsScrapeInterval == 0 {
		klog.Infoln("scrape interval is not set, disabling the scraper")
		return nil
	}

	ms := &Metrics{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/metrics"),
		apiKey:         *flags.APIKey,
		listenAddr:     *flags.ListenAddress,
		ksmAddr:        *flags.KubeStateMetricsAddress,
		scrapeInterval: *flags.MetricsScrapeInterval,
		scrapeTimeout:  *flags.MetricsScrapeTimeout,
		walDir:         *flags.MetricsWALDir,
		reg:            prometheus.NewRegistry(),
		targets:        map[string]*Target{},
		k8s:            k8s,
	}

	klog.Infof("endpoint: %s, scrape interval: %s", ms.endpoint, ms.scrapeInterval)

	return ms
}

func (ms *Metrics) Start() error {
	go ms.discoverFromPods()
	go ms.startExporters()
	return ms.runScraper()
}

func (ms *Metrics) ListenConfigUpdates(updates <-chan config.Config) {
	go func() {
		for cfg := range updates {
			ms.updateAWS(cfg.AWSConfig)
			ms.discoverFromConfig(cfg.ApplicationInstrumentation)
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
	defer ms.targetsLock.Unlock()
	t := ms.targets[target.Addr]
	if t != nil {
		t.StopExporter(ms.reg)
	}
	delete(ms.targets, target.Addr)
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
			klog.Errorln("cannot retrieve secrets: access forbidden; update Coroot Operator to proceed")
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
			if err := t.ValidateCredentials(credentials); err != nil {
				t.logger.Errorf("failed to start exporter: %s", err)
				continue
			}
			if err := t.StartExporter(ms.reg, credentials, ms.scrapeInterval, ms.scrapeTimeout); err != nil {
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
		d, err := aws.NewDiscoverer(cfg, ms.reg)
		if err != nil {
			klog.Errorln(err)
		} else {
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

func (ms *Metrics) discoverFromPods() {
	for e := range ms.k8sPodEvents {
		switch e.Type {
		case k8s.PodEventTypeAdd, k8s.PodEventTypeChange:
			target := TargetFromPod(e.Pod)
			if target == nil {
				if t := TargetFromPod(e.Old); t != nil {
					ms.delTarget(t)
				}
				continue
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

func (ms *Metrics) discoverFromConfig(instrumentation []config.ApplicationInstrumentation) {
	actual := map[string]bool{}
	for _, i := range instrumentation {
		target := TargetFromConfig(i)
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
	targets := maps.Values(ms.targets)
	ms.targetsLock.Unlock()
	for _, t := range targets {
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
