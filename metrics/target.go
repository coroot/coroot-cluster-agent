package metrics

import (
	"cmp"
	"fmt"
	"maps"
	"net"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics/exporter"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
)

type TargetType = exporter.Type

const (
	TargetTypePostgres  = exporter.TypePostgres
	TargetTypeMysql     = exporter.TypeMysql
	TargetTypeRedis     = exporter.TypeRedis
	TargetTypeMongodb   = exporter.TypeMongodb
	TargetTypeMemcached = exporter.TypeMemcached
)

type Credentials = exporter.Credentials

type CredentialsSecret struct {
	Namespace   string
	Name        string
	UsernameKey string
	PasswordKey string
}

type TLSSecret struct {
	Namespace string
	Name      string
	CAKey     string
	CertKey   string
	KeyKey    string
}

type Target struct {
	Type              TargetType
	Addr              string
	Credentials       Credentials
	CredentialsSecret CredentialsSecret
	TLSSecret         TLSSecret
	Params            map[string]string

	Description                  string
	LogService                   string // the service the logs read from the database server (e.g. the MySQL error log) are forwarded as, if any
	DiscoveredFromPodAnnotations bool

	coll     prometheus.Collector
	collLock sync.Mutex
	stop     func()
	logger   logger.Logger
}

func (t *Target) collector() prometheus.Collector {
	t.collLock.Lock()
	defer t.collLock.Unlock()
	return t.coll
}

func (t *Target) setCollector(c prometheus.Collector) {
	t.collLock.Lock()
	defer t.collLock.Unlock()
	t.coll = c
}

func (t *Target) Equal(other *Target) bool {
	return t.Type == other.Type &&
		t.Addr == other.Addr &&
		t.Credentials == other.Credentials &&
		t.CredentialsSecret == other.CredentialsSecret &&
		t.TLSSecret == other.TLSSecret &&
		t.LogService == other.LogService &&
		maps.Equal(t.Params, other.Params)
}

func (t *Target) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("exporter", "", nil, nil)
}

func (t *Target) Collect(ch chan<- prometheus.Metric) {
	if coll := t.collector(); coll != nil {
		start := time.Now()
		coll.Collect(ch)
		t.logger.Info("metrics collection completed in", time.Since(start).Truncate(time.Millisecond))
	}
}

func (t *Target) Labels() prometheus.Labels {
	return prometheus.Labels{"address": t.Addr}
}

func (t *Target) String() string {
	return fmt.Sprintf("%s://%s (%s)", t.Type, t.Addr, t.Description)
}

func (t *Target) IsExporterStarted() bool {
	return t.collector() != nil
}

func (t *Target) StartExporter(reg *prometheus.Registry, credentials Credentials, tlsCreds common.TLSCredentials, opts exporter.Options) error {
	e, err := exporter.New(exporter.Config{
		Type:        t.Type,
		Addr:        t.Addr,
		Credentials: credentials,
		TLS:         tlsCreds,
		Params:      t.Params,
		Description: t.Description,
		LogService:  t.LogService,
	}, opts, t.logger)
	if err != nil {
		return err
	}
	t.setCollector(e)
	t.stop = e.Close
	return prometheus.WrapRegistererWith(t.Labels(), reg).Register(t)
}

func (t *Target) StopExporter(reg *prometheus.Registry) {
	if t.collector() != nil {
		prometheus.WrapRegistererWith(t.Labels(), reg).Unregister(t)
		if t.stop != nil {
			t.stop()
		}
		t.setCollector(nil)
	}
}

func TargetFromConfig(i config.ApplicationInstrumentation) *Target {
	t := &Target{
		Type: TargetType(i.Type),
		Addr: net.JoinHostPort(i.Host, i.Port),
		Credentials: Credentials{
			Username: i.Credentials.Username,
			Password: i.Credentials.Password,
		},
		Params:      i.Params,
		Description: i.Instance,
	}
	t.logger = logger.NewKlog(t.String())
	return t
}

func TargetFromPod(pod *k8s.Pod) *Target {
	if pod == nil || pod.Annotations == nil || pod.IP == "" {
		return nil
	}

	var t *Target

	if pod.Annotations["coroot.com/postgres-scrape"] == "true" {
		t = &Target{
			Type: TargetTypePostgres,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/postgres-scrape-port"], "5432")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/postgres-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/postgres-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/postgres-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/postgres-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/postgres-scrape-credentials-secret-password-key"],
			},
			TLSSecret: tlsSecretFromPod(pod, "postgres"),
			Params: map[string]string{
				"sslmode": pod.Annotations["coroot.com/postgres-scrape-param-sslmode"],
			},
		}
	}

	if pod.Annotations["coroot.com/mysql-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMysql,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/mysql-scrape-port"], "3306")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/mysql-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/mysql-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/mysql-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/mysql-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/mysql-scrape-credentials-secret-password-key"],
			},
			TLSSecret: tlsSecretFromPod(pod, "mysql"),
			Params: map[string]string{
				"tls": pod.Annotations["coroot.com/mysql-scrape-param-tls"],
			},
		}
	}

	if pod.Annotations["coroot.com/redis-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeRedis,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/redis-scrape-port"], "6379")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/redis-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/redis-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/redis-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/redis-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/redis-scrape-credentials-secret-password-key"],
			},
		}
	}

	if pod.Annotations["coroot.com/mongodb-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMongodb,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/mongodb-scrape-port"], "27017")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/mongodb-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/mongodb-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-password-key"],
			},
			TLSSecret: tlsSecretFromPod(pod, "mongodb"),
			Params: map[string]string{
				"tls":        pod.Annotations["coroot.com/mongodb-scrape-param-tls"],
				"authSource": pod.Annotations["coroot.com/mongodb-scrape-param-auth-source"],
			},
		}
	}

	if pod.Annotations["coroot.com/memcached-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMemcached,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/memcached-scrape-port"], "11211")),
		}
	}

	if t != nil {
		t.DiscoveredFromPodAnnotations = true
		t.Description = fmt.Sprintf("ns=%s, pod=%s, node=%s", pod.Id.Namespace, pod.Id.Name, pod.Id.NodeName)
		t.logger = logger.NewKlog(t.String())
	}

	return t
}

func tlsSecretFromPod(pod *k8s.Pod, targetType string) TLSSecret {
	name := pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-name"]
	if name == "" {
		return TLSSecret{}
	}
	return TLSSecret{
		Namespace: pod.Id.Namespace,
		Name:      name,
		CAKey:     pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-ca-key"],
		CertKey:   pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-cert-key"],
		KeyKey:    pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-key-key"],
	}
}
