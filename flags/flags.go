package flags

import (
	"github.com/alecthomas/kingpin/v2"
	"k8s.io/klog"
)

var (
	ListenAddress = kingpin.Flag("listen", "Listen address - ip:port or :port").Default("127.0.0.1:10301").Envar("LISTEN").String()
	CorootURL     = kingpin.Flag("coroot-url", "Coroot URL").Envar("COROOT_URL").URL()
	APIKey        = kingpin.Flag("api-key", "Coroot API key").Envar("API_KEY").String()

	ConfigUpdateInterval = kingpin.Flag("config-update-interval", "Interval between configuration updates from Coroot").Envar("CONFIG_UPDATE_INTERVAL").Default("60s").Duration()
	ConfigUpdateTimeout  = kingpin.Flag("config-update-timeout", "Timeout for configuration update requests").Envar("CONFIG_UPDATE_TIMEOUT").Default("10s").Duration()

	MetricsScrapeInterval = kingpin.Flag("metrics-scrape-interval", "Interval between metrics scrapes").Envar("METRICS_SCRAPE_INTERVAL").Duration()
	MetricsScrapeTimeout  = kingpin.Flag("metrics-scrape-timeout", "Timeout for metrics scrape requests").Envar("METRICS_SCRAPE_TIMEOUT").Default("10s").Duration()
	MetricsWALDir         = kingpin.Flag("metrics-wal-dir", "Directory for the metrics write-ahead log").Envar("METRICS_WAL_DIR").Default("/tmp").String()

	ProfilesScrapeInterval = kingpin.Flag("profiles-scrape-interval", "Interval between profiling scrapes").Envar("PROFILES_SCRAPE_INTERVAL").Default("60s").Duration()
	ProfilesScrapeTimeout  = kingpin.Flag("profiles-scrape-timeout", "Timeout for profiling scrape requests").Envar("PROFILES_SCRAPE_TIMEOUT").Default("10s").Duration()

	KubeStateMetricsListenAddress = kingpin.Flag("kube-state-metrics-listen-address", "Listen address for the kube-state-metrics endpoint").Default("127.0.0.1:10303").Envar("KUBE_STATE_METRICS_LISTEN_ADDRESS").String()

	InsecureSkipVerify = kingpin.Flag("insecure-skip-verify", "Skip TLS certificate verification").Envar("INSECURE_SKIP_VERIFY").Default("false").Bool()

	CollectKubernetesEvents = kingpin.Flag("collect-kubernetes-events", "Collect and forward Kubernetes events").Envar("COLLECT_KUBERNETES_EVENTS").Default("true").Bool()

	TrackDatabaseChanges = kingpin.Flag("track-database-changes", "Track schema and settings changes in databases").Envar("TRACK_DATABASE_CHANGES").Default("true").Bool()
	TrackDatabaseSizes   = kingpin.Flag("track-database-sizes", "Collect per-database and per-table size metrics").Envar("TRACK_DATABASE_SIZES").Default("true").Bool()
	MaxTablesPerDatabase = kingpin.Flag("max-tables-per-database", "Skip databases with more tables than this limit").Envar("MAX_TABLES_PER_DATABASE").Default("1000").Int()
	ExcludeDatabases     = kingpin.Flag("exclude-databases", "Databases to exclude from schema and size tracking").Envar("EXCLUDE_DATABASES").Default("postgres").Strings()
)

func init() {
	kingpin.HelpFlag.Short('h').Hidden()
	kingpin.Parse()

	if *CorootURL == nil {
		klog.Exitln("coroot-url is required")
	}
}
