package flags

import (
	"github.com/alecthomas/kingpin/v2"
	"k8s.io/klog"
)

var (
	ListenAddress = kingpin.Flag("listen", "Listen address - ip:port or :port").Default("127.0.0.1:10301").Envar("LISTEN").String()
	CorootURL     = kingpin.Flag("coroot-url", "Coroot URL").Envar("COROOT_URL").URL()
	APIKey        = kingpin.Flag("api-key", "Coroot API key").Envar("API_KEY").String()

	ConfigUpdateInterval = kingpin.Flag("config-update-interval", "").Envar("CONFIG_UPDATE_INTERVAL").Default("60s").Duration()
	ConfigUpdateTimeout  = kingpin.Flag("config-update-timeout", "").Envar("CONFIG_UPDATE_TIMEOUT").Default("10s").Duration()

	MetricsScrapeInterval = kingpin.Flag("metrics-scrape-interval", "").Envar("METRICS_SCRAPE_INTERVAL").Duration()
	MetricsScrapeTimeout  = kingpin.Flag("metrics-scrape-timeout", "").Envar("METRICS_SCRAPE_TIMEOUT").Default("10s").Duration()
	MetricsWALDir         = kingpin.Flag("metrics-wal-dir", "").Envar("METRICS_WAL_DIR").Default("/tmp").String()

	ProfilesScrapeInterval = kingpin.Flag("profiles-scrape-interval", "").Envar("PROFILES_SCRAPE_INTERVAL").Default("60s").Duration()
	ProfilesScrapeTimeout  = kingpin.Flag("profiles-scrape-timeout", "").Envar("PROFILES_SCRAPE_TIMEOUT").Default("10s").Duration()

	KubeStateMetricsListenAddress = kingpin.Flag("kube-state-metrics-listen-address", "").Default("127.0.0.1:10303").Envar("KUBE_STATE_METRICS_LISTEN_ADDRESS").String()

	InsecureSkipVerify = kingpin.Flag("insecure-skip-verify", "whether to skip verifying the certificate or not").Envar("INSECURE_SKIP_VERIFY").Default("false").Bool()
)

func init() {
	kingpin.HelpFlag.Short('h').Hidden()
	kingpin.Parse()

	if *CorootURL == nil {
		klog.Exitln("coroot-url is required")
	}
}
