package common

import (
	"net"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

func AuthHeaders(apiKey string) map[string]string {
	return map[string]string{
		"X-Api-Key": apiKey,
	}
}

func SetAuthHeaders(r *http.Request, apiKey string) {
	for k, v := range AuthHeaders(apiKey) {
		r.Header.Set(k, v)
	}
}

func Desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, labels, nil)
}

func Gauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func Counter(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labels...)
}

func SplitHostPort(addr string) (host string, port int, err error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}
