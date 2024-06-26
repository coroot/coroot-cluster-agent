package metrics

import (
	"fmt"
	"maps"
	"net"

	"github.com/coroot/coroot/model"
	"github.com/prometheus/client_golang/prometheus"
)

type Exporter struct {
	Config    ExporterConfig
	Collector prometheus.Collector
	Stop      func()
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("exporter", "", nil, nil)
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.Collector.Collect(ch)
}

type ExporterConfig model.ApplicationInstrumentation

func (c ExporterConfig) Address() string {
	return net.JoinHostPort(c.Host, c.Port)
}

func (c ExporterConfig) String() string {
	return fmt.Sprintf("%s/%s", c.Type, c.Address())
}

func (c ExporterConfig) Labels() prometheus.Labels {
	return prometheus.Labels{"address": c.Address()}
}

func (c ExporterConfig) Equal(other ExporterConfig) bool {
	return c.Type == other.Type &&
		c.Address() == other.Address() &&
		c.Credentials == other.Credentials &&
		maps.Equal(c.Params, other.Params)
}
