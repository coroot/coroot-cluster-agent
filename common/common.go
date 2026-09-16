package common

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"

	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

func TlsConfig() *tls.Config {
	cfg := &tls.Config{InsecureSkipVerify: *flags.InsecureSkipVerify}
	if *flags.CAFile != "" {
		ca, err := os.ReadFile(*flags.CAFile)
		if err != nil {
			klog.Fatalln(err)
			return cfg
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			klog.Warningln("failed to load system cert pool, starting with empty pool:", err)
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(ca) {
			klog.Fatalf("failed to parse CA from %s", *flags.CAFile)
		}
		cfg.RootCAs = pool
	}
	return cfg
}

type TLSCredentials struct {
	CA, Cert, Key string
}

func DatabaseTLSConfig(creds TLSCredentials, insecureSkipVerify bool) (*tls.Config, error) {
	cfg := &tls.Config{}
	if insecureSkipVerify {
		cfg.InsecureSkipVerify = true
	} else if creds.CA != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(creds.CA)) {
			return nil, fmt.Errorf("invalid CA certificate")
		}
		// Instances are scraped by IP, so we don't know which DNS name to expect in the
		// server certificate, and the built-in hostname verification would always fail.
		// crypto/tls has no "verify the chain but skip the hostname" mode, hence:
		// disable its verification and re-do the chain-vs-CA check in VerifyConnection.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("no server certificate")
			}
			opts := x509.VerifyOptions{Roots: pool, Intermediates: x509.NewCertPool()}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	}
	if creds.Cert != "" && creds.Key != "" {
		clientCert, err := tls.X509KeyPair([]byte(creds.Cert), []byte(creds.Key))
		if err != nil {
			return nil, fmt.Errorf("invalid client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{clientCert}
	}
	return cfg, nil
}

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

type Endpoint struct {
	Host string
	Port string
}
