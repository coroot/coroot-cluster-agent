package main

import (
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/discovery"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/metrics"
	"github.com/coroot/coroot-cluster-agent/profiles"
	"github.com/gorilla/mux"
	"k8s.io/klog"
)

var (
	version = "unknown"
)

func main() {
	klog.Infoln("version:", version)

	router := mux.NewRouter()
	router.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t := time.Now()
			handler.ServeHTTP(w, r)
			klog.Infof("%s %s %d %s", r.Method, r.RequestURI, r.ContentLength, time.Since(t).Truncate(time.Millisecond))
		})
	})
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
	router.HandleFunc("/health", health).Methods(http.MethodGet)

	cu, err := config.NewUpdater()
	if err != nil {
		klog.Exitln(err)
	}

	ms, err := metrics.NewMetrics()
	if err != nil {
		klog.Exitln(err)
	}
	if ms != nil {
		cu.Subscribe(ms)
		router.Handle("/metrics", ms.HttpHandler())
	}

	cu.Start()
	defer cu.Stop()

	k8s, err := discovery.NewK8S()
	if err != nil {
		klog.Exitln(err)
	}

	if k8s != nil {
		ps, err := profiles.NewProfiles()
		if err != nil {
			klog.Exitln(err)
		}
		if ps != nil {
			k8s.Subscribe(ps)
			ps.Start()
		}
		k8s.Start()
		defer k8s.Stop()
	}

	klog.Infoln("listening on", *flags.ListenAddress)
	klog.Exitln(http.ListenAndServe(*flags.ListenAddress, router))
}

func health(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("OK"))
}
