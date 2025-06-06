package main

import (
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/k8s"
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

	config, err := config.NewUpdater()
	if err != nil {
		klog.Exitln(err)
	}

	k8s, err := k8s.NewK8S()
	if err != nil {
		klog.Exitln(err)
	}

	ms, err := metrics.NewMetrics(k8s)
	if err != nil {
		klog.Exitln(err)
	}
	if ms != nil {
		config.SubscribeForUpdates(ms)
		k8s.SubscribeForPodEvents(ms)
		router.Handle("/metrics", ms.HttpHandler())
		err = ms.Start()
		if err != nil {
			klog.Exitln(err)
		}
		defer ms.Stop()
	}

	if ps := profiles.NewProfiles(); ps != nil {
		k8s.SubscribeForPodEvents(ps)
		ps.Start()
	}

	config.Start()
	defer config.Stop()

	k8s.Start()
	defer k8s.Stop()

	klog.Infoln("listening on", *flags.ListenAddress)
	klog.Exitln(http.ListenAndServe(*flags.ListenAddress, router))
}

func health(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("OK"))
}
