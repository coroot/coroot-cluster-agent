package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/coroot/coroot-cluster-agent/clickhouse"
	"github.com/coroot/coroot-cluster-agent/profiles"
	"github.com/gorilla/mux"
	"k8s.io/klog"
)

var (
	version = "unknown"
	name    = "CorootClusterAgent/" + version
)

func main() {
	klog.Infoln("version:", version)
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		klog.Exitln(err)
	}

	cl, err := clickhouse.NewClient(cfg.Clickhouse, name)
	if err != nil {
		klog.Exitln(err)
	}

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

	ps, err := profiles.NewProfiles(cfg.Profiles, cl, name)
	if err != nil {
		klog.Exitln(err)
	}
	router.HandleFunc("/profiles", ps.Handler).Methods(http.MethodPost)

	klog.Infoln("listening on", cfg.Listen)
	klog.Exitln(http.ListenAndServe(cfg.Listen, router))
}

func health(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("OK"))
}
