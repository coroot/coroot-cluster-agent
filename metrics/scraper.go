package metrics

import (
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	promCommon "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/agent"
)

const (
	RemoteWriteTimeout  = 30 * time.Second
	RemoteFlushDeadline = time.Minute
	jobName             = "coroot-cluster-agent"
)

func (ms *Metrics) runScraper() error {
	logger := level.NewFilter(Logger{}, level.AllowInfo())
	cfg := config.DefaultConfig
	cfg.GlobalConfig.ScrapeInterval = model.Duration(ms.scrapeInterval)
	cfg.GlobalConfig.ScrapeTimeout = model.Duration(ms.scrapeTimeout)
	cfg.RemoteWriteConfigs = append(cfg.RemoteWriteConfigs,
		&config.RemoteWriteConfig{
			URL:           &promCommon.URL{URL: ms.endpoint},
			Headers:       common.AuthHeaders(ms.apiKey),
			RemoteTimeout: model.Duration(RemoteWriteTimeout),
			QueueConfig:   config.DefaultQueueConfig,
		},
	)
	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, &config.ScrapeConfig{
		JobName:                 jobName,
		HonorLabels:             true,
		ScrapeClassicHistograms: true,
		MetricsPath:             "/metrics",
		Scheme:                  "http",
		EnableCompression:       false,
	})
	localStorage := &readyStorage{stats: tsdb.NewDBStats()}
	scraper := &readyScrapeManager{}
	remoteStorage := remote.NewStorage(logger, prometheus.DefaultRegisterer, localStorage.StartTime, ms.walDir, RemoteFlushDeadline, scraper)
	fanoutStorage := storage.NewFanout(logger, localStorage, remoteStorage)

	if err := remoteStorage.ApplyConfig(&cfg); err != nil {
		return err
	}

	scrapeManager, err := scrape.NewManager(nil, logger, fanoutStorage, prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}
	if err = scrapeManager.ApplyConfig(&cfg); err != nil {
		return err
	}
	scraper.Set(scrapeManager)
	db, err := agent.Open(logger, prometheus.DefaultRegisterer, remoteStorage, ms.walDir, agent.DefaultOptions())
	if err != nil {
		return err
	}
	localStorage.Set(db, 0)
	db.SetWriteNotified(remoteStorage)

	tch := make(chan map[string][]*targetgroup.Group)
	go scrapeManager.Run(tch)
	tch <- map[string][]*targetgroup.Group{
		jobName: {
			&targetgroup.Group{
				Targets: []model.LabelSet{
					{
						model.AddressLabel:  model.LabelValue(ms.listenAddr),
						model.InstanceLabel: model.LabelValue(ms.listenAddr),
					},
				},
				Labels: model.LabelSet{model.JobLabel: jobName},
			},
		},
	}

	return nil
}
