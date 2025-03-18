package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"k8s.io/klog"
)

type Listener interface {
	ListenConfigUpdates(updates <-chan Config)
}

type Updater struct {
	endpoint       *url.URL
	apiKey         string
	updateInterval time.Duration
	httpClient     *http.Client
	subscribers    []chan<- Config
}

func NewUpdater() (*Updater, error) {
	c := &Updater{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/config"),
		apiKey:         *flags.APIKey,
		updateInterval: *flags.ConfigUpdateInterval,
		httpClient:     &http.Client{Timeout: *flags.ConfigUpdateTimeout},
	}
	klog.Infof("endpoint: %s, update interval: %s", c.endpoint, c.updateInterval)

	return c, nil
}

func (u *Updater) SubscribeForUpdates(l Listener) {
	ch := make(chan Config)
	l.ListenConfigUpdates(ch)
	u.subscribers = append(u.subscribers, ch)
}

func (u *Updater) Start() {
	go func() {
		ticker := time.NewTicker(u.updateInterval)
		defer ticker.Stop()
		for {
			cfg, err := u.fetchConfig()
			if err != nil {
				klog.Error(err)
			} else {
				for _, s := range u.subscribers {
					s <- *cfg
				}
			}
			<-ticker.C
		}
	}()
}

func (u *Updater) Stop() {
	for _, s := range u.subscribers {
		close(s)
	}
}

func (u *Updater) fetchConfig() (*Config, error) {
	req, err := http.NewRequest(http.MethodGet, u.endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	common.SetAuthHeaders(req, u.apiKey)
	resp, err := u.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, string(body))
	}
	var cfg Config
	err = json.NewDecoder(resp.Body).Decode(&cfg)
	return &cfg, err
}
