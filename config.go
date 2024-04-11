package main

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/coroot/coroot-cluster-agent/profiles"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Listen string `yaml:"listen"`

	CorootUrl string `yaml:"coroot_url"`
	ApiKey    string `yaml:"api_key"`

	Profiles profiles.Config `yaml:"profiles"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	data = []byte(os.ExpandEnv(string(data)))

	cfg := &Config{
		Listen: ":80",
	}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	if cfg.CorootUrl == "" {
		return nil, fmt.Errorf("coroot_url is required")
	}

	u, err := url.Parse(cfg.CorootUrl)
	if err != nil {
		return nil, err
	}

	if cfg.Profiles.Endpoint == "" {
		cfg.Profiles.Endpoint = u.JoinPath("/v1/profiles").String()
	}
	if cfg.Profiles.Scrape.Timeout == 0 {
		cfg.Profiles.Scrape.Timeout = 10 * time.Second
	}

	return cfg, nil
}
