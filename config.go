package main

import (
	"os"
	"time"

	"github.com/coroot/coroot-cluster-agent/clickhouse"
	"github.com/coroot/coroot-cluster-agent/profiles"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Listen string `yaml:"listen"`

	Clickhouse clickhouse.Config `yaml:"clickhouse"`
	Profiles   profiles.Config   `yaml:"profiles"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	data = []byte(os.ExpandEnv(string(data)))

	cfg := &Config{
		Listen: ":80",
		Profiles: profiles.Config{
			TTLDays: 7,
		},
	}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Profiles.Scrape.Timeout == 0 {
		cfg.Profiles.Scrape.Timeout = 10 * time.Second
	}

	return cfg, nil
}
