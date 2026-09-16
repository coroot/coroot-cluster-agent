package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Static struct {
	AWS       *AWSConfig `yaml:"aws"`
	GCP       *GCPConfig `yaml:"gcp"`
	Databases []Database `yaml:"databases"`
}

func LoadStatic(path string) (*Static, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Static
	if err = yaml.Unmarshal([]byte(os.ExpandEnv(string(data))), &s); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	if err = s.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return &s, nil
}

func (s *Static) Validate() error {
	if s.AWS != nil && (s.AWS.AccessKeyID == "") != (s.AWS.SecretAccessKey == "") {
		return fmt.Errorf("aws: both accessKeyId and secretAccessKey must be set, or neither")
	}
	for i, d := range s.Databases {
		if d.Type == "" {
			return fmt.Errorf("databases[%d]: type is required", i)
		}
		sources := 0
		for _, v := range []string{d.Host, d.RDS, d.Elasticache, d.CloudSQL, d.Memorystore} {
			if v != "" {
				sources++
			}
		}
		if sources != 1 {
			return fmt.Errorf("databases[%d]: exactly one of host, rds, elasticache, cloudsql or memorystore is required", i)
		}
		if d.Host != "" && d.Port == "" {
			return fmt.Errorf("databases[%d]: port is required with host", i)
		}
	}
	return nil
}
