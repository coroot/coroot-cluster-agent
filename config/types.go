package config

import "golang.org/x/exp/maps"

type Config struct {
	ApplicationInstrumentation []ApplicationInstrumentation `json:"application_instrumentation"`

	AWSConfig *AWSConfig `json:"aws_config"`
}

type ApplicationInstrumentation struct {
	Type        string            `json:"type"`
	Host        string            `json:"host"`
	Port        string            `json:"port"`
	Credentials Credentials       `json:"credentials"`
	Params      map[string]string `json:"params"`
	Instance    string            `json:"instance"`
}

type Credentials struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

type Database struct {
	Type        string            `yaml:"type"`
	Host        string            `yaml:"host"`
	Port        string            `yaml:"port"`
	RDS         string            `yaml:"rds"`
	Elasticache string            `yaml:"elasticache"`
	Credentials Credentials       `yaml:"credentials"`
	Params      map[string]string `yaml:"params"`
}

type AWSConfig struct {
	Region          string `json:"region" yaml:"region"`
	AccessKeyID     string `json:"access_key_id" yaml:"accessKeyId"`
	SecretAccessKey string `json:"secret_access_key" yaml:"secretAccessKey"`

	RDSTagFilters         map[string]string `json:"rds_tag_filters" yaml:"rdsTagFilters"`
	ElasticacheTagFilters map[string]string `json:"elasticache_tag_filters" yaml:"elasticacheTagFilters"`
}

func (c *AWSConfig) Equal(other *AWSConfig) bool {
	return c.Region == other.Region &&
		c.AccessKeyID == other.AccessKeyID &&
		c.SecretAccessKey == other.SecretAccessKey &&
		maps.Equal(c.RDSTagFilters, other.RDSTagFilters) &&
		maps.Equal(c.ElasticacheTagFilters, other.ElasticacheTagFilters)
}
