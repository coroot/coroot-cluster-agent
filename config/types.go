package config

import (
	"slices"

	"golang.org/x/exp/maps"
)

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
	CloudSQL    string            `yaml:"cloudsql"`
	Memorystore string            `yaml:"memorystore"`
	OCIDB       string            `yaml:"ocidb"`
	OCICache    string            `yaml:"ocicache"`
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

type GCPConfig struct {
	ProjectID               string            `json:"project_id" yaml:"projectId"`
	Region                  string            `json:"region" yaml:"region"`
	CredentialsJSON         string            `json:"credentials_json" yaml:"credentialsJson"`
	CloudSQLLabelFilters    map[string]string `json:"cloudsql_label_filters" yaml:"cloudsqlLabelFilters"`
	MemorystoreLabelFilters map[string]string `json:"memorystore_label_filters" yaml:"memorystoreLabelFilters"`
}

func (c *GCPConfig) Equal(other *GCPConfig) bool {
	return c.ProjectID == other.ProjectID &&
		c.Region == other.Region &&
		c.CredentialsJSON == other.CredentialsJSON &&
		maps.Equal(c.CloudSQLLabelFilters, other.CloudSQLLabelFilters) &&
		maps.Equal(c.MemorystoreLabelFilters, other.MemorystoreLabelFilters)
}

type OCIConfig struct {
	CompartmentIDs  []string          `json:"compartment_ids" yaml:"compartmentIds"`
	Region          string            `json:"region" yaml:"region"`
	TenancyID       string            `json:"tenancy_id" yaml:"tenancyId"` // API key auth: the four fields below
	UserID          string            `json:"user_id" yaml:"userId"`
	Fingerprint     string            `json:"fingerprint" yaml:"fingerprint"`
	PrivateKey      string            `json:"private_key" yaml:"privateKey"`
	DBTagFilters    map[string]string `json:"db_tag_filters" yaml:"dbTagFilters"`
	CacheTagFilters map[string]string `json:"cache_tag_filters" yaml:"cacheTagFilters"`
}

func (c *OCIConfig) Equal(other *OCIConfig) bool {
	return slices.Equal(c.CompartmentIDs, other.CompartmentIDs) &&
		c.Region == other.Region &&
		c.TenancyID == other.TenancyID &&
		c.UserID == other.UserID &&
		c.Fingerprint == other.Fingerprint &&
		c.PrivateKey == other.PrivateKey &&
		maps.Equal(c.DBTagFilters, other.DBTagFilters) &&
		maps.Equal(c.CacheTagFilters, other.CacheTagFilters)
}
