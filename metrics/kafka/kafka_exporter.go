package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	namespace = "kafka"
	clientID  = "kafka_exporter"
)

const (
	INFO  = 0
	DEBUG = 1
	TRACE = 2
)

var (
	clusterBrokers                     = common.Desc("kafka_brokers", "Number of brokers in the Kafka cluster")
	clusterBrokerInfo                  = common.Desc("kafka_broker_info", "Information about the Kafka broker", "id", "broker_address", "rack", "is_controller")
	topicPartitions                    = common.Desc("kafka_topic_partitions", "Number of partitions for a topic", "topic")
	topicCurrentOffset                 = common.Desc("kafka_topic_partition_current_offset", "Current offset of a partition", "topic", "partition")
	topicOldestOffset                  = common.Desc("kafka_topic_partition_oldest_offset", "Oldest offset of a partition", "topic", "partition")
	topicPartitionLeader               = common.Desc("kafka_topic_partition_leader", "Leader broker ID of a partition", "topic", "partition")
	topicPartitionReplicas             = common.Desc("kafka_topic_partition_replicas", "Number of replicas for a partition", "topic", "partition")
	topicPartitionInSyncReplicas       = common.Desc("kafka_topic_partition_in_sync_replica", "Number of in-sync replicas for a partition", "topic", "partition")
	topicPartitionUsesPreferredReplica = common.Desc("kafka_topic_partition_leader_is_preferred", "Whether the partition is using the preferred broker", "topic", "partition")
	topicUnderReplicatedPartition      = common.Desc("kafka_topic_partition_under_replicated_partition", "Whether the partition is under-replicated", "topic", "partition")
	consumergroupCurrentOffset         = common.Desc("kafka_consumergroup_current_offset", "Current offset of a consumer group", "consumergroup", "topic", "partition")
	consumergroupCurrentOffsetSum      = common.Desc("kafka_consumergroup_current_offset_sum", "Sum of current offsets for a consumer group", "consumergroup", "topic")
	consumergroupLag                   = common.Desc("kafka_consumergroup_lag", "Lag of a consumer group", "consumergroup", "topic", "partition")
	consumergroupLagSum                = common.Desc("kafka_consumergroup_lag_sum", "Sum of lag for a consumer group", "consumergroup", "topic")
	consumergroupLagZookeeper          = common.Desc("kafka_consumergroupzookeeper_lag_zookeeper", "Lag of a consumer group from ZooKeeper", "consumergroup", "topic", "partition")
	consumergroupMembers               = common.Desc("kafka_consumergroup_members", "Number of members in a consumer group", "consumergroup")
	topicConfigDesc                    = common.Desc("kafka_topic_config", "Configuration of a Kafka topic", "topic", "cleanup_policy", "retention_ms", "max_message_bytes", "segment_bytes", "retention_bytes")
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	adminClient             sarama.ClusterAdmin
	topicFilter             *regexp.Regexp
	topicExclude            *regexp.Regexp
	groupFilter             *regexp.Regexp
	groupExclude            *regexp.Regexp
	mu                      sync.RWMutex
	useZooKeeperLag         bool
	zookeeperClient         *kazoo.Kazoo
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
	offsetShowAll           bool
	topicWorkers            int
	allowConcurrent         bool
	sgMutex                 sync.Mutex
	sgWaitCh                chan struct{}
	sgChans                 []chan<- prometheus.Metric
	consumerGroupFetchAll   bool
	logger                  logger.Logger
}

type deferredGroupTask struct {
	group  *sarama.GroupDescription
	blocks map[string]map[int32]*sarama.OffsetFetchResponseBlock
}

type TopicConfig struct {
	CleanupPolicy   string
	RetentionMs     string
	MaxMessageBytes string
	SegmentBytes    string
	RetentionBytes  string
}

type KafkaOpts struct {
	Uri                      []string `default:"kafka:9092"`
	UseSASL                  bool     `default:"false"`
	UseSASLHandshake         bool     `default:"true"`
	SaslUsername             string   `default:""`
	SaslPassword             string   `default:""`
	SaslMechanism            string   `default:""`
	SaslDisablePAFXFast      bool     `default:"false"`
	SaslAwsRegion            string   `default:""`
	SaslOAuthBearerTokenUrl  string   `default:""`
	UseTLS                   bool     `default:"false"`
	TlsServerName            string   `default:""`
	TlsCAFile                string   `default:""`
	TlsCertFile              string   `default:""`
	TlsKeyFile               string   `default:""`
	TlsInsecureSkipTLSVerify bool     `default:"false"`
	KafkaVersion             string   `default:"2.0.0.0"`
	UseZooKeeperLag          bool     `default:"false"`
	UriZookeeper             []string `default:"localhost:2181"`
	Labels                   string   `default:""`
	MetadataRefreshInterval  string   `default:"30s"`
	ServiceName              string   `default:""`
	KerberosConfigPath       string   `default:""`
	Realm                    string   `default:""`
	KeyTabPath               string   `default:""`
	KerberosAuthType         string   `default:""`
	OffsetShowAll            bool     `default:"true"`
	TopicWorkers             int      `default:"100"`
	AllowConcurrent          bool     `default:"false"`
	AllowAutoTopicCreation   bool     `default:"false"`
	VerbosityLogLevel        int      `default:"0"`
	TopicFilter              string   `default:".*"`
	TopicExclude             string   `default:"^$"`
	GroupFilter              string   `default:".*"`
	GroupExclude             string   `default:"^$"`
}

func NewKafkaOpts() KafkaOpts {
	return KafkaOpts{
		Uri:                      []string{"kafka:9092"},
		UseSASL:                  false,
		UseSASLHandshake:         true,
		SaslUsername:             "",
		SaslPassword:             "",
		SaslMechanism:            "",
		SaslDisablePAFXFast:      false,
		SaslAwsRegion:            "",
		SaslOAuthBearerTokenUrl:  "",
		UseTLS:                   false,
		TlsServerName:            "",
		TlsCAFile:                "",
		TlsCertFile:              "",
		TlsKeyFile:               "",
		TlsInsecureSkipTLSVerify: false,
		KafkaVersion:             sarama.V2_0_0_0.String(),
		UseZooKeeperLag:          false,
		UriZookeeper:             []string{"localhost:2181"},
		Labels:                   "",
		MetadataRefreshInterval:  "30s",
		ServiceName:              "",
		KerberosConfigPath:       "",
		Realm:                    "",
		KeyTabPath:               "",
		KerberosAuthType:         "",
		OffsetShowAll:            true,
		TopicWorkers:             100,
		AllowConcurrent:          false,
		AllowAutoTopicCreation:   false,
		VerbosityLogLevel:        0,
		TopicFilter:              ".*",
		TopicExclude:             "^$",
		GroupFilter:              ".*",
		GroupExclude:             "^$",
	}
}

type MSKAccessTokenProvider struct {
	region string
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthToken(context.TODO(), m.region)
	return &sarama.AccessToken{Token: token}, err
}

type OAuth2Config interface {
	Token(ctx context.Context) (*oauth2.Token, error)
}

type oauthbearerTokenProvider struct {
	tokenExpiration time.Time
	token           string
	oauth2Config    OAuth2Config
}

func newOauthbearerTokenProvider(oauth2Config OAuth2Config) *oauthbearerTokenProvider {
	return &oauthbearerTokenProvider{
		tokenExpiration: time.Time{},
		token:           "",
		oauth2Config:    oauth2Config,
	}
}

func (o *oauthbearerTokenProvider) Token() (*sarama.AccessToken, error) {
	var accessToken string
	var err error

	if o.token != "" && time.Now().Before(o.tokenExpiration.Add(time.Duration(-2)*time.Second)) {
		accessToken = o.token
		err = nil
	} else {
		token, err := o.oauth2Config.Token(context.Background())
		if err == nil {
			accessToken = token.AccessToken
			o.token = token.AccessToken
			o.tokenExpiration = token.Expiry
		}
	}

	return &sarama.AccessToken{Token: accessToken}, err
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if !certReadable && !keyReadable {
		return false, nil
	}

	if !certReadable {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if !keyReadable {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts KafkaOpts, logger logger.Logger) (*Exporter, error) {

	topicFilter := opts.TopicFilter
	topicExclude := opts.TopicExclude
	groupFilter := opts.GroupFilter
	groupExclude := opts.GroupExclude

	var zookeeperClient *kazoo.Kazoo
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.KafkaVersion)
	if err != nil {
		return nil, err
	}
	config.Version = kafkaVersion

	if opts.UseSASL {
		// Convert to lowercase so that SHA512 and SHA256 is still valid
		opts.SaslMechanism = strings.ToLower(opts.SaslMechanism)

		saslPassword := opts.SaslPassword
		if saslPassword == "" {
			saslPassword = os.Getenv("SASL_USER_PASSWORD")
		}

		switch opts.SaslMechanism {
		case "scram-sha512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		case "scram-sha256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		case "gssapi":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeGSSAPI)
			config.Net.SASL.GSSAPI.ServiceName = opts.ServiceName
			config.Net.SASL.GSSAPI.KerberosConfigPath = opts.KerberosConfigPath
			config.Net.SASL.GSSAPI.Realm = opts.Realm
			config.Net.SASL.GSSAPI.Username = opts.SaslUsername
			if opts.KerberosAuthType == "keytabAuth" {
				config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
				config.Net.SASL.GSSAPI.KeyTabPath = opts.KeyTabPath
			} else {
				config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
				config.Net.SASL.GSSAPI.Password = saslPassword
			}
			if opts.SaslDisablePAFXFast {
				config.Net.SASL.GSSAPI.DisablePAFXFAST = true
			}
		case "awsiam":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeOAuth)
			config.Net.SASL.TokenProvider = &MSKAccessTokenProvider{region: opts.SaslAwsRegion}
		case "oauthbearer":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeOAuth)
			tokenUrl := opts.SaslOAuthBearerTokenUrl
			if tokenUrl == "" {
				tokenUrl = os.Getenv("SASL_OAUTHBEARER_TOKEN_URL")
			}
			if tokenUrl == "" {
				log.Fatalf("[ERROR] sasl.oauthbearer-token-url must be configured or SASL_OAUTHBEARER_TOKEN_URL environment variable must be set when using the OAuthBearer SASL mechanism")
			}
			saslUsername := opts.SaslUsername
			if saslUsername == "" {
				log.Fatalf("[ERROR] sasl.username must be configured when using the OAuthBearer SASL mechanism")
			}
			oauth2Config := clientcredentials.Config{
				TokenURL:     tokenUrl,
				ClientID:     saslUsername,
				ClientSecret: saslPassword,
			}
			config.Net.SASL.TokenProvider = newOauthbearerTokenProvider(&oauth2Config)
		case "plain":
		default:
			return nil, fmt.Errorf(
				`invalid sasl mechanism %q: can only be "scram-sha256", "scram-sha512", "gssapi", "awsiam" or "plain"`,
				opts.SaslMechanism,
			)
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.UseSASLHandshake

		if opts.SaslUsername != "" {
			config.Net.SASL.User = opts.SaslUsername
		}

		if saslPassword != "" {
			config.Net.SASL.Password = saslPassword
		}
	}

	if opts.UseTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			ServerName:         opts.TlsServerName,
			InsecureSkipVerify: opts.TlsInsecureSkipTLSVerify,
		}

		if opts.TlsCAFile != "" {
			if ca, err := os.ReadFile(opts.TlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs = x509.NewCertPool()
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				return nil, err
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.TlsCertFile, opts.TlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("error reading cert and key: %w", err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.TlsCertFile, opts.TlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				return nil, err
			}
		}
	}

	if opts.UseZooKeeperLag {
		logger.Info("using zookeeper lag, connecting to zookeeper")
		zookeeperClient, err = kazoo.NewKazoo(opts.UriZookeeper, nil)
		if err != nil {
			return nil, fmt.Errorf("error connecting to zookeeper: %w", err)
		}
	}

	interval, err := time.ParseDuration(opts.MetadataRefreshInterval)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse metadata refresh interval: %w", err)
	}

	config.Metadata.RefreshFrequency = interval

	config.Metadata.AllowAutoTopicCreation = opts.AllowAutoTopicCreation

	client, err := sarama.NewClient(opts.Uri, config)
	if err != nil {
		return nil, fmt.Errorf("Error Init Kafka Client: %w", err)
	}

	adminClient, err := sarama.NewClusterAdminFromClient(client)

	if err != nil {
		return nil, fmt.Errorf("Error Init Admin Kafka Client: %w", err)
	}

	logger.Info("kafka client initialized")
	// Init our exporter.
	return &Exporter{
		client:                  client,
		adminClient:             adminClient,
		topicFilter:             regexp.MustCompile(topicFilter),
		topicExclude:            regexp.MustCompile(topicExclude),
		groupFilter:             regexp.MustCompile(groupFilter),
		groupExclude:            regexp.MustCompile(groupExclude),
		useZooKeeperLag:         opts.UseZooKeeperLag,
		zookeeperClient:         zookeeperClient,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
		offsetShowAll:           opts.OffsetShowAll,
		topicWorkers:            opts.TopicWorkers,
		allowConcurrent:         opts.AllowConcurrent,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   config.Version.IsAtLeast(sarama.V2_0_0_0),
		logger:                  logger,
	}, nil
}

func (e *Exporter) fetchOffsetVersion() int16 {
	version := e.client.Config().Version
	if e.client.Config().Version.IsAtLeast(sarama.V2_0_0_0) {
		return 4
	} else if version.IsAtLeast(sarama.V0_10_2_0) {
		return 2
	} else if version.IsAtLeast(sarama.V0_8_2_2) {
		return 1
	}
	return 0
}

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- clusterBrokers
	ch <- clusterBrokerInfo
	ch <- topicConfigDesc
	ch <- topicCurrentOffset
	ch <- topicOldestOffset
	ch <- topicPartitions
	ch <- topicPartitionLeader
	ch <- topicPartitionReplicas
	ch <- topicPartitionInSyncReplicas
	ch <- topicPartitionUsesPreferredReplica
	ch <- topicUnderReplicatedPartition
	ch <- consumergroupCurrentOffset
	ch <- consumergroupCurrentOffsetSum
	ch <- consumergroupLag
	ch <- consumergroupLagZookeeper
	ch <- consumergroupLagSum
	ch <- consumergroupMembers
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if e.allowConcurrent {
		e.collect(ch)
		return
	}
	// Locking to avoid race add
	e.sgMutex.Lock()
	e.sgChans = append(e.sgChans, ch)
	// Safe to compare length since we own the Lock
	if len(e.sgChans) == 1 {
		e.sgWaitCh = make(chan struct{})
		go e.collectChans(e.sgWaitCh)
	} else {
		e.logger.Info("concurrent calls detected, waiting for first to finish")
	}
	// Put in another variable to ensure not overwriting it in another Collect once we wait
	waiter := e.sgWaitCh
	e.sgMutex.Unlock()
	// Released lock, we have insurance that our chan will be part of the collectChan slice
	<-waiter
	// collectChan finished
}

func (e *Exporter) collectChans(quit chan struct{}) {
	original := make(chan prometheus.Metric)
	container := make([]prometheus.Metric, 0, 100)
	go func() {
		for metric := range original {
			container = append(container, metric)
		}
	}()
	e.collect(original)
	close(original)
	// Lock to avoid modification on the channel slice
	e.sgMutex.Lock()
	for _, ch := range e.sgChans {
		for _, metric := range container {
			ch <- metric
		}
	}
	// Reset the slice
	e.sgChans = e.sgChans[:0]
	// Notify remaining waiting Collect they can return
	close(quit)
	// Release the lock so Collect can append to the slice again
	e.sgMutex.Unlock()
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		e.logger.Info("collect process took %v", duration)
	}()

	brokers, controllerId, err := e.adminClient.DescribeCluster()
	if err != nil {
		e.logger.Errorf("Cannot describe cluster, using client broker list: %v", err)
		brokers = e.client.Brokers()
	}

	ch <- common.Gauge(
		clusterBrokers, float64(len(brokers)),
	)
	for _, b := range brokers {
		ch <- common.Gauge(
			clusterBrokerInfo, 1, strconv.Itoa(int(b.ID())), b.Addr(), b.Rack(), func() string {
				if b.ID() == controllerId {
					return "1"
				} else {
					return "0"
				}
			}(),
		)
	}

	offset := make(map[string]map[int32]int64)
	// initialize the topic partition leader mapping
	topicPartitionLeaders := make(map[string]map[int32]int32)
	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		e.logger.Info("Refreshing client metadata")
		if err := e.client.RefreshMetadata(); err != nil {
			e.logger.Errorf("Cannot refresh topics, using cached data: %v", err)
		}
		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	// ==================== Phase 1: Collect Topic Metrics ====================
	phase1Start := time.Now()
	topics, err := e.client.Topics()
	if err != nil {
		e.logger.Errorf("Cannot get topics: %v", err)
		return
	}

	for _, topic := range topics {
		if !e.topicFilter.MatchString(topic) || e.topicExclude.MatchString(topic) {
			continue
		}
		resource := sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic,
		}
		entries, err := e.adminClient.DescribeConfig(resource)
		if err != nil {
			e.logger.Errorf("Failed to describe config for topic %s: %v", topic, err)
			continue
		}

		topicConfig := &TopicConfig{}

		for _, entry := range entries {
			switch entry.Name {
			case "cleanup.policy":
				topicConfig.CleanupPolicy = entry.Value
			case "retention.ms":
				topicConfig.RetentionMs = entry.Value
			case "max.message.bytes":
				topicConfig.MaxMessageBytes = entry.Value
			case "segment.bytes":
				topicConfig.SegmentBytes = entry.Value
			case "retention.bytes":
				topicConfig.RetentionBytes = entry.Value
			}
		}

		ch <- common.Gauge(
			topicConfigDesc, 1, topic, topicConfig.CleanupPolicy, topicConfig.RetentionMs, topicConfig.MaxMessageBytes, topicConfig.SegmentBytes, topicConfig.RetentionBytes,
		)
	}

	e.logger.Infof("Phase 1: Fetching topic offsets, Found %v topics", len(topics))
	// initialize the broker newest offset and oldest offset requests
	brokerNewestOffsetRequests := make(map[int32]*sarama.OffsetRequest)
	brokerOldestOffsetRequests := make(map[int32]*sarama.OffsetRequest)

	// iterate through all topic partitions, group by leader
	for _, topic := range topics {
		if !e.topicFilter.MatchString(topic) || e.topicExclude.MatchString(topic) {
			continue
		}

		partitions, err := e.client.Partitions(topic)
		if err != nil {
			e.logger.Errorf("Cannot get partitions of topic %s: %v", topic, err)
			continue
		}
		ch <- common.Gauge(
			topicPartitions, float64(len(partitions)), topic,
		)
		e.mu.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		topicPartitionLeaders[topic] = make(map[int32]int32, len(partitions))
		e.mu.Unlock()
		for _, partition := range partitions {
			leader, err := e.client.Leader(topic, partition)
			if err != nil {
				e.logger.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
				continue
			}
			e.mu.Lock()
			topicPartitionLeaders[topic][partition] = leader.ID()
			e.mu.Unlock()

			ch <- common.Gauge(
				topicPartitionLeader, float64(leader.ID()), topic, strconv.FormatInt(int64(partition), 10),
			)

			// build the newest offset request
			if _, ok := brokerNewestOffsetRequests[leader.ID()]; !ok {
				brokerNewestOffsetRequests[leader.ID()] = &sarama.OffsetRequest{}
			}
			brokerNewestOffsetRequests[leader.ID()].AddBlock(topic, partition, sarama.OffsetNewest, 1)

			// build the oldest offset request
			if _, ok := brokerOldestOffsetRequests[leader.ID()]; !ok {
				brokerOldestOffsetRequests[leader.ID()] = &sarama.OffsetRequest{}
			}
			brokerOldestOffsetRequests[leader.ID()].AddBlock(topic, partition, sarama.OffsetOldest, 1)

			replicas, err := e.client.Replicas(topic, partition)
			if err != nil {
				e.logger.Errorf("Cannot get replicas of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- common.Gauge(
					topicPartitionReplicas, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			inSyncReplicas, err := e.client.InSyncReplicas(topic, partition)
			if err != nil {
				e.logger.Errorf("Cannot get in-sync replicas of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- common.Gauge(
					topicPartitionInSyncReplicas, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			if leader != nil && replicas != nil && len(replicas) > 0 && leader.ID() == replicas[0] {
				ch <- common.Gauge(
					topicPartitionUsesPreferredReplica, float64(1), topic, strconv.FormatInt(int64(partition), 10),
				)
			} else {
				ch <- common.Gauge(
					topicPartitionUsesPreferredReplica, float64(0), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			if replicas != nil && inSyncReplicas != nil && len(inSyncReplicas) < len(replicas) {
				ch <- common.Gauge(
					topicUnderReplicatedPartition, float64(1), topic, strconv.FormatInt(int64(partition), 10),
				)
			} else {
				ch <- common.Gauge(
					topicUnderReplicatedPartition, float64(0), topic, strconv.FormatInt(int64(partition), 10),
				)
			}
		}
	}

	var offsetWg sync.WaitGroup
	fetchOffsetsFromBroker := func(brokerID int32, req *sarama.OffsetRequest, isNewest bool) {
		defer offsetWg.Done()

		broker, err := e.client.Broker(brokerID)
		if err != nil || broker == nil {
			e.logger.Errorf("Cannot get broker %d (nil: %v): %v", brokerID, broker == nil, err)
			return
		}

		// send the batch request to get topic partition offsets
		resp, err := broker.GetAvailableOffsets(req)
		if err != nil {
			e.logger.Errorf("Cannot get offsets from broker %d: %v", brokerID, err)
			return
		}

		// parse and report the topic partition offsets
		for topic, partitions := range resp.Blocks {
			for partition, block := range partitions {
				if block.Err != sarama.ErrNoError {
					e.logger.Errorf("Error fetching offset for %s:%d from broker %d: %v", topic, partition, brokerID, block.Err)
					continue
				}

				val := block.Offsets[0]
				if isNewest {
					e.mu.Lock()
					offset[topic][partition] = val
					e.mu.Unlock()
					ch <- common.Gauge(
						topicCurrentOffset, float64(val),
						topic, strconv.FormatInt(int64(partition), 10),
					)
				} else {
					ch <- common.Gauge(
						topicOldestOffset, float64(val),
						topic, strconv.FormatInt(int64(partition), 10),
					)
				}
			}
		}
	}

	for bid, req := range brokerNewestOffsetRequests {
		offsetWg.Add(1)
		go fetchOffsetsFromBroker(bid, req, true)
	}

	for bid, req := range brokerOldestOffsetRequests {
		offsetWg.Add(1)
		go fetchOffsetsFromBroker(bid, req, false)
	}

	offsetWg.Wait()

	if e.useZooKeeperLag {
		ConsumerGroups, err := e.zookeeperClient.Consumergroups()
		if err != nil {
			e.logger.Errorf("Cannot get consumer group %v", err)
		} else {
			for _, group := range ConsumerGroups {
				for topic, partitions := range offset {
					for partition := range partitions {
						zkOffset, _ := group.FetchOffset(topic, partition)
						if zkOffset > 0 {
							e.mu.RLock()
							currentOffset := offset[topic][partition]
							e.mu.RUnlock()
							consumerGroupLag := currentOffset - zkOffset
							ch <- common.Gauge(
								consumergroupLagZookeeper, float64(consumerGroupLag),
								group.Name, topic, strconv.FormatInt(int64(partition), 10),
							)
						}
					}
				}
			}
		}
	}

	e.logger.Info("Phase 1 (Topic Offsets) took %v", time.Since(phase1Start))

	// ==================== Phase 2: Collect Group Metrics and Calculate Lag ====================
	phase2Start := time.Now()
	e.logger.Info("Phase 2: Fetching consumer group offsets and calculating lag")

	var cgWg sync.WaitGroup
	var deferredTasks []*deferredGroupTask
	var tasksMu sync.Mutex

	processConsumerGroup := func(broker *sarama.Broker) {
		defer cgWg.Done()
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			e.logger.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			e.logger.Errorf("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) && !e.groupExclude.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			e.logger.Errorf("Cannot get describe groups: %v", err)
			return
		}
		for _, group := range describeGroups.Groups {
			if group.Err != 0 {
				e.logger.Errorf("Cannot describe for the group %s with error code %d", group.GroupId, group.Err)
				continue
			}

			// calculate and export the group metrics, if have nagetive group lag, will be deferred to be processed later
			task := e.emitGroupMetric(group, broker, offset, topicPartitionLeaders, ch)
			if task != nil {
				tasksMu.Lock()
				deferredTasks = append(deferredTasks, task)
				tasksMu.Unlock()
			}
		}
	}

	if len(e.client.Brokers()) > 0 {
		uniqueBrokerAddresses := make(map[string]bool)
		var servers []*sarama.Broker
		for _, broker := range e.client.Brokers() {
			normalizedAddress := strings.ToLower(broker.Addr())
			if !uniqueBrokerAddresses[normalizedAddress] {
				uniqueBrokerAddresses[normalizedAddress] = true
				servers = append(servers, broker)
			}
		}

		for _, broker := range servers {
			cgWg.Add(1)
			go processConsumerGroup(broker)
		}

		cgWg.Wait()
		e.logger.Info("All processConsumerGroup goroutines completed")
	} else {
		e.logger.Error("No valid broker, cannot get consumer group metrics")
	}

	e.logger.Info("Phase 2 (Consumer Group Offsets + Lag) took %v", time.Since(phase2Start))

	// ==================== Phase 3: Process the case if the group lag is negative ====================
	phase3Start := time.Now()
	if len(deferredTasks) > 0 {
		e.logger.Info("Phase 3: Processing %d groups with negative lag", len(deferredTasks))

		// 1. summarize all the topic partitions that need to be re-fetched
		toRefresh := make(map[int32]*sarama.OffsetRequest)
		for _, task := range deferredTasks {
			for topic, partitions := range task.blocks {
				for partition, block := range partitions {
					e.mu.RLock()
					lag := offset[topic][partition] - block.Offset
					e.mu.RUnlock()

					if block.Offset != -1 && lag < 0 {
						e.mu.RLock()
						bid := topicPartitionLeaders[topic][partition]
						e.mu.RUnlock()
						if _, ok := toRefresh[bid]; !ok {
							toRefresh[bid] = &sarama.OffsetRequest{}
						}
						toRefresh[bid].AddBlock(topic, partition, sarama.OffsetNewest, 1)
					}
				}
			}
		}

		// 2. send the batch request to get the new topic partition offsets and update the global offsetMap
		var refreshWg sync.WaitGroup
		for bid, req := range toRefresh {
			refreshWg.Add(1)
			go func(id int32, r *sarama.OffsetRequest) {
				defer refreshWg.Done()
				b, err := e.client.Broker(id)
				if err != nil || b == nil {
					e.logger.Errorf("Cannot get broker %d (it might be nil): %v", id, err)
					return
				}

				if ok, _ := b.Connected(); !ok {
					if err := b.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
						e.logger.Errorf("Cannot open broker %d: %v", id, err)
						return
					}
				}
				resp, err := b.GetAvailableOffsets(r)
				if err != nil {
					e.logger.Errorf("Cannot get available offsets from broker %d: %v", id, err)
					return
				}
				for t, ps := range resp.Blocks {
					for p, block := range ps {
						if block.Err == sarama.ErrNoError {
							e.mu.Lock()
							offset[t][p] = block.Offsets[0]
							e.mu.Unlock()
						}
					}
				}
			}(bid, req)
		}
		refreshWg.Wait()

		// 3. iterate through all the deferred tasks again, and report the metrics using the updated offsetMap
		for _, task := range deferredTasks {
			e.reportGroupMetrics(task.group.GroupId, task.blocks, offset, ch)
		}
		e.logger.Info("Phase 3 (Refresh negative lag offsets) took %v", time.Since(phase3Start))
	}
}

func (e *Exporter) emitGroupMetric(group *sarama.GroupDescription, broker *sarama.Broker, offsetMap map[string]map[int32]int64, topicPartitionLeaders map[string]map[int32]int32, ch chan<- prometheus.Metric) *deferredGroupTask {
	// build the offset fetch request
	offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: e.fetchOffsetVersion()}
	if e.offsetShowAll {
		for topic, partitions := range offsetMap {
			for partition := range partitions {
				offsetFetchRequest.AddPartition(topic, partition)
			}
		}
	} else {
		for _, member := range group.Members {
			if len(member.MemberAssignment) == 0 {
				continue
			}
			assignment, err := member.GetMemberAssignment()
			if err != nil {
				continue
			}
			for topic, partitions := range assignment.Topics {
				for _, partition := range partitions {
					offsetFetchRequest.AddPartition(topic, partition)
				}
			}
		}
	}

	ch <- common.Gauge(
		consumergroupMembers, float64(len(group.Members)), group.GroupId,
	)

	offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
	if err != nil {
		e.logger.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
		return nil
	}

	hasNegativeLag := false
	for topic, partitions := range offsetFetchResponse.Blocks {
		for partition, block := range partitions {
			e.mu.RLock()
			cachedOffset := offsetMap[topic][partition]
			e.mu.RUnlock()
			if block.Offset != -1 && cachedOffset-block.Offset < 0 {
				hasNegativeLag = true
				break
			}
		}
		if hasNegativeLag {
			break
		}
	}

	if hasNegativeLag {
		// 发现负 Lag，返回任务供后续批量处理
		return &deferredGroupTask{
			group:  group,
			blocks: offsetFetchResponse.Blocks,
		}
	}

	// 如果没有负 Lag，执行监控上报逻辑
	e.reportGroupMetrics(group.GroupId, offsetFetchResponse.Blocks, offsetMap, ch)
	return nil
}

// reportGroupMetrics 统一上报消费组指标
func (e *Exporter) reportGroupMetrics(
	groupId string,
	blocks map[string]map[int32]*sarama.OffsetFetchResponseBlock,
	offsetMap map[string]map[int32]int64, // 各分区的 Topic 最新位点
	ch chan<- prometheus.Metric,
) {
	for topic, partitions := range blocks {
		var currentOffsetSum int64
		var lagSum int64
		topicConsumed := false

		for _, block := range partitions {
			if block.Offset != -1 {
				topicConsumed = true
				break
			}
		}
		if !topicConsumed {
			continue
		}

		for partition, block := range partitions {
			if block.Err != sarama.ErrNoError {
				e.logger.Errorf("Error for partition %d: %v", partition, block.Err.Error())
				continue
			}

			currentOffset := block.Offset
			if currentOffset != -1 {
				currentOffsetSum += currentOffset
			}

			ch <- common.Gauge(
				consumergroupCurrentOffset, float64(currentOffset),
				groupId, topic, strconv.FormatInt(int64(partition), 10),
			)

			var lag int64
			if block.Offset == -1 {
				lag = -1
			} else {
				e.mu.RLock()
				cachedOffset, _ := offsetMap[topic][partition]
				e.mu.RUnlock()

				lag = cachedOffset - block.Offset

				lagSum += lag
			}
			ch <- common.Gauge(
				consumergroupLag, float64(lag),
				groupId, topic, strconv.FormatInt(int64(partition), 10),
			)
		}

		if topicConsumed {
			ch <- common.Gauge(
				consumergroupCurrentOffsetSum, float64(currentOffsetSum),
				groupId, topic,
			)
			ch <- common.Gauge(
				consumergroupLagSum, float64(lagSum),
				groupId, topic,
			)
		}
	}
}
