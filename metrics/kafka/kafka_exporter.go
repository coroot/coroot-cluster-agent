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
	clusterBrokerInfo                  = common.Desc("kafka_broker_info", "Information about the Kafka broker", "id", "broker_address")
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
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	topicExclude            *regexp.Regexp
	groupFilter             *regexp.Regexp
	groupExclude            *regexp.Regexp
	mu                      sync.Mutex
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

	logger.Info("kafka client initialized")
	// Init our exporter.
	return &Exporter{
		client:                  client,
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
	wg := sync.WaitGroup{}
	ch <- common.Gauge(
		clusterBrokers, float64(len(e.client.Brokers())),
	)
	for _, b := range e.client.Brokers() {
		ch <- common.Gauge(
			clusterBrokerInfo, 1, strconv.Itoa(int(b.ID())), b.Addr(),
		)
	}

	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		e.logger.Info("refreshing client metadata")

		if err := e.client.RefreshMetadata(); err != nil {
			e.logger.Warning(fmt.Errorf("cannot refresh topics, using cached data: %w", err))
		}

		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	topics, err := e.client.Topics()
	if err != nil {
		e.logger.Warning(fmt.Errorf("cannot get topics: %w", err))
		return
	}

	topicChannel := make(chan string)

	getTopicMetrics := func(topic string) {
		defer wg.Done()

		if !e.topicFilter.MatchString(topic) || e.topicExclude.MatchString(topic) {
			return
		}

		partitions, err := e.client.Partitions(topic)
		if err != nil {
			e.logger.Warning(fmt.Errorf("cannot get partitions of topic %s: %w", topic, err))
			return
		}
		ch <- common.Gauge(
			topicPartitions, float64(len(partitions)), topic,
		)
		e.mu.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		e.mu.Unlock()
		for _, partition := range partitions {
			broker, err := e.client.Leader(topic, partition)
			if err != nil {
				e.logger.Warning(fmt.Errorf("cannot get leader of topic %s partition %d: %w", topic, partition, err))
			} else {
				ch <- common.Gauge(
					topicPartitionLeader, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			currentOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				e.logger.Warning(fmt.Errorf("cannot get current offset of topic %s partition %d: %w", topic, partition, err))
			} else {
				e.mu.Lock()
				offset[topic][partition] = currentOffset
				e.mu.Unlock()
				ch <- common.Gauge(
					topicCurrentOffset, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			oldestOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				e.logger.Warning(fmt.Errorf("cannot get oldest offset of topic %s partition %d: %w", topic, partition, err))
			} else {
				ch <- common.Gauge(
					topicOldestOffset, float64(oldestOffset), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			replicas, err := e.client.Replicas(topic, partition)
			if err != nil {
				e.logger.Warning(fmt.Errorf("cannot get replicas of topic %s partition %d: %w", topic, partition, err))
			} else {
				ch <- common.Gauge(
					topicPartitionReplicas, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			inSyncReplicas, err := e.client.InSyncReplicas(topic, partition)
			if err != nil {
				e.logger.Warning(fmt.Errorf("cannot get in-sync replicas of topic %s partition %d: %w", topic, partition, err))
			} else {
				ch <- common.Gauge(
					topicPartitionInSyncReplicas, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			if broker != nil && replicas != nil && len(replicas) > 0 && broker.ID() == replicas[0] {
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

			if e.useZooKeeperLag {
				ConsumerGroups, err := e.zookeeperClient.Consumergroups()
				if err != nil {
					e.logger.Warning(fmt.Errorf("cannot get consumer group: %w", err))
				}

				for _, group := range ConsumerGroups {
					offset, _ := group.FetchOffset(topic, partition)
					if offset > 0 {

						consumerGroupLag := currentOffset - offset
						ch <- common.Gauge(
							consumergroupLagZookeeper, float64(consumerGroupLag), group.Name, topic, strconv.FormatInt(int64(partition), 10),
						)
					}
				}
			}
		}
	}

	loopTopics := func() {
		ok := true
		for ok {
			topic, open := <-topicChannel
			ok = open
			if open {
				getTopicMetrics(topic)
			}
		}
	}

	minx := func(x int, y int) int {
		if x < y {
			return x
		} else {
			return y
		}
	}

	N := len(topics)
	if N > 1 {
		N = minx(N/2, e.topicWorkers)
	}

	for w := 1; w <= N; w++ {
		go loopTopics()
	}

	for _, topic := range topics {
		if e.topicFilter.MatchString(topic) && !e.topicExclude.MatchString(topic) {
			wg.Add(1)
			topicChannel <- topic
		}
	}
	close(topicChannel)

	wg.Wait()

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			e.logger.Warning(fmt.Errorf("cannot connect to broker %d: %w", broker.ID(), err))
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			e.logger.Warning(fmt.Errorf("cannot get consumer group: %w", err))
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
			e.logger.Warning(fmt.Errorf("cannot get describe groups: %w", err))
			return
		}
		for _, group := range describeGroups.Groups {
			if group.Err != 0 {
				e.logger.Warning(fmt.Errorf("cannot describe for the group %s with error code %d", group.GroupId, group.Err))
				continue
			}
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: e.fetchOffsetVersion()}
			if e.offsetShowAll {
				for topic, partitions := range offset {
					for partition := range partitions {
						offsetFetchRequest.AddPartition(topic, partition)
					}
				}
			} else {
				for _, member := range group.Members {
					if len(member.MemberAssignment) == 0 {
						e.logger.Warning(fmt.Errorf("memberAssignment is empty for group member: %v in group: %v", member.MemberId, group.GroupId))
						continue
					}
					assignment, err := member.GetMemberAssignment()
					if err != nil {
						e.logger.Warning(fmt.Errorf("cannot get getMemberAssignment of group member %v: %w", member, err))
						continue
					}
					for topic, partions := range assignment.Topics {
						for _, partition := range partions {
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
				e.logger.Warning(fmt.Errorf("cannot get offset of group %s: %w", group.GroupId, err))
				continue
			}

			for topic, partitions := range offsetFetchResponse.Blocks {
				// If the topic is not consumed by that consumer group, skip it
				topicConsumed := false
				for _, offsetFetchResponseBlock := range partitions {
					// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
					if offsetFetchResponseBlock.Offset != -1 {
						topicConsumed = true
						break
					}
				}
				if !topicConsumed {
					continue
				}

				var currentOffsetSum int64
				var lagSum int64
				for partition, offsetFetchResponseBlock := range partitions {
					err := offsetFetchResponseBlock.Err
					if err != sarama.ErrNoError {
						e.logger.Warning(fmt.Errorf("error for partition %d: %v", partition, err.Error()))
						continue
					}
					currentOffset := offsetFetchResponseBlock.Offset
					currentOffsetSum += currentOffset
					ch <- common.Gauge(
						consumergroupCurrentOffset, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
					)
					e.mu.Lock()
					currentPartitionOffset, currentPartitionOffsetError := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
					if currentPartitionOffsetError != nil {
						e.logger.Warning(fmt.Errorf("cannot get current offset of topic %s partition %d: %w", topic, partition, currentPartitionOffsetError))
					} else {
						var lag int64
						if offsetFetchResponseBlock.Offset == -1 {
							lag = -1
						} else {
							if offset, ok := offset[topic][partition]; ok {
								if currentPartitionOffset == -1 {
									currentPartitionOffset = offset
								}
							}
							lag = currentPartitionOffset - offsetFetchResponseBlock.Offset
							lagSum += lag
						}

						ch <- common.Gauge(
							consumergroupLag, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
						)
					}
					e.mu.Unlock()
				}
				ch <- common.Gauge(
					consumergroupCurrentOffsetSum, float64(currentOffsetSum), group.GroupId, topic,
				)
				ch <- common.Gauge(
					consumergroupLagSum, float64(lagSum), group.GroupId, topic,
				)
			}
		}
	}

	e.logger.Info("fetching consumer group metrics")
	if len(e.client.Brokers()) > 0 {
		uniqueBrokerAddresses := make(map[string]bool)
		var servers []string
		for _, broker := range e.client.Brokers() {
			normalizedAddress := strings.ToLower(broker.Addr())
			if !uniqueBrokerAddresses[normalizedAddress] {
				uniqueBrokerAddresses[normalizedAddress] = true
				servers = append(servers, broker.Addr())
			}
		}
		e.logger.Info(fmt.Sprintf("discovered %d unique broker(s)", len(servers)))
		for _, broker := range e.client.Brokers() {
			for _, server := range servers {
				if server == broker.Addr() {
					wg.Add(1)
					go getConsumerGroupMetrics(broker)
				}
			}
		}
		wg.Wait()
	} else {
		e.logger.Warning(fmt.Errorf("no valid broker, cannot get consumer group metrics"))
	}
}
