package aws

import (
	"net"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

var (
	dECInfo = common.Desc("aws_elasticache_info", "Elasticache instance info",
		"region", "availability_zone", "endpoint", "ipv4", "port",
		"engine", "engine_version", "instance_type", "cluster_id",
	)
	dECStatus = common.Desc("aws_elasticache_status", "Status of the Elasticache instance", "status")
)

type ECCollector struct {
	region  string
	cluster *elasticache.CacheCluster
	node    *elasticache.CacheNode
	ip      *net.IPAddr
}

func NewECCollector(region string, cluster *elasticache.CacheCluster, node *elasticache.CacheNode) *ECCollector {
	return &ECCollector{region: region, cluster: cluster, node: node}
}

func (c *ECCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("aws_elasticache_collector", "", nil, nil)
}

func (c *ECCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- common.Gauge(dECStatus, 1, aws.StringValue(c.node.CacheNodeStatus))

	cluster := aws.StringValue(c.cluster.ReplicationGroupId)
	if cluster == "" {
		cluster = aws.StringValue(c.cluster.CacheClusterId)
	}
	var address, port, ip string
	if c.node.Endpoint != nil {
		address = aws.StringValue(c.node.Endpoint.Address)
		port = strconv.Itoa(int(aws.Int64Value(c.node.Endpoint.Port)))
	}
	if c.ip != nil {
		ip = c.ip.String()
	}
	ch <- common.Gauge(dECInfo, 1,
		c.region,
		aws.StringValue(c.node.CustomerAvailabilityZone),
		address,
		ip,
		port,
		aws.StringValue(c.cluster.Engine),
		aws.StringValue(c.cluster.EngineVersion),
		aws.StringValue(c.cluster.CacheNodeType),
		cluster,
	)
}

func (c *ECCollector) Stop() {
}

func (c *ECCollector) update(region string, cluster *elasticache.CacheCluster, node *elasticache.CacheNode) {
	c.region = region
	c.cluster = cluster
	c.node = node
	if c.node.Endpoint != nil {
		if ip, err := net.ResolveIPAddr("", aws.StringValue(c.node.Endpoint.Address)); err != nil {
			klog.Errorln(err)
		} else {
			c.ip = ip
		}
	}
}
