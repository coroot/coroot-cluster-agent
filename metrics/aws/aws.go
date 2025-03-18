package aws

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

const (
	discoveryInterval = time.Minute
)

var (
	dError = common.Desc("aws_discovery_error", "AWS discovery error", "error")
)

type Discoverer struct {
	cfg  *config.AWSConfig
	sess *session.Session
	reg  prometheus.Registerer
	stop chan struct{}

	errors     map[string]bool
	errorsLock sync.RWMutex

	rdsCollectors map[string]*RDSCollector
	ecCollectors  map[string]*ECCollector
}

func NewDiscoverer(cfg *config.AWSConfig, reg prometheus.Registerer) (*Discoverer, error) {
	sess, err := newSession(cfg)
	if err != nil {
		return nil, err
	}
	d := &Discoverer{
		cfg:  cfg,
		sess: sess,
		reg:  reg,
		stop: make(chan struct{}),

		errors: map[string]bool{},

		rdsCollectors: map[string]*RDSCollector{},
		ecCollectors:  map[string]*ECCollector{},
	}

	err = reg.Register(d)
	if err != nil {
		return nil, err
	}

	go func() {
		d.discover()
		t := time.NewTicker(discoveryInterval)
		defer t.Stop()
		for {
			select {
			case <-d.stop:
				return
			case <-t.C:
				d.discover()
			}
		}
	}()
	return d, nil
}

func (d *Discoverer) ClientConfig(serviceName string, cfgs ...*aws.Config) client.Config {
	return d.sess.ClientConfig(serviceName, cfgs...)
}

func (d *Discoverer) Stop() {
	d.stop <- struct{}{}
	for id, c := range d.rdsCollectors {
		prometheus.WrapRegistererWith(rdsLabels(id), d.reg).Unregister(c)
		c.Stop()
	}
	for id, c := range d.ecCollectors {
		prometheus.WrapRegistererWith(ecLabels(id), d.reg).Unregister(c)
		c.Stop()
	}
	d.reg.Unregister(d)
}

func (d *Discoverer) Update(cfg *config.AWSConfig) error {
	if d.cfg.Equal(cfg) {
		return nil
	}
	sess, err := newSession(cfg)
	if err != nil {
		return err
	}
	d.cfg = cfg
	d.sess = sess
	return nil
}

func (d *Discoverer) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("aws_discoverer", "", nil, nil)
}

func (d *Discoverer) Collect(ch chan<- prometheus.Metric) {
	d.errorsLock.RLock()
	defer d.errorsLock.RUnlock()
	if len(d.errors) > 0 {
		for e := range d.errors {
			ch <- common.Gauge(dError, 1, e)
		}
	} else {
		ch <- common.Gauge(dError, 0, "")
	}
}

func (d *Discoverer) registerError(err error) {
	var awsErr awserr.Error
	ok := errors.As(err, &awsErr)
	if ok {
		d.errorsLock.Lock()
		d.errors[awsErr.Message()] = true
		d.errorsLock.Unlock()
	}
}

func (d *Discoverer) discover() {
	d.errorsLock.Lock()
	d.errors = map[string]bool{}
	d.errorsLock.Unlock()
	d.discoverRDS()
	d.discoverEC()
}

func (d *Discoverer) discoverRDS() {
	svc := rds.New(d.sess)
	input := &rds.DescribeDBInstancesInput{}
	seen := map[string]bool{}
	for {
		output, err := svc.DescribeDBInstances(input)
		if err != nil {
			klog.Error(err)
			d.registerError(err)
			break
		}
		for _, instance := range output.DBInstances {
			if filters := d.cfg.RDSTagFilters; len(filters) > 0 {
				o, err := svc.ListTagsForResource(&rds.ListTagsForResourceInput{ResourceName: instance.DBInstanceArn})
				if err != nil {
					klog.Error(err)
					d.registerError(err)
					continue
				}
				tags := map[string]string{}
				for _, t := range o.TagList {
					tags[aws.StringValue(t.Key)] = aws.StringValue(t.Value)
				}
				if !tagsMatched(filters, tags) {
					klog.Infof("RDS instance %s (tags: %s) was skipped according to the tag-based filters: %s", aws.StringValue(instance.DBInstanceIdentifier), tags, filters)
					continue
				}
			}
			id := d.cfg.Region + "/" + aws.StringValue(instance.DBInstanceIdentifier)
			seen[id] = true
			if d.rdsCollectors[id] == nil {
				klog.Infoln("new RDS instance found:", id)
				c := NewRDSCollector(d, d.cfg.Region, instance)
				if err = prometheus.WrapRegistererWith(rdsLabels(id), d.reg).Register(c); err != nil {
					klog.Error(err)
					continue
				}
				d.rdsCollectors[id] = c
			}
			d.rdsCollectors[id].update(d.cfg.Region, instance)
		}
		if output.Marker == nil {
			break
		}
		input.SetMarker(aws.StringValue(output.Marker))
	}

	for id, c := range d.rdsCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(rdsLabels(id), d.reg).Unregister(c)
			delete(d.rdsCollectors, id)
			c.Stop()
		}
	}
}

func (d *Discoverer) discoverEC() {
	svc := elasticache.New(d.sess)
	seen := map[string]bool{}
	for _, v := range []bool{false, true} {
		input := &elasticache.DescribeCacheClustersInput{
			ShowCacheNodeInfo:                       aws.Bool(true),
			ShowCacheClustersNotInReplicationGroups: aws.Bool(v),
		}
		for {
			output, err := svc.DescribeCacheClusters(input)
			if err != nil {
				klog.Error(err)
				d.registerError(err)
				break
			}
			for _, cluster := range output.CacheClusters {
				if filters := d.cfg.ElasticacheTagFilters; len(filters) > 0 {
					o, err := svc.ListTagsForResource(&elasticache.ListTagsForResourceInput{ResourceName: cluster.ARN})
					if err != nil {
						klog.Error(err)
						d.registerError(err)
						continue
					}
					tags := map[string]string{}
					for _, t := range o.TagList {
						tags[aws.StringValue(t.Key)] = aws.StringValue(t.Value)
					}
					if !tagsMatched(filters, tags) {
						klog.Infof("EC cluster %s (tags: %s) was skipped according to the tag-based filters: %s", aws.StringValue(cluster.CacheClusterId), tags, filters)
						continue
					}
				}
				for _, node := range cluster.CacheNodes {
					id := d.cfg.Region + "/" + aws.StringValue(cluster.CacheClusterId) + "/" + aws.StringValue(node.CacheNodeId)
					seen[id] = true
					if d.ecCollectors[id] == nil {
						klog.Infoln("new EC instance found:", id)
						c := NewECCollector(d.cfg.Region, cluster, node)
						if err = prometheus.WrapRegistererWith(ecLabels(id), d.reg).Register(c); err != nil {
							klog.Error(err)
							continue
						}
						d.ecCollectors[id] = c
					}
					d.ecCollectors[id].update(d.cfg.Region, cluster, node)
				}
			}
			if output.Marker == nil {
				break
			}
			input.SetMarker(aws.StringValue(output.Marker))
		}
	}

	for id, c := range d.ecCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(ecLabels(id), d.reg).Unregister(c)
			c.Stop()
			delete(d.ecCollectors, id)
		}
	}
}

func rdsLabels(id string) prometheus.Labels {
	return prometheus.Labels{"rds_instance_id": id}
}

func ecLabels(id string) prometheus.Labels {
	return prometheus.Labels{"ec_instance_id": id}
}

func newSession(cfg *config.AWSConfig) (*session.Session, error) {
	creds := credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey, "")
	config := aws.NewConfig().WithRegion(cfg.Region).WithCredentials(creds)
	config.Retryer = client.DefaultRetryer{
		NumMaxRetries:    5,
		MinRetryDelay:    500 * time.Millisecond,
		MaxRetryDelay:    10 * time.Second,
		MinThrottleDelay: 500 * time.Millisecond,
		MaxThrottleDelay: 10 * time.Second,
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	return sess, nil
}

func idWithRegion(region, id string) string {
	if id == "" {
		return ""
	}
	if arn.IsARN(id) {
		a, _ := arn.Parse(id)
		region = a.Region
		id = a.Resource
		parts := strings.Split(a.Resource, ":")
		if len(parts) > 1 {
			id = parts[1]
		}
	}
	return region + "/" + id
}

func tagsMatched(filters, tags map[string]string) bool {
	for tagName, desiredValue := range filters {
		value := tags[tagName]
		if matched, _ := filepath.Match(desiredValue, value); !matched {
			return false
		}
	}
	return true
}
