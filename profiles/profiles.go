package profiles

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/coroot/coroot-cluster-agent/clickhouse"
	"github.com/google/pprof/profile"
	"k8s.io/klog"
)

const (
	goCPUProfileSeconds = 10
)

type Config struct {
	TTLDays int `yaml:"ttl_days"`
	Scrape  struct {
		Interval time.Duration `yaml:"interval"`
		Timeout  time.Duration `yaml:"timeout"`
	}
}

type Profiles struct {
	cfg        Config
	clickhouse *clickhouse.Client
	httpClient *http.Client
	userAgent  string

	targets     map[string]ScrapeTarget
	targetsLock sync.Mutex

	prevCache     map[ProfileKey]map[uint64]int64
	prevCacheLock sync.Mutex
}

func NewProfiles(cfg Config, cl *clickhouse.Client, userAgent string) (*Profiles, error) {
	ps := &Profiles{
		cfg:        cfg,
		clickhouse: cl,
		httpClient: &http.Client{},
		userAgent:  userAgent,
		prevCache:  map[ProfileKey]map[uint64]int64{},
		targets:    map[string]ScrapeTarget{},
	}
	err := ps.createTables()
	if err != nil {
		return nil, err
	}

	targetsCh := make(chan ScrapeTarget)
	go ps.registerScrapeTargets(targetsCh)
	err = k8sDiscovery(targetsCh)
	if err != nil {
		close(targetsCh)
		return nil, err
	}

	go ps.scrapeLoop()

	return ps, nil
}

func (ps *Profiles) Handler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	var profileSource Source
	var serviceName string
	labels := Labels{}
	for k, vs := range query {
		if len(vs) != 1 {
			continue
		}
		v := vs[0]
		if k == "profile.source" {
			profileSource = Source(v)
			continue
		}
		if k == "service.name" {
			serviceName = v
			continue
		}
		labels[k] = v
	}
	if profileSource == "" {
		klog.Errorln("profile.source is empty")
		http.Error(w, "profile.source is empty", 400)
		return
	}
	if serviceName == "" {
		klog.Errorln("service.name is empty")
		http.Error(w, "service.name is empty", 400)
		return
	}
	p, err := profile.Parse(r.Body)
	if err != nil {
		klog.Errorln(err)
		http.Error(w, err.Error(), 400)
		return
	}
	err = ps.save(r.Context(), serviceName, labels, profileSource, "", p)
	if err != nil {
		klog.Errorln(err)
		http.Error(w, err.Error(), 500)
		return
	}
}

func (ps *Profiles) registerScrapeTargets(ch <-chan ScrapeTarget) {
	for target := range ch {
		ps.targetsLock.Lock()
		if target.ServiceName == "" {
			t, ok := ps.targets[target.Address]
			if ok {
				labelsHash := t.Labels.Hash()
				ps.prevCacheLock.Lock()
				for key := range ps.prevCache {
					if t.ServiceName == key.ServiceName && labelsHash == key.LabelsHash {
						delete(ps.prevCache, key)
					}
				}
				ps.prevCacheLock.Unlock()
				klog.Infoln("removing target:", target.Address, target.ServiceName, target.Labels)
				delete(ps.targets, target.Address)
			}
		} else {
			klog.Infoln("new target:", target.Address, target.ServiceName, target.Labels)
			ps.targets[target.Address] = target
		}
		ps.targetsLock.Unlock()
	}
}

func (ps *Profiles) scrapeLoop() {
	if ps.cfg.Scrape.Interval == 0 {
		klog.Infoln("scrape interval is not set, disabling the scraper")
		return
	}
	klog.Infoln("scrape interval:", ps.cfg.Scrape.Interval)
	for {
		start := time.Now()
		var targets []ScrapeTarget
		ps.targetsLock.Lock()
		for _, t := range ps.targets {
			targets = append(targets, t)
		}
		ps.targetsLock.Unlock()
		var wg sync.WaitGroup
		for _, target := range targets {
			u, err := url.Parse("http://" + target.Address)
			if err != nil {
				klog.Errorln(err)
				continue
			}
			for _, profileType := range goProfileTypes {
				wg.Add(1)
				go func(st ScrapeTarget, pt string) {
					defer wg.Done()
					p, err := ps.scrape(pt, u)
					if err != nil {
						klog.Errorln("failed to scrape:", err)
						return
					}
					if p.DurationNanos == 0 {
						p.DurationNanos = ps.cfg.Scrape.Interval.Nanoseconds()
					}
					err = ps.save(context.TODO(), st.ServiceName, st.Labels, SourceGo, pt, p)
					if err != nil {
						klog.Errorln("failed to save:", err)
						return
					}
				}(target, profileType)
			}
		}
		wg.Wait()
		duration := time.Since(start)
		klog.Infof("scraped %d targets in %s", len(targets), duration.Truncate(time.Millisecond))
		time.Sleep(ps.cfg.Scrape.Interval - duration)
	}
}

func (ps *Profiles) scrape(profileType string, target *url.URL) (*profile.Profile, error) {
	u := target.JoinPath("/debug/pprof", profileType)
	timeout := ps.cfg.Scrape.Timeout
	if profileType == GoProfileProfile {
		timeout += time.Duration(goCPUProfileSeconds) * time.Second
		q := u.Query()
		q.Set("seconds", strconv.Itoa(goCPUProfileSeconds))
		u.RawQuery = q.Encode()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ps.userAgent)
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, string(data))
	}
	p, err := profile.Parse(resp.Body)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (ps *Profiles) save(ctx context.Context, serviceName string, labels Labels, source Source, profileType string, p *profile.Profile) error {
	end := time.Unix(0, p.TimeNanos)
	start := end.Add(-time.Duration(p.DurationNanos))

	colServiceName := new(proto.ColStr).LowCardinality()
	colType := new(proto.ColStr).LowCardinality()
	var colValue proto.ColInt64
	var colStackHash proto.ColUInt64
	colStart := new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
	colEnd := new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
	colLabels := proto.NewMap[string, string](new(proto.ColStr).LowCardinality(), new(proto.ColStr))
	colStack := new(proto.ColStr).Array()

	profileKey := ProfileKey{
		ServiceName: serviceName,
		LabelsHash:  labels.Hash(),
	}
	for i, st := range p.SampleType {
		typ := st.Type
		if profileType != "" {
			typ = profileType + "_" + typ
		}
		typ = fmt.Sprintf("%s:%s:%s", source, typ, st.Unit)
		cumulative := false
		switch profileType {
		case GoProfileProfile:
			switch st.Type {
			case "samples":
				continue
			}
		case GoProfileHeap:
			switch st.Type {
			case "alloc_objects", "alloc_space":
				cumulative = true
			}
		case GoProfileMutex, GoProfileBlock:
			cumulative = true
		}

		hasPrev := true
		if cumulative {
			profileKey.ProfileType = typ
			ps.prevCacheLock.Lock()
			if ps.prevCache[profileKey] == nil {
				ps.prevCache[profileKey] = map[uint64]int64{}
				hasPrev = false
			}
			ps.prevCacheLock.Unlock()
		}

		values := map[uint64]int64{}
		stacks := map[uint64]Stack{}
		for _, s := range p.Sample {
			var stack Stack
			for _, location := range s.Location {
				for _, line := range location.Line {
					if line.Function == nil {
						continue
					}
					l := fmt.Sprintf("%s %s:%d", line.Function.Name, line.Function.Filename, line.Line)
					stack = append(stack, l)
				}
			}
			stackHash := stack.Hash()
			values[stackHash] += s.Value[i]
			stacks[stackHash] = stack
		}

		if cumulative {
			ps.prevCacheLock.Lock()
		}
		for stackHash, value := range values {
			if cumulative {
				prev := ps.prevCache[profileKey][stackHash]
				ps.prevCache[profileKey][stackHash] = value
				if !hasPrev {
					continue
				}
				if value-prev >= 0 {
					value -= prev
				}
			}
			colServiceName.Append(serviceName)
			colType.Append(typ)
			colStart.Append(start)
			colEnd.Append(end)
			colLabels.Append(labels)
			colValue.Append(value)
			colStackHash.Append(stackHash)
			colStack.Append(stacks[stackHash])
		}
		if cumulative {
			ps.prevCacheLock.Unlock()
		}
	}

	input := proto.Input{
		{Name: "ServiceName", Data: colServiceName},
		{Name: "Hash", Data: colStackHash},
		{Name: "LastSeen", Data: colEnd},
		{Name: "Stack", Data: colStack},
	}
	err := ps.clickhouse.Do(ctx, ch.Query{Body: input.Into("profiling_stacks"), Input: input})
	if err != nil {
		return err
	}
	input = proto.Input{
		{Name: "ServiceName", Data: colServiceName},
		{Name: "Type", Data: colType},
		{Name: "Start", Data: colStart},
		{Name: "End", Data: colEnd},
		{Name: "Labels", Data: colLabels},
		{Name: "StackHash", Data: colStackHash},
		{Name: "Value", Data: colValue},
	}
	err = ps.clickhouse.Do(ctx, ch.Query{Body: input.Into("profiling_samples"), Input: input})
	if err != nil {
		return err
	}
	return nil
}
