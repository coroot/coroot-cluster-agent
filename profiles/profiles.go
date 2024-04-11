package profiles

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"k8s.io/klog"
)

const (
	goCPUProfileSeconds = 10
)

type Config struct {
	Endpoint string `yaml:"endpoint"`
	Scrape   struct {
		Interval time.Duration `yaml:"interval"`
		Timeout  time.Duration `yaml:"timeout"`
	}
}

type Profiles struct {
	cfg    Config
	apiKey string

	httpClient *http.Client

	targets     map[string]ScrapeTarget
	targetsLock sync.Mutex

	prevCache     map[ProfileKey]map[uint64]int64
	prevCacheLock sync.Mutex

	k8sDiscoveryStopCh chan struct{}
}

func NewProfiles(cfg Config, apiKey string) (*Profiles, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("empty endpoint")
	}
	_, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint: %s", err)
	}
	klog.Infoln("profiles endpoint:", cfg.Endpoint)

	ps := &Profiles{
		cfg:                cfg,
		apiKey:             apiKey,
		httpClient:         &http.Client{},
		prevCache:          map[ProfileKey]map[uint64]int64{},
		targets:            map[string]ScrapeTarget{},
		k8sDiscoveryStopCh: make(chan struct{}),
	}

	targetsCh := make(chan ScrapeTarget)
	go ps.registerScrapeTargets(targetsCh)
	err = k8sDiscovery(targetsCh, ps.k8sDiscoveryStopCh)
	if err != nil {
		close(targetsCh)
		return nil, err
	}

	go ps.scrapeLoop()

	return ps, nil
}

func (ps *Profiles) Close() {
	close(ps.k8sDiscoveryStopCh)
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
		for _, t := range targets {
			addr, err := url.Parse("http://" + t.Address)
			if err != nil {
				klog.Errorln(err)
				continue
			}
			for _, profileType := range goProfileTypes {
				wg.Add(1)
				go func(sn string, ls Labels, u *url.URL, pt string) {
					defer wg.Done()
					p, err := ps.scrape(pt, u)
					if err != nil {
						klog.Errorln("failed to scrape:", err)
						return
					}
					if len(p.Sample) == 0 {
						return
					}
					if p.DurationNanos == 0 {
						p.DurationNanos = ps.cfg.Scrape.Interval.Nanoseconds()
					}

					ps.diff(sn, ls, SourceGo, pt, p)

					err = ps.upload(sn, ls, p)
					if err != nil {
						klog.Errorln("failed to upload:", err)
						return
					}
				}(t.ServiceName, t.Labels, addr, profileType)
			}
		}
		wg.Wait()

		duration := time.Since(start)
		klog.Infof("scraped %d targets in %s", len(targets), duration.Truncate(time.Millisecond))
		time.Sleep(ps.cfg.Scrape.Interval - duration)
	}
}

func (ps *Profiles) scrape(profileType string, addr *url.URL) (*profile.Profile, error) {
	u := addr.JoinPath("/debug/pprof", profileType)
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
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, resp.Status)
	}
	p, err := profile.Parse(resp.Body)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (ps *Profiles) diff(serviceName string, labels Labels, source Source, profileType string, p *profile.Profile) {
	for i, st := range p.SampleType {
		cumulative := false
		switch profileType {
		case GoProfileProfile:
			switch st.Type {
			case "samples":
				st.Type = ""
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

		st.Type = fmt.Sprintf("%s:%s_%s:%s", source, profileType, st.Type, st.Unit)

		samples := map[uint64]*profile.Sample{}
		for _, s := range p.Sample {
			h := fnv.New64a()
			for _, location := range s.Location {
				for _, line := range location.Line {
					if line.Function == nil {
						continue
					}
					_, _ = h.Write([]byte(line.Function.Name))
					_, _ = h.Write([]byte(line.Function.Filename))
					_, _ = h.Write([]byte(fmt.Sprintf("%d", line.Line)))
				}
			}
			hash := h.Sum64()
			if samples[hash] == nil {
				samples[hash] = s
			} else {
				samples[hash].Value[i] += s.Value[i]
			}
		}

		if len(samples) < len(p.Sample) {
			p.Sample = p.Sample[:0]
			for _, s := range samples {
				p.Sample = append(p.Sample, s)
			}
			p.Compact()
		}

		if !cumulative {
			continue
		}

		hasPrev := true
		key := ProfileKey{
			ServiceName: serviceName,
			LabelsHash:  labels.Hash(),
			ProfileType: st.Type,
		}
		ps.prevCacheLock.Lock()
		if ps.prevCache[key] == nil {
			ps.prevCache[key] = map[uint64]int64{}
			hasPrev = false
		}
		for hash, s := range samples {
			value := s.Value[i]
			prev := ps.prevCache[key][hash]
			ps.prevCache[key][hash] = value
			if !hasPrev {
				continue
			}
			if value-prev >= 0 {
				value -= prev
			}
			s.Value[i] = value
		}
		ps.prevCacheLock.Unlock()
	}
}

func (ps *Profiles) upload(serviceName string, labels Labels, p *profile.Profile) error {
	u, err := url.Parse(ps.cfg.Endpoint)
	if err != nil {
		return err
	}
	q := u.Query()
	for k, v := range labels {
		q.Set(k, v)
	}
	q.Set("service.name", serviceName)
	u.RawQuery = q.Encode()

	buf := bytes.NewBuffer(nil)
	err = p.Write(buf)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), buf)
	if err != nil {
		return err
	}

	req.Header.Set("X-Api-Key", ps.apiKey)

	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
