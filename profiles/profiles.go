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

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/discovery"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/google/pprof/profile"
	"k8s.io/klog"
)

const (
	goCPUProfileSeconds = 10
)

type Profiles struct {
	endpoint       *url.URL
	apiKey         string
	scrapeInterval time.Duration
	scrapeTimeout  time.Duration

	httpClient *http.Client

	targets     map[string]ScrapeTarget
	targetsLock sync.Mutex

	prevCache     map[ProfileKey]map[uint64]int64
	prevCacheLock sync.Mutex

	k8sPodEvents <-chan discovery.PodEvent
}

func NewProfiles() (*Profiles, error) {
	if *flags.ProfilesScrapeInterval == 0 {
		klog.Infoln("scrape interval is not set, disabling the scraper")
		return nil, nil
	}

	ps := &Profiles{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/profiles"),
		apiKey:         *flags.APIKey,
		scrapeInterval: *flags.ProfilesScrapeInterval,
		scrapeTimeout:  *flags.ProfilesScrapeTimeout,
		httpClient:     &http.Client{},
		prevCache:      map[ProfileKey]map[uint64]int64{},
		targets:        map[string]ScrapeTarget{},
	}

	klog.Infof("endpoint: %s, scrape interval: %s", ps.endpoint, ps.scrapeInterval)

	return ps, nil
}

func (ps *Profiles) ListenPodEvents(events <-chan discovery.PodEvent) {
	ps.k8sPodEvents = events
}

func (ps *Profiles) Start() {
	targetsCh := make(chan ScrapeTarget)
	go ps.registerScrapeTargets(targetsCh)
	go k8sDiscovery(ps.k8sPodEvents, targetsCh)
	go ps.scrapeLoop()
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
						p.DurationNanos = ps.scrapeInterval.Nanoseconds()
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
		time.Sleep(ps.scrapeInterval - duration)
	}
}

func (ps *Profiles) scrape(profileType string, addr *url.URL) (*profile.Profile, error) {
	u := addr.JoinPath("/debug/pprof", profileType)
	timeout := ps.scrapeTimeout
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
	u := *ps.endpoint
	q := u.Query()
	for k, v := range labels {
		q.Set(k, v)
	}
	q.Set("service.name", serviceName)
	u.RawQuery = q.Encode()

	buf := bytes.NewBuffer(nil)
	err := p.Write(buf)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), buf)
	if err != nil {
		return err
	}

	common.SetAuthHeaders(req, ps.apiKey)

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

func k8sDiscovery(events <-chan discovery.PodEvent, targets chan<- ScrapeTarget) {
	for e := range events {
		switch e.Type {
		case discovery.PodEventTypeAdd:
			pod := Pod{Pod: e.Curr}
			if pod.scrapeOn() && pod.scrapeAddress() != "" && pod.Running() {
				targets <- pod.addTarget()
			}
		case discovery.PodEventTypeChange:
			prevPod := Pod{Pod: e.Prev}
			currPod := Pod{Pod: e.Curr}
			if (prevPod.scrapeOn() && !currPod.scrapeOn()) || (prevPod.scrapePort() != "" && currPod.scrapePort() == "") {
				targets <- prevPod.delTarget()
			}
			if currPod.scrapeOn() && currPod.scrapeAddress() != "" && currPod.Running() {
				targets <- currPod.addTarget()
			}

		case discovery.PodEventTypeDelete:
			pod := Pod{Pod: e.Curr}
			if pod.scrapeOn() {
				targets <- pod.delTarget()
			}
		}
	}
}
