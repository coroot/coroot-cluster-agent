package profiles

import (
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/coroot/coroot-cluster-agent/discovery"
)

type Source string

const (
	SourceEbpf Source = "ebpf"
	SourceGo   Source = "go"

	GoProfileProfile   = "profile"
	GoProfileHeap      = "heap"
	GoProfileGoroutine = "goroutine"
	GoProfileMutex     = "mutex"
	GoProfileBlock     = "block"
)

var (
	goProfileTypes = []string{GoProfileProfile, GoProfileHeap, GoProfileGoroutine, GoProfileMutex, GoProfileBlock}
)

type ProfileKey struct {
	ServiceName string
	LabelsHash  uint64
	ProfileType string
}

type ScrapeTarget struct {
	Address     string
	ServiceName string
	Labels      Labels
}

type Labels map[string]string

func (ls Labels) Hash() uint64 {
	if len(ls) == 0 {
		return 0
	}
	keys := make([]string, 0, len(ls))
	for k := range ls {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte(ls[k]))
	}
	return h.Sum64()
}

type Pod struct {
	*discovery.Pod
}

func (p *Pod) scrapeOn() bool {
	return p.Annotations["coroot.com/profile-scrape"] == "true" || p.Annotations["pyroscope.io/scrape"] == "true"
}

func (p *Pod) scrapePort() string {
	if p.Annotations["coroot.com/profile-port"] != "" {
		return p.Annotations["coroot.com/profile-port"]
	}
	return p.Annotations["pyroscope.io/port"]
}

func (p *Pod) scrapeAddress() string {
	if p.IP == "" || p.scrapePort() == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s", p.IP, p.scrapePort())
}

func (p *Pod) addTarget() ScrapeTarget {
	return ScrapeTarget{
		Address:     p.scrapeAddress(),
		ServiceName: p.ServiceName(),
		Labels: Labels{
			"namespace": p.Id.Namespace,
			"pod":       p.Id.Name,
			"node":      p.Id.NodeName,
		},
	}
}

func (p *Pod) delTarget() ScrapeTarget {
	return ScrapeTarget{
		Address:     p.scrapeAddress(),
		ServiceName: "", // means delete
	}
}
