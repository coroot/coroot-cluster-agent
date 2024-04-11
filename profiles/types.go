package profiles

import (
	"hash/fnv"
	"sort"
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
