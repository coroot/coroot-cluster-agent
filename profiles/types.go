package profiles

import (
	"cmp"
	"fmt"
	"hash/fnv"
	"net"
	"sort"

	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/logger"
	"golang.org/x/exp/maps"
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

type Target struct {
	Address     string
	ServiceName string
	Labels      Labels

	Description string

	logger logger.Logger
}

func (t *Target) Equal(other *Target) bool {
	return t.Address == other.Address && t.ServiceName == other.ServiceName && maps.Equal(t.Labels, other.Labels)
}

func (t *Target) String() string {
	return fmt.Sprintf("%s (%s)", t.Address, t.Description)
}

func TargetFromPod(pod *k8s.Pod) *Target {
	if pod == nil || pod.Annotations == nil {
		return nil
	}

	var t *Target

	if cmp.Or(pod.Annotations["coroot.com/profile-scrape"], pod.Annotations["pyroscope.io/scrape"]) == "true" {
		port := cmp.Or(pod.Annotations["coroot.com/profile-port"], pod.Annotations["pyroscope.io/port"])
		if port != "" {
			t = &Target{
				Address:     net.JoinHostPort(pod.IP, port),
				ServiceName: pod.ServiceName(),
				Labels: Labels{
					"namespace": pod.Id.Namespace,
					"pod":       pod.Id.Name,
					"node":      pod.Id.NodeName,
				},
			}
		}
	}

	if t != nil {
		t.Description = fmt.Sprintf("ns=%s, pod=%s, node=%s", pod.Id.Namespace, pod.Id.Name, pod.Id.NodeName)
		t.logger = logger.NewKlog(t.String())
	}

	return t
}
