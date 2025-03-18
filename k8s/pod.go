package k8s

import (
	"fmt"
	"maps"
	"regexp"
	"slices"

	corev1 "k8s.io/api/core/v1"
)

var (
	deploymentPodRegex  = regexp.MustCompile(`([a-z0-9-]+)-[0-9a-f]{1,10}-[bcdfghjklmnpqrstvwxz2456789]{5}`)
	daemonsetPodRegex   = regexp.MustCompile(`([a-z0-9-]+)-[bcdfghjklmnpqrstvwxz2456789]{5}`)
	statefulsetPodRegex = regexp.MustCompile(`([a-z0-9-]+)-\d+`)
)

type PodId struct {
	Namespace string
	Name      string
	NodeName  string
}

type Pod struct {
	// update Pod.Equal when adding fields
	Id          PodId
	Phase       corev1.PodPhase
	IP          string
	Annotations map[string]string
	Ports       []int
}

func podFromObj(obj any) *Pod {
	pod := obj.(*corev1.Pod)
	res := &Pod{
		Id: PodId{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			NodeName:  pod.Spec.NodeName,
		},
		Phase:       pod.Status.Phase,
		IP:          pod.Status.PodIP,
		Annotations: pod.Annotations,
	}

	for _, c := range pod.Spec.Containers {
		for _, p := range c.Ports {
			res.Ports = append(res.Ports, int(p.ContainerPort))
		}
	}

	return res
}

func (p *Pod) Running() bool {
	return p.Phase == corev1.PodRunning
}

func (p *Pod) Equal(other *Pod) bool {
	return p.Id == other.Id &&
		p.Phase == other.Phase &&
		p.IP == other.IP &&
		maps.Equal(p.Annotations, other.Annotations) &&
		slices.Equal(p.Ports, other.Ports)
}

func (p *Pod) ServiceName() string {
	for _, r := range []*regexp.Regexp{deploymentPodRegex, daemonsetPodRegex, statefulsetPodRegex} {
		if g := r.FindStringSubmatch(p.Id.Name); len(g) == 2 {
			return fmt.Sprintf("/k8s/%s/%s", p.Id.Namespace, g[1])
		}
	}
	return ""
}
