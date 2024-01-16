package profiles

import (
	"fmt"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	deploymentPodRegex  = regexp.MustCompile(`([a-z0-9-]+)-[0-9a-f]{1,10}-[bcdfghjklmnpqrstvwxz2456789]{5}`)
	daemonsetPodRegex   = regexp.MustCompile(`([a-z0-9-]+)-[bcdfghjklmnpqrstvwxz2456789]{5}`)
	statefulsetPodRegex = regexp.MustCompile(`([a-z0-9-]+)-\d+`)
)

type Pod corev1.Pod

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
	if p.Status.PodIP == "" || p.scrapePort() == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s", p.Status.PodIP, p.scrapePort())
}

func (p *Pod) serviceName() string {
	for _, r := range []*regexp.Regexp{deploymentPodRegex, daemonsetPodRegex, statefulsetPodRegex} {
		if g := r.FindStringSubmatch(p.Name); len(g) == 2 {
			return fmt.Sprintf("/k8s/%s/%s", p.Namespace, g[1])
		}
	}
	return ""
}

func (p *Pod) labels() Labels {
	return Labels{
		"namespace": p.Namespace,
		"pod":       p.Name,
		"node":      p.Spec.NodeName,
	}
}

func (p *Pod) addTarget() ScrapeTarget {
	return ScrapeTarget{
		Address:     p.scrapeAddress(),
		ServiceName: p.serviceName(),
		Labels:      p.labels(),
	}
}

func (p *Pod) delTarget() ScrapeTarget {
	return ScrapeTarget{
		Address:     p.scrapeAddress(),
		ServiceName: "", // means delete
	}
}

func k8sDiscovery(ch chan<- ScrapeTarget) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	watcher := cache.NewListWatchFromClient(client.CoreV1().RESTClient(), "pods", metav1.NamespaceAll, fields.Everything())
	_, informer := cache.NewInformer(watcher, &corev1.Pod{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := Pod(*obj.(*corev1.Pod))
			if pod.scrapeOn() && pod.scrapeAddress() != "" && pod.Status.Phase == corev1.PodRunning {
				ch <- pod.addTarget()
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod := Pod(*oldObj.(*corev1.Pod))
			newPod := Pod(*newObj.(*corev1.Pod))
			if (oldPod.scrapeOn() && !newPod.scrapeOn()) || (oldPod.scrapePort() != "" && newPod.scrapePort() == "") {
				ch <- oldPod.delTarget()
			}
			if newPod.scrapeOn() && newPod.scrapeAddress() != "" && newPod.Status.Phase == corev1.PodRunning {
				ch <- newPod.addTarget()
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod := Pod(*obj.(*corev1.Pod))
			if pod.scrapeOn() {
				ch <- pod.delTarget()
			}
		},
	})
	go informer.Run(make(chan struct{}))
	return nil
}
