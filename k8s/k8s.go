package k8s

import (
	"errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

var (
	ErrForbidden = errors.New("forbidden")
	ErrNotFound  = errors.New("not found")
)

type PodEventType int

const (
	PodEventTypeAdd PodEventType = iota
	PodEventTypeChange
	PodEventTypeDelete
)

type PodEventsListener interface {
	ListenPodEvents(events <-chan PodEvent)
}

type PodEvent struct {
	Type PodEventType
	Pod  *Pod
	Old  *Pod
}

type K8S struct {
	client *kubernetes.Clientset
	stopCh chan struct{}

	subscribers []chan<- PodEvent
}

func NewK8S() (*K8S, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		if errors.Is(err, rest.ErrNotInCluster) {
			klog.Infoln("not running inside a kubernetes cluster")
			return nil, nil
		}
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	k8s := &K8S{
		client: client,
		stopCh: make(chan struct{}),
	}

	return k8s, nil
}

func (k8s *K8S) Start() {
	if k8s == nil || len(k8s.subscribers) == 0 {
		return
	}
	go k8s.start()
}

func (k8s *K8S) Stop() {
	if k8s == nil {
		return
	}
	close(k8s.stopCh)
	for _, s := range k8s.subscribers {
		close(s)
	}
}

func (k8s *K8S) start() {
	watcher := cache.NewListWatchFromClient(k8s.client.CoreV1().RESTClient(), "pods", metav1.NamespaceAll, fields.Everything())
	_, informer := cache.NewInformer(watcher, &corev1.Pod{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := podFromObj(obj)
			if !pod.Running() {
				return
			}
			k8s.sendEvent(PodEvent{Type: PodEventTypeAdd, Pod: pod})
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := podFromObj(newObj)
			old := podFromObj(oldObj)
			if !pod.Running() || pod.Equal(old) {
				return
			}
			k8s.sendEvent(PodEvent{Type: PodEventTypeChange, Pod: pod, Old: old})
		},
		DeleteFunc: func(obj interface{}) {
			pod := podFromObj(obj)
			k8s.sendEvent(PodEvent{Type: PodEventTypeDelete, Pod: pod})
		},
	})

	go informer.Run(k8s.stopCh)
}

func (k8s *K8S) SubscribeForPodEvents(l PodEventsListener) {
	if k8s == nil {
		return
	}
	ch := make(chan PodEvent)
	l.ListenPodEvents(ch)
	k8s.subscribers = append(k8s.subscribers, ch)
}

func (k8s *K8S) sendEvent(e PodEvent) {
	for _, s := range k8s.subscribers {
		s <- e
	}
}
