package discovery

import (
	"errors"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
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
	Curr *Pod
	Prev *Pod
}

type K8S struct {
	client *kubernetes.Clientset
	pods   map[PodId]*Pod
	lock   sync.Mutex
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
		pods:   map[PodId]*Pod{},
		stopCh: make(chan struct{}),
	}

	return k8s, nil
}

func (k8s *K8S) Start() {
	if len(k8s.subscribers) == 0 {
		return
	}
	go k8s.start()
}

func (k8s *K8S) Stop() {
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
			k8s.lock.Lock()
			defer k8s.lock.Unlock()
			k8s.pods[pod.Id] = pod
			k8s.sendEvent(PodEvent{Type: PodEventTypeAdd, Curr: pod})
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := podFromObj(newObj)
			prev := podFromObj(oldObj)
			if !pod.Running() || pod.Equal(prev) {
				return
			}
			k8s.lock.Lock()
			defer k8s.lock.Unlock()
			k8s.pods[pod.Id] = pod
			k8s.sendEvent(PodEvent{Type: PodEventTypeChange, Curr: pod, Prev: prev})
		},
		DeleteFunc: func(obj interface{}) {
			pod := podFromObj(obj)
			k8s.lock.Lock()
			defer k8s.lock.Unlock()
			delete(k8s.pods, pod.Id)
			k8s.sendEvent(PodEvent{Type: PodEventTypeDelete, Curr: pod})
		},
	})

	go informer.Run(k8s.stopCh)
}

func (k8s *K8S) Subscribe(l PodEventsListener) {
	ch := make(chan PodEvent)
	l.ListenPodEvents(ch)
	k8s.subscribers = append(k8s.subscribers, ch)
}

func (k8s *K8S) sendEvent(e PodEvent) {
	for _, s := range k8s.subscribers {
		s <- e
	}
}
