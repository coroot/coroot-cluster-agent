package k8s

import (
	"context"
	"errors"

	"github.com/coroot/coroot-cluster-agent/flags"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
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
	if k8s == nil {
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
	factory := informers.NewSharedInformerFactory(k8s.client, 0)

	if !*flags.CollectKubernetesEvents {
		klog.Infoln("events collector disabled")
	} else {
		events := factory.Core().V1().Events().Informer()
		events.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
			if apierrors.IsForbidden(err) {
				klog.Errorln("Cannot watch events: access forbidden. Update Coroot Operator to proceed.")
				return
			}
			cache.DefaultWatchErrorHandler(context.TODO(), r, err)
		})
		eventsLogger := NewEventsLogger()
		startTime := metav1.Now()
		events.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				event := obj.(*corev1.Event)
				if !event.LastTimestamp.IsZero() && event.LastTimestamp.Before(&startTime) {
					return
				}
				eventsLogger.EmitEvent(event)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				old := oldObj.(*corev1.Event)
				event := newObj.(*corev1.Event)
				if event.Count > old.Count {
					eventsLogger.EmitEvent(event)
				}
			},
		})
	}

	if len(k8s.subscribers) > 0 {
		pods := factory.Core().V1().Pods().Informer()
		pods.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := podFromObj(obj)
				if !pod.Running() {
					return
				}
				k8s.sendPodEvent(PodEvent{Type: PodEventTypeAdd, Pod: pod})
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				pod := podFromObj(newObj)
				old := podFromObj(oldObj)
				if !pod.Running() || pod.Equal(old) {
					return
				}
				k8s.sendPodEvent(PodEvent{Type: PodEventTypeChange, Pod: pod, Old: old})
			},
			DeleteFunc: func(obj interface{}) {
				pod := podFromObj(obj)
				k8s.sendPodEvent(PodEvent{Type: PodEventTypeDelete, Pod: pod})
			},
		})
	}

	factory.Start(k8s.stopCh)
	factory.WaitForCacheSync(k8s.stopCh)
}

func (k8s *K8S) SubscribeForPodEvents(l PodEventsListener) {
	if k8s == nil {
		return
	}
	ch := make(chan PodEvent)
	l.ListenPodEvents(ch)
	k8s.subscribers = append(k8s.subscribers, ch)
}

func (k8s *K8S) sendPodEvent(e PodEvent) {
	for _, s := range k8s.subscribers {
		s <- e
	}
}
