package k8s

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (k8s *K8S) GetNodeRegion(ctx context.Context) (string, error) {
	if k8s == nil {
		return "", nil
	}
	nodes, err := k8s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 10})
	if err != nil {
		return "", err
	}
	for _, n := range nodes.Items {
		for _, l := range []string{"topology.kubernetes.io/region", "failure-domain.beta.kubernetes.io/region"} {
			if r := n.Labels[l]; r != "" {
				return r, nil
			}
		}
	}
	return "", nil
}
