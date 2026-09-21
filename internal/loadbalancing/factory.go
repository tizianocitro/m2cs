package loadbalancing

import (
	"context"
	"fmt"
	"io"
	"time"
)

type Client interface {
	GetObject(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error)
}

type ClientGroup struct {
	Clients []Client
}

type LoadBalancer interface {
	Apply(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error)
}

// LatencyUpdater is implemented by LoadBalancers that support manual latency
// injection. FileClient.SetLatency uses this interface to forward updates to
// the active balancer without exposing the concrete balancer type.
type LatencyUpdater interface {
	SetLatency(flatIndex int, d time.Duration) error
}

type Strategy int

const (
	CLASSIC Strategy = iota
	ROUND_ROBIN
	LATENCY_BASED
)

type Factory struct {
}

func (Factory) NewLoadBalancer(strategy Strategy, groups []ClientGroup) (LoadBalancer, error) {
	switch strategy {
	case CLASSIC:
		loadBalancer := NewClassicLB(groups)
		return loadBalancer, nil
	case ROUND_ROBIN:
		loadBalancer := NewRoundRobinLB(groups)
		return loadBalancer, nil
	case LATENCY_BASED:
		return NewLatencyBasedLB(groups, nil), nil
	}

	return nil, fmt.Errorf("unsupported load balancing strategy: %v", strategy)
}
