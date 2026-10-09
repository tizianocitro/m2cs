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

// LocationSetter is implemented by LoadBalancers that support geographic
// routing. FileClient.SetBackendLocation and SetClientLocation use this
// interface to forward coordinate updates to the active balancer without
// exposing the concrete balancer type.
type LocationSetter interface {
	SetBackendLocation(flatIndex int, coords Coordinates) error
	SetClientLocation(coords Coordinates)
}

// P2CUpdater is implemented by the LEAST_LATENCY_P2C balancer. It allows
// FileClient to inject per-backend error margins without exposing the concrete type.
type P2CUpdater interface {
	SetErrorMargin(flatIndex int, margin float64) error
}

type Strategy int

const (
	CLASSIC Strategy = iota
	ROUND_ROBIN
	LATENCY_BASED
	GEOPROXIMITY
	LEAST_LATENCY_P2C
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
	case GEOPROXIMITY:
		return NewGeoproximityLB(groups, nil, nil), nil
	case LEAST_LATENCY_P2C:
		return NewLeastLatencyP2CLB(groups, nil, p2cDefaultErrorMargin), nil
	}

	return nil, fmt.Errorf("unsupported load balancing strategy: %v", strategy)
}
