package loadbalancing

import (
	"context"
	"fmt"
	"io"
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

type Strategy int

const (
	CLASSIC Strategy = iota
	ROUND_ROBIN
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
	}
	
	return nil, fmt.Errorf("unsupported load balancing strategy: %v", strategy)
}
