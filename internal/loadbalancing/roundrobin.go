package loadbalancing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

type roundRobinLB struct {
	group         []ClientGroup
	currentClient int
	mu            sync.Mutex
}

func NewRoundRobinLB(group []ClientGroup) *roundRobinLB {
	return &roundRobinLB{group: group}
}

func (r *roundRobinLB) Apply(ctx context.Context, storeBox, fileName string) (io.ReadCloser, error) {
	if len(r.group) == 0 {
		return nil, fmt.Errorf("no client groups configured")
	}

	var primary []Client
	var errs []error

	if len(r.group[0].Clients) > 0 {
		r.mu.Lock()
		clientNum := len(r.group[0].Clients)
		start := r.currentClient % clientNum

		r.currentClient = (start + 1) % clientNum

		primary = make([]Client, 0, clientNum)
		for i := 0; i < clientNum; i++ {
			idx := (start + i) % clientNum
			primary = append(primary, r.group[0].Clients[idx])
		}
		r.mu.Unlock()

		for _, client := range primary {
			obj, err := client.GetObject(ctx, storeBox, fileName)
			if err == nil {
				return obj, nil
			}
			errs = append(errs, fmt.Errorf("group#0: %w", err))
		}
	}

	// --- fallback: other groups in classic balancing
	for gi, group := range r.group[1:] {
		for _, client := range group.Clients {
			obj, err := client.GetObject(ctx, storeBox, fileName)
			if err == nil {
				return obj, nil
			}
			errs = append(errs, fmt.Errorf("group#%d: %w", gi+1, err))
		}
	}

	if len(errs) == 0 {
		return nil, fmt.Errorf("no clients available")
	}
	return nil, errors.Join(errs...)
}
