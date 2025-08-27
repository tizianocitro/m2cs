package loadbalancing

import (
	"context"
	"fmt"
	"io"
)

type classicLB struct {
	group []ClientGroup
}

func NewClassicLB(group []ClientGroup) *classicLB {
	return &classicLB{group: group}
}

func (c *classicLB) Apply(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error) {
	if len(c.group) == 0 {
		return nil, fmt.Errorf("no clients available in the group")
	}

	for _, g := range c.group {
		for _, client := range g.Clients {
			obj, err := client.GetObject(ctx, storeBox, fileName)
			if err == nil {
				return obj, nil
			}
		}
	}

	return nil, fmt.Errorf("all clients failed to get the object")
}
