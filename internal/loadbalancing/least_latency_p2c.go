package loadbalancing

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const p2cWeightLoad = 0.1
const p2cDefaultErrorMargin = 1.0 // ms

// p2cEntry holds all runtime state for a single backend in the P2C pool.
type p2cEntry struct {
	// latency is the EMA of observed response times.
	// A zero value means the backend has not been observed yet.
	latency time.Duration

	// inFlight is the number of requests currently being served by this backend.
	inFlight int64

	// errorMargin is a per-backend additive noise term (in ms) that prevents the score
	// from being too sensitive to tiny differences.
	errorMargin float64

	unavailable bool
}

type leastLatencyP2CLB struct {
	mu sync.Mutex

	flat    []Client
	entries []p2cEntry
}

// NewLeastLatencyP2CLB creates a leastLatencyP2CLB from the given ClientGroups.
// initialLatencies is an optional map from flat-pool index to initial EMA latency.
// defaultErrorMargin is applied to all backends initially.
func NewLeastLatencyP2CLB(groups []ClientGroup, initialLatencies map[int]time.Duration, defaultErrorMargin float64) *leastLatencyP2CLB {
	var flat []Client
	for _, g := range groups {
		flat = append(flat, g.Clients...)
	}

	entries := make([]p2cEntry, len(flat))
	for i := range entries {
		entries[i].errorMargin = defaultErrorMargin
	}

	for idx, d := range initialLatencies {
		if idx >= 0 && idx < len(entries) {
			entries[idx].latency = d
		}
	}

	return &leastLatencyP2CLB{
		flat:    flat,
		entries: entries,
	}
}

// SetLatency sets the initial or manually-observed latency for the backend.
// Implements LatencyUpdater.
func (p *leastLatencyP2CLB) SetLatency(flatIndex int, d time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if flatIndex < 0 || flatIndex >= len(p.entries) {
		return fmt.Errorf("leastLatencyP2CLB: flatIndex %d out of range [0, %d)", flatIndex, len(p.entries))
	}
	p.entries[flatIndex].latency = d
	return nil
}

// SetErrorMargin sets the error margin (in ms) for the backend.
// Implements P2CUpdater.
func (p *leastLatencyP2CLB) SetErrorMargin(flatIndex int, margin float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if flatIndex < 0 || flatIndex >= len(p.entries) {
		return fmt.Errorf("leastLatencyP2CLB: flatIndex %d out of range [0, %d)", flatIndex, len(p.entries))
	}
	p.entries[flatIndex].errorMargin = margin
	return nil
}

func (p *leastLatencyP2CLB) applyEMA(idx int, measured time.Duration) {
	cur := p.entries[idx].latency
	if cur == 0 {
		p.entries[idx].latency = measured
		return
	}
	p.entries[idx].latency = time.Duration(
		emaAlpha*float64(measured) + (1-emaAlpha)*float64(cur),
	)
}

// score calculates the composite score for a given backend.
// Lower score is better.
func (p *leastLatencyP2CLB) score(idx int) float64 {
	latMs := float64(p.entries[idx].latency.Milliseconds())
	inFlight := atomic.LoadInt64(&p.entries[idx].inFlight)
	return latMs*(1.0+p2cWeightLoad*float64(inFlight)) + p.entries[idx].errorMargin
}

// pickTwo selects two distinct indices from the available slice uniformly at random.
// Must be called with p.mu held, and len(available) >= 2.
func (p *leastLatencyP2CLB) pickTwo(available []int) (int, int) {
	idx1 := rand.Intn(len(available))
	idx2 := rand.Intn(len(available) - 1)
	if idx2 >= idx1 {
		idx2++
	}
	return available[idx1], available[idx2]
}

// Apply picks two available candidates at random and tries the one with the better score.
// If it fails, it tries the other one as fallback.
func (p *leastLatencyP2CLB) Apply(ctx context.Context, storeBox, fileName string) (io.ReadCloser, error) {
	p.mu.Lock()
	if len(p.flat) == 0 {
		p.mu.Unlock()
		return nil, fmt.Errorf("leastLatencyP2CLB: no clients configured")
	}

	available := make([]int, 0, len(p.flat))
	for i := range p.flat {
		if !p.entries[i].unavailable {
			available = append(available, i)
		}
	}

	if len(available) == 0 {
		for i := range p.entries {
			p.entries[i].unavailable = false
		}
		available = make([]int, len(p.flat))
		for i := range p.flat {
			available[i] = i
		}
	}

	var candidates []int
	if len(available) == 1 {
		candidates = []int{available[0]}
	} else {
		idxA, idxB := p.pickTwo(available)
		scoreA := p.score(idxA)
		scoreB := p.score(idxB)
		if scoreA <= scoreB {
			candidates = []int{idxA, idxB} // Try A first, then B
		} else {
			candidates = []int{idxB, idxA} // Try B first, then A
		}
	}
	p.mu.Unlock()

	var errs []error
	for _, idx := range candidates {
		atomic.AddInt64(&p.entries[idx].inFlight, 1)

		start := time.Now()
		obj, err := p.flat[idx].GetObject(ctx, storeBox, fileName)
		elapsed := time.Since(start)

		atomic.AddInt64(&p.entries[idx].inFlight, -1)

		p.mu.Lock()
		if err != nil {
			p.entries[idx].unavailable = true
			p.mu.Unlock()
			errs = append(errs, fmt.Errorf("backend[%d]: %w", idx, err))
			continue // Try the second candidate (if present)
		}

		p.entries[idx].unavailable = false
		p.applyEMA(idx, elapsed)
		p.mu.Unlock()
		return obj, nil
	}

	return nil, fmt.Errorf("leastLatencyP2CLB: candidates failed: %v", errs)
}
