package loadbalancing

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// emaAlpha is the smoothing factor for the Exponential Moving Average (EMA).
// A value of 0.2 gives more weight to historical observations, filtering
// transient spikes while still tracking long-term latency trends.
const emaAlpha = 0.2

// latencyEntry holds the observed latency and the availability status for a
// single backend in the flat client pool.
type latencyEntry struct {
	latency     time.Duration
	unavailable bool
}

// latencyBasedLB is a LoadBalancer that routes every request to the backend
// with the lowest observed latency.
// Latency tracking uses an Exponential Moving Average (EMA) with α = 0.2,
// updated automatically after each successful Apply call.
// Thread-safe: Apply and SetLatency may be called concurrently.
type latencyBasedLB struct {
	mu sync.Mutex

	// flat is the ordered list of all clients, built once at construction time.
	// The position of a client in this slice is its "flat index", which is the
	// same value accepted by SetLatency.
	flat []Client

	// latencies[i] tracks the observed latency and availability of flat[i].
	latencies []latencyEntry
}

func NewLatencyBasedLB(groups []ClientGroup, initialLatencies map[int]time.Duration) *latencyBasedLB {
	var flat []Client
	for _, g := range groups {
		flat = append(flat, g.Clients...)
	}

	entries := make([]latencyEntry, len(flat))
	for idx, d := range initialLatencies {
		if idx >= 0 && idx < len(entries) {
			entries[idx].latency = d
		}
	}

	return &latencyBasedLB{
		flat:      flat,
		latencies: entries,
	}
}

func (l *latencyBasedLB) SetLatency(flatIndex int, d time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if flatIndex < 0 || flatIndex >= len(l.latencies) {
		return fmt.Errorf("latencyBasedLB: flatIndex %d out of range [0, %d)", flatIndex, len(l.latencies))
	}
	l.latencies[flatIndex].latency = d
	return nil
}

// Apply selects the available backend with the lowest observed latency, invokes
// GetObject on it, and updates the EMA for that backend.
// If all backends are simultaneously unavailable, their flags are cleared and
// selection restarts from the full pool, preventing a permanent deadlock.
func (l *latencyBasedLB) Apply(ctx context.Context, storeBox, fileName string) (io.ReadCloser, error) {
	l.mu.Lock()
	if len(l.flat) == 0 {
		l.mu.Unlock()
		return nil, fmt.Errorf("latencyBasedLB: no clients configured")
	}
	// Build the sorted candidate list while holding the lock, then release
	// before making any network calls to avoid blocking concurrent updates.
	order := l.buildOrder()
	l.mu.Unlock()

	var errs []error
	for _, idx := range order {
		start := time.Now()
		obj, err := l.flat[idx].GetObject(ctx, storeBox, fileName)
		elapsed := time.Since(start)

		l.mu.Lock()
		if err != nil {
			l.latencies[idx].unavailable = true
			l.mu.Unlock()
			errs = append(errs, fmt.Errorf("backend[%d]: %w", idx, err))
			continue
		}
		// Successful call: clear any previous failure flag and update the EMA.
		l.latencies[idx].unavailable = false
		l.applyEMA(idx, elapsed)
		l.mu.Unlock()
		return obj, nil
	}

	return nil, fmt.Errorf("latencyBasedLB: all %d backends failed: %v", len(l.flat), errs)
}

// buildOrder returns the indices of currently available backends sorted by ascending latency.
// Must be called with l.mu held.
func (l *latencyBasedLB) buildOrder() []int {
	available := make([]int, 0, len(l.flat))
	for i := range l.flat {
		if !l.latencies[i].unavailable {
			available = append(available, i)
		}
	}

	// If every backend is unavailable, reset all flags and use the full pool.
	// This prevents a permanent deadlock where no backend can ever be selected.
	if len(available) == 0 {
		for i := range l.latencies {
			l.latencies[i].unavailable = false
		}
		available = make([]int, len(l.flat))
		for i := range l.flat {
			available[i] = i
		}
	}

	// Insertion sort by latency (ascending).
	for i := 1; i < len(available); i++ {
		for j := i; j > 0 && l.latencies[available[j]].latency < l.latencies[available[j-1]].latency; j-- {
			available[j], available[j-1] = available[j-1], available[j]
		}
	}
	return available
}

// applyEMA updates the Exponential Moving Average for the backend at idx.
//
// On the first observation (current latency == 0), the measured value is stored
// directly to avoid artificially deflating the estimate by blending with zero.
//
// EMA formula: new = alpha * measured + (1-alpha) * current, with alpha = emaAlpha.
//
// Must be called with l.mu held.
func (l *latencyBasedLB) applyEMA(idx int, measured time.Duration) {
	cur := l.latencies[idx].latency
	if cur == 0 {
		// First measurement: seed the EMA with the raw observation.
		l.latencies[idx].latency = measured
		return
	}
	l.latencies[idx].latency = time.Duration(
		emaAlpha*float64(measured) + (1-emaAlpha)*float64(cur),
	)
}
