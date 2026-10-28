package loadbalancing

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
)

// Coordinates represents a geographic position using the WGS-84 reference system.
// Lat must be in the range [-90, 90] and Lon in the range [-180, 180].
type Coordinates struct {
	Lat float64
	Lon float64
}

// geoEntry holds the geographic coordinates and availability status for a
// single backend in the flat client pool.
type geoEntry struct {
	// coords is the geographic position of this backend.
	coords    Coordinates
	hasCoords bool
	// unavailable is true when the most recent GetObject call returned an error.
	unavailable bool
}

// geoproximityLB is a LoadBalancer that routes every request to the backend
// geographically closest to the client's current position (distances are computed using the Haversine formula)

// All clients across all ClientGroups are merged into a single flat pool at
// construction time; group boundaries are intentionally ignored so the nearest
// backend is always preferred regardless of its role (replica vs. main).

type geoproximityLB struct {
	mu           sync.Mutex
	flat         []Client
	entries      []geoEntry
	clientLoc    Coordinates
	hasClientLoc bool
}

// All clients across all groups are merged into a single flat pool, preserving
// the order in which they appear (group 0 first, then group 1, etc.).

func NewGeoproximityLB(groups []ClientGroup, initialCoords map[int]Coordinates, clientLoc *Coordinates) *geoproximityLB {
	var flat []Client
	for _, g := range groups {
		flat = append(flat, g.Clients...)
	}

	entries := make([]geoEntry, len(flat))
	for idx, c := range initialCoords {
		if idx >= 0 && idx < len(entries) {
			entries[idx].coords = c
			entries[idx].hasCoords = true
		}
	}

	lb := &geoproximityLB{
		flat:    flat,
		entries: entries,
	}
	if clientLoc != nil {
		lb.clientLoc = *clientLoc
		lb.hasClientLoc = true
	}
	return lb
}

// SetBackendLocation assigns geographic coordinates to the backend at the given
// flat-pool index. This is the implementation point for the LocationSetter
// interface, called by FileClient.SetBackendLocation.
func (g *geoproximityLB) SetBackendLocation(flatIndex int, coords Coordinates) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if flatIndex < 0 || flatIndex >= len(g.entries) {
		return fmt.Errorf("geoproximityLB: flatIndex %d out of range [0, %d)", flatIndex, len(g.entries))
	}
	g.entries[flatIndex].coords = coords
	g.entries[flatIndex].hasCoords = true
	return nil
}

func (g *geoproximityLB) SetClientLocation(coords Coordinates) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.clientLoc = coords
	g.hasClientLoc = true
}

// Apply selects the available backend closest to the client's current position
// and invokes GetObject on it.
func (g *geoproximityLB) Apply(ctx context.Context, storeBox, fileName string) (io.ReadCloser, error) {
	g.mu.Lock()
	if len(g.flat) == 0 {
		g.mu.Unlock()
		return nil, fmt.Errorf("geoproximityLB: no clients configured")
	}
	// Build the sorted candidate list while holding the lock, then release
	// before making any network calls to avoid blocking concurrent updates.
	order := g.buildOrder()
	g.mu.Unlock()

	var errs []error
	for _, idx := range order {
		obj, err := g.flat[idx].GetObject(ctx, storeBox, fileName)

		g.mu.Lock()
		if err != nil {
			g.entries[idx].unavailable = true
			g.mu.Unlock()
			errs = append(errs, fmt.Errorf("backend[%d]: %w", idx, err))
			continue
		}
		// Successful call: clear any previous failure flag.
		g.entries[idx].unavailable = false
		g.mu.Unlock()
		return obj, nil
	}

	return nil, fmt.Errorf("geoproximityLB: all %d backends failed: %v", len(g.flat), errs)
}

// buildOrder returns the indices of currently available backends sorted by
// ascending distance from the client position.

func (g *geoproximityLB) buildOrder() []int {
	available := make([]int, 0, len(g.flat))
	for i := range g.flat {
		if !g.entries[i].unavailable {
			available = append(available, i)
		}
	}

	// If every backend is unavailable, reset all flags and use the full pool.
	// This prevents a permanent deadlock where no backend can ever be selected.
	if len(available) == 0 {
		for i := range g.entries {
			g.entries[i].unavailable = false
		}
		available = make([]int, len(g.flat))
		for i := range g.flat {
			available[i] = i
		}
	}

	// If the client position is unknown, preserve the original positional order.
	if !g.hasClientLoc {
		return available
	}

	// Precompute distances to avoid redundant Haversine evaluations during sort.
	// Backends without coordinates receive math.MaxFloat64 and sink to the back.
	dist := make([]float64, len(available))
	for i, idx := range available {
		if !g.entries[idx].hasCoords {
			dist[i] = math.MaxFloat64
		} else {
			dist[i] = haversineKm(g.clientLoc, g.entries[idx].coords)
		}
	}

	// Insertion sort by precomputed distance (ascending).
	for i := 1; i < len(available); i++ {
		for j := i; j > 0 && dist[j] < dist[j-1]; j-- {
			available[j], available[j-1] = available[j-1], available[j]
			dist[j], dist[j-1] = dist[j-1], dist[j]
		}
	}
	return available
}

// haversineKm returns the great-circle distance in kilometres between two
// geographic positions using the Haversine formula on a spherical Earth

func haversineKm(a, b Coordinates) float64 {
	const earthRadiusKm = 6371.0

	dLat := toRad(b.Lat - a.Lat)
	dLon := toRad(b.Lon - a.Lon)
	lat1 := toRad(a.Lat)
	lat2 := toRad(b.Lat)

	sinDLat := math.Sin(dLat / 2)
	sinDLon := math.Sin(dLon / 2)
	h := sinDLat*sinDLat + math.Cos(lat1)*math.Cos(lat2)*sinDLon*sinDLon

	return 2 * earthRadiusKm * math.Atan2(math.Sqrt(h), math.Sqrt(1-h))
}

// toRad converts decimal degrees to radians.
func toRad(deg float64) float64 {
	return deg * math.Pi / 180
}
