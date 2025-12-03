package caching

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type SamplingValidation struct {
	SampleRate uint8 // Percentage of cache entries to validate (0-100)
}

func (sv *SamplingValidation) Apply(cache *FileCache) error {
	if cache == nil {
		return fmt.Errorf("cache is nil")
	}

	if cache.Options.TTL <= 0 {
		return fmt.Errorf("cache TTL must be greater than zero for sampling validation")
	}
	rate := sv.SampleRate
	if rate > 100 {
		rate = 100
	}
	if rate <= 0 {
		return nil
	}

	cache.mu.Lock()
	n := len(cache.File)
	if n == 0 {
		cache.mu.Unlock()
		return nil
	}

	type entry struct {
		key      string
		createAt time.Time
	}
	entries := make([]entry, 0, n)
	ttl := cache.Options.TTL
	for k, fi := range cache.File {
		if fi != nil {
			entries = append(entries, entry{key: k, createAt: fi.createAt})
		} else {
			entries = append(entries, entry{key: k})
		}
	}
	cache.mu.Unlock()

	sampleCount := int(math.Ceil(float64(len(entries)) * float64(rate) / 100.0))
	if sampleCount == 0 {
		sampleCount = 1
	}
	if sampleCount > len(entries) {
		sampleCount = len(entries)
	}

	rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })

	now := time.Now()
	for i := 0; i < sampleCount; i++ {
		e := entries[i]
		if e.createAt.IsZero() {
			continue
		}
		if e.createAt.Add(ttl).Before(now) {
			// Lock only to verify current state and delete if still expired.
			cache.mu.Lock()
			if fi, ok := cache.File[e.key]; ok && fi != nil && fi.createAt.Equal(e.createAt) {
				if fi.createAt.Add(ttl).Before(time.Now()) {
					delete(cache.File, e.key)
				}
			}
			cache.mu.Unlock()
		}
	}
	return nil
}
