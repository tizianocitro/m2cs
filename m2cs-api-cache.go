package m2cs

import (
	"time"

	"github.com/tizianocitro/m2cs/internal/caching"
)

type CacheOptions struct {
	Enabled            bool               // Indicates if caching is enabled (default: false)
	MaxSizeMB          int64              // Maximum size of the cache in megabytes (default: 1024)
	TTL                time.Duration      // Time-to-live for cache entries (default: 10 * time.Minute)
	MaxItems           int                // Maximum number of items in the cache (default: 5)
	ValidationStrategy ValidationStrategy // Strategy for validating cached items (default: No Validation)
}

type ValidationStrategy *caching.ValidationOptions

// NoValidationStrategy returns a strategy that performs no validation on cache entries.
// Validation is only performed when an item is retrieved from the cache; at read time
// the item's validity is checked.
func NoValidationStrategy() ValidationStrategy {
	return &caching.ValidationOptions{
		Strategy: caching.NO_VALIDATION,
	}
}

// SamplingValidationStrategy creates a strategy that, at regular intervals,
// randomly selects a percentage `samplingPercent` of the keys in the cache
// and checks whether they are still valid. Useful for lightweight checks on large caches
// without having to scan them completely.
func SamplingValidationStrategy(samplingPercent uint8, validationInterval time.Duration) ValidationStrategy {
	if samplingPercent > 100 {
		samplingPercent = 100
	}
	if samplingPercent <= 0 {
		samplingPercent = 10
	}

	if validationInterval <= 0 {
		validationInterval = 30 * time.Minute
	}
	return &caching.ValidationOptions{
		Strategy:           caching.SAMPLING_VALIDATION,
		SamplingPercent:    samplingPercent,
		ValidationInterval: validationInterval,
	}
}
