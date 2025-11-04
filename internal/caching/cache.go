package caching

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

type FileInformation struct {
	data     []byte
	createAt time.Time
}

type CacheOptions struct {
	Enabled           bool               // Indicates if caching is enabled (default: false)
	MaxSizeMB         int64              // Maximum size of the cache in megabytes (default: 1024)
	TTL               time.Duration      // Time-to-live for cache entries (default: 10 * time.Minute)
	MaxItems          int                // Maximum number of items in the cache (default: 5)
	ValidationOptions *ValidationOptions // Options for cache validation strategy

}

type FileCache struct {
	mu      sync.Mutex                  // Mutex to protect concurrent access
	File    map[string]*FileInformation // In-memory map to store cached files
	Options CacheOptions                // Cache configuration options

	// lifecycle validation routine
	valMu     sync.Mutex
	valCancel context.CancelFunc
	valWG     sync.WaitGroup
}

// Store adds a file to the cache.
func (s *FileCache) Store(fileName string, data []byte) {
	if !s.Enabled() {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	size := int64(len(data))
	if size > int64(s.Options.MaxSizeMB*1024*1024) {
		return
	}

	// If the file already exists, update its data and timestamp
	if _, exists := s.File[fileName]; exists {
		s.File[fileName].data = data
		s.File[fileName].createAt = time.Now()
		return
	}

	s.File[fileName] = &FileInformation{
		data:     data,
		createAt: time.Now(),
	}

	// If the cache exceeds the maximum number of items, remove the oldest item
	if len(s.File) > s.Options.MaxItems {
		var oldestFile string
		var oldestTime = time.Now()
		for name, file := range s.File {
			if file.createAt.Before(oldestTime) {
				oldestTime = file.createAt
				oldestFile = name
			}
		}
		delete(s.File, oldestFile)
	}
}

// GetFile retrieves a file from the cache.
// Returns nil if the file is not found or has expired.
// If has expired, it is removed from the cache.
func (s *FileCache) GetFile(fileName string) io.ReadCloser {
	if !s.Enabled() {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fileInfo, exists := s.File[fileName]
	if !exists {
		return nil
	}

	if fileInfo.createAt.Before(time.Now().Add(-s.Options.TTL)) {
		delete(s.File, fileName)
		return nil
	}

	return io.NopCloser(bytes.NewReader(fileInfo.data))
}

// Invalidate removes a file from the cache.
func (s *FileCache) Invalidate(fileName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.File, fileName)
}

// Clear removes all files from the cache.
func (s *FileCache) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.File = make(map[string]*FileInformation)
}

func (s *FileCache) Enabled() bool {
	return s != nil && s.Options.Enabled
}

// StartValidationRoutine starts the cache validation routine based on the configured strategy.
// If a validation routine is already running, it does nothing.
func (s *FileCache) StartValidationRoutine() error {
	if s == nil {
		return fmt.Errorf("cache is nil")
	}

	s.valMu.Lock()
	defer s.valMu.Unlock()

	if s.valCancel != nil {
		return nil
	}

	s.mu.Lock()
	v := s.Options.ValidationOptions
	enabled := s.Options.Enabled
	s.mu.Unlock()

	if !enabled || v == nil || v.Strategy == NO_VALIDATION {
		return nil
	}

	iv := v.ValidationInterval
	if iv <= 0 {
		iv = 30 * time.Minute
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.valCancel = cancel
	s.valWG.Add(1)

	go func(interval time.Duration) {
		defer s.valWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.mu.Lock()
				v := s.Options.ValidationOptions
				enabled := s.Options.Enabled
				curIV := interval
				if v != nil && v.ValidationInterval > 0 {
					curIV = v.ValidationInterval
				}
				stop := !enabled || v == nil || v.Strategy == NO_VALIDATION
				change := curIV != interval
				s.mu.Unlock()

				if stop || change {
					return
				}
				s.validateCache()

			case <-ctx.Done():
				return
			}
		}
	}(iv)

	return nil
}

// StopValidationRoutine stops the cache validation routine if it is running.
func (s *FileCache) StopValidationRoutine() {
	if s == nil {
		return
	}
	s.valMu.Lock()
	cancel := s.valCancel
	s.valCancel = nil
	s.valMu.Unlock()

	if cancel != nil {
		cancel()
		s.valWG.Wait()
	}
}

// validateCache performs cache validation based on the configured strategy.
func (s *FileCache) validateCache() error {
	s.mu.Lock()
	v := s.Options.ValidationOptions
	s.mu.Unlock()

	runner, err := ValidationStrategyFactory(v)
	if err != nil {
		return fmt.Errorf("failed to create validation strategy: %w", err)
	}
	return runner.Apply(s)
}

func ValidationStrategyFactory(v *ValidationOptions) (ValidationRunner, error) {
	if v == nil || v.Strategy == NO_VALIDATION {
		return nil, nil
	}

	switch v.Strategy {
	
	case SAMPLING_VALIDATION:
		return &SamplingValidation{SampleRate: v.SamplingPercent}, nil

	default:
		return nil, fmt.Errorf("unsupported validation strategy: %v", v.Strategy)
	}
}

func (s *FileCache) SetValidationOptions(v *ValidationOptions) {
	s.mu.Lock()
	s.Options.ValidationOptions = v
	s.mu.Unlock()

	s.StopValidationRoutine()
	_ = s.StartValidationRoutine()
}

type ValidationOptions struct {
	Strategy           Strategy
	SamplingPercent    uint8
	ValidationInterval time.Duration
}
type Strategy int

const (
	NO_VALIDATION Strategy = iota
	SAMPLING_VALIDATION
)

type ValidationRunner interface {
	Apply(cache *FileCache) error
}
