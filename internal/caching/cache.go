package caching

import (
	"bytes"
	"io"
	"sync"
	"time"
)

var (
	GlobalFileCache *FileCache
)

type FileInformation struct {
	data     []byte
	createAt time.Time
}

type CacheOptions struct {
	Enabled   bool          // Indicates if caching is enabled (default: false)
	MaxSizeMB int64         // Maximum size of the cache in megabytes (default: 1024)
	TTL       time.Duration // Time-to-live for cache entries (default: 10 * time.Minute)
	MaxItems  int           // Maximum number of items in the cache (default: 5)
}

type FileCache struct {
	mu      sync.Mutex                  // Mutex to protect concurrent access
	File    map[string]*FileInformation // In-memory map to store cached files
	Options CacheOptions                // Cache configuration options
}

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

func (s *FileCache) Invalidate(fileName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.File, fileName)
}

func (s *FileCache) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.File = make(map[string]*FileInformation)
}

func (s *FileCache) Enabled() bool {
	return s != nil && s.Options.Enabled
}
