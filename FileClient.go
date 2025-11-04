package m2cs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/tizianocitro/m2cs/internal/caching"
	"github.com/tizianocitro/m2cs/internal/loadbalancing"
	common "github.com/tizianocitro/m2cs/pkg"
	"github.com/tizianocitro/m2cs/pkg/filestorage"
)

type FileClient struct {
	storages        []filestorage.FileStorage
	replicationMode ReplicationMode
	lbStrategy      LoadBalancingStrategy
	lb              loadbalancing.LoadBalancer
	cache           *caching.FileCache
}

func NewFileClient(replicationMode ReplicationMode, loadBalacingStrategy LoadBalancingStrategy, storages ...filestorage.FileStorage) *FileClient {
	return &FileClient{
		storages:        storages,
		replicationMode: replicationMode,
		lbStrategy:      loadBalacingStrategy,
		cache:           nil,
	}
}

// PutObject uploads an object to all main storages based on the replication mode.
// In ASYNC_REPLICATION mode, it attempts to write to one main storage and then fans out
// the write to other main storages in the background.
// In SYNC_REPLICATION mode, it writes to all main storages and collects errors.
func (f *FileClient) PutObject(ctx context.Context, storeBox, fileName string, reader io.Reader) error {
	if reader == nil {
		return fmt.Errorf("reader is nil")
	}

	buf, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read input stream: %w", err)
	}

	var mains []filestorage.FileStorage
	for _, s := range f.storages {
		if s.GetConnectionProperties().IsMainInstance {
			mains = append(mains, s)
		}
	}
	if len(mains) == 0 {
		return errors.New("no main instance found for PutObject operation")
	}

	switch f.replicationMode {
	case ASYNC_REPLICATION:
		var oneSuccess = false

		for i, storage := range mains {
			err := storage.PutObject(ctx, storeBox, fileName, bytes.NewReader(buf))
			if err == nil {
				oneSuccess = true
				mains = append(mains[:i], mains[i+1:]...)
				break
			}
		}
		if !oneSuccess {
			return fmt.Errorf("[async] PutObject failed on all main storages")
		}

		for _, storage := range mains {
			s := storage
			go func() {
				localCtx := context.Background()
				if err := s.PutObject(localCtx, storeBox, fileName, bytes.NewReader(buf)); err != nil {
					log.Printf("[async] PutObject failed on %T: %v", s, err)
				}
			}()
		}

		if f.cache != nil && f.cache.Enabled() {
			f.cache.Invalidate(storeBox + "/" + fileName)
		}

		return nil

	case SYNC_REPLICATION:
		var errs []error
		for _, storage := range mains {
			if err := storage.PutObject(ctx, storeBox, fileName, bytes.NewReader(buf)); err != nil {
				errs = append(errs, fmt.Errorf("[sync] PutObject failed on %T: %w", storage, err))
			}
		}
		if len(errs) == 0 {
			if f.cache != nil && f.cache.Enabled() {
				f.cache.Invalidate(storeBox + "/" + fileName)
			}
			return nil
		}
		if len(errs) == len(mains) {
			return fmt.Errorf("[sync] PutObject failed on all %d storages: %w", len(mains), errors.Join(errs...))
		}
		return fmt.Errorf("[sync] PutObject partially failed on %d/%d storages: %w", len(errs), len(mains), errors.Join(errs...))

	default:
		return fmt.Errorf("unsupported replication mode: %v", f.replicationMode)
	}
}

// GetObject retrieves an object using the configured load balancing strategy.
func (f *FileClient) GetObject(ctx context.Context, storeBox, fileName string) (io.ReadCloser, error) {
	if f.cache != nil && f.cache.Enabled() {
		data := f.cache.GetFile(storeBox + "/" + fileName)
		if data != nil {
			return data, nil
		}
	}

	var obj io.ReadCloser
	var mainStorages []filestorage.FileStorage
	var nonMainStorages []filestorage.FileStorage

	for _, storage := range f.storages {
		if storage.GetConnectionProperties().IsMainInstance {
			mainStorages = append(mainStorages, storage)
		} else {
			nonMainStorages = append(nonMainStorages, storage)
		}
	}

	var groups []loadbalancing.ClientGroup

	if len(nonMainStorages) > 0 {
		groups = append(groups, loadbalancing.ClientGroup{
			Clients: toLB(nonMainStorages),
		})
	}
	if len(mainStorages) > 0 {
		groups = append(groups, loadbalancing.ClientGroup{
			Clients: toLB(mainStorages),
		})
	}

	var err error

	if f.lb == nil {
		var strategy loadbalancing.Strategy
		switch f.lbStrategy {
		case READ_REPLICA_FIRST:
			strategy = loadbalancing.CLASSIC
		case ROUND_ROBIN:
			strategy = loadbalancing.ROUND_ROBIN
		default:
			return nil, fmt.Errorf("unsupported load balancing strategy: %v", f.lbStrategy)
		}

		f.lb, err = loadbalancing.Factory{}.NewLoadBalancer(strategy, groups)
		if err != nil {
			return nil, fmt.Errorf("failed to create load balancer: %w", err)
		}

	}

	obj, err = f.lb.Apply(ctx, storeBox, fileName)
	if err != nil {
		return nil, fmt.Errorf("FileClient GetObject error: %w", err)
	}

	var buf []byte
	buf, err = io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	if f.cache != nil && f.cache.Enabled() {
		f.cache.Store(storeBox+"/"+fileName, buf)
	}

	return io.NopCloser(bytes.NewReader(buf)), nil

}

// RemoveObject deletes an object from all main storages in parallel.
// Errors are collected across storages and aggregated:
//   - If all storages fail, the function returns a consolidated error.
//   - If some storages fail, a partial error is returned with details.
//   - If no errors occur, the function returns nil.
func (f *FileClient) RemoveObject(ctx context.Context, storeBox string, fileName string) error {
	var errs []error

	var mainStorages []filestorage.FileStorage

	for _, storage := range f.storages {
		if storage.GetConnectionProperties().IsMainInstance {
			mainStorages = append(mainStorages, storage)
		}
	}

	if len(mainStorages) == 0 {
		return errors.New("no main instance found for RemoveObject operation")
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, storage := range mainStorages {
		wg.Add(1)
		go func(s filestorage.FileStorage) {
			defer wg.Done()
			if err := s.RemoveObject(ctx, storeBox, fileName); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("RemoveObject failed on storage %T: %w", s, err))
				mu.Unlock()
			}
		}(storage)
	}

	wg.Wait()

	if len(errs) == 0 {
		if f.cache != nil && f.cache.Enabled() {
			f.cache.Invalidate(storeBox + "/" + fileName)
		}
		return nil
	}

	if len(errs) == len(mainStorages) {
		return fmt.Errorf("RemoveObject failed on all main storages: %w", errors.Join(errs...))
	}

	return fmt.Errorf("RemoveObject partially failed on %d/%d storages: %w", len(errs), len(f.storages), errors.Join(errs...))
}

// CacheOptions defines the configuration options for the file cache.
func (f *FileClient) ConfigureCache(options CacheOptions) error {
	if f == nil {
		return fmt.Errorf("file client is nil")
	}

	if options.MaxSizeMB <= 0 {
		options.MaxSizeMB = 1024
	}
	if options.TTL <= 0 {
		options.TTL = 10 * time.Minute
	}
	if options.MaxItems <= 0 {
		options.MaxItems = 5
	}

	if f.cache != nil {
		f.cache.StopValidationRoutine()
	}

	f.cache = &caching.FileCache{
		File: make(map[string]*caching.FileInformation),
		Options: caching.CacheOptions{
			Enabled:           options.Enabled,
			MaxSizeMB:         options.MaxSizeMB,
			TTL:               options.TTL,
			MaxItems:          options.MaxItems,
			ValidationOptions: options.ValidationStrategy,
		},
	}
	if f.cache.Options.Enabled {
		f.cache.StartValidationRoutine()
	}

	return nil
}

// EnableCache marks the cache as enabled and starts the validation routine
// if a validation strategy is configured.
func (f *FileClient) EnableCache() error {
	if f.cache == nil {
		return fmt.Errorf("cache is not configured; configure it before enabling")
	}
	if f.cache.Options.Enabled {
		return nil
	}

	f.cache.Options.Enabled = true

	// Start validation routine if a strategy is set
	if v := f.cache.Options.ValidationOptions; v != nil && v.Strategy != caching.NO_VALIDATION {
		_ = f.cache.StartValidationRoutine()
	}
	return nil
}

// DisableCache marks the cache as disabled and stops the validation routine.
// Safe to call multiple times.
func (f *FileClient) DisableCache() {
	if f.cache == nil {
		return
	}

	f.cache.StopValidationRoutine()
	f.cache.Options.Enabled = false
}

func (f *FileClient) ClearCache() {
	if f.cache != nil {
		f.cache.Clear()
	}
}

func toLB(storages []filestorage.FileStorage) []loadbalancing.Client {
	var clients []loadbalancing.Client
	for _, s := range storages {
		clients = append(clients, s)
	}
	return clients
}

// ReplicationMode defines the replication modes for file storage.
// SYNC_REPLICATION indicates that the replication is synchronous.
// ASYNC_REPLICATION indicates that the replication is asynchronous.
type ReplicationMode int

const (
	SYNC_REPLICATION ReplicationMode = iota
	ASYNC_REPLICATION
)

// Re-export types (type alias)
type CompressionAlgorithm = common.CompressionAlgorithm
type EncryptionAlgorithm = common.EncryptionAlgorithm

// Re-export constants
const (
	NO_COMPRESSION   = common.NO_COMPRESSION
	GZIP_COMPRESSION = common.GZIP_COMPRESSION

	NO_ENCRYPTION     = common.NO_ENCRYPTION
	AES256_ENCRYPTION = common.AES256_ENCRYPTION
)

type LoadBalancingStrategy int

const (
	READ_REPLICA_FIRST LoadBalancingStrategy = iota
	ROUND_ROBIN
)
