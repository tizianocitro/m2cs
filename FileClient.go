package m2cs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"m2cs/internal/loadbalancing"
	"m2cs/pkg/filestorage"
	"sync"
)

type FileClient struct {
	storages        []filestorage.FileStorage
	replicationMode ReplicationMode
	lbStrategy      LoadBalancingStrategy
	lb              loadbalancing.LoadBalancer
}

func NewFileClient(replicationMode ReplicationMode, loadBalacingStrategy LoadBalancingStrategy, storages ...filestorage.FileStorage) *FileClient {
	return &FileClient{
		storages:        storages,
		replicationMode: replicationMode,
		lbStrategy:      loadBalacingStrategy,
	}
}

// PutObject uploads an object to all main storages based on the replication mode.
// In ASYNC_REPLICATION mode, it attempts to write to one main storage and then fans out
// the write to other main storages in the background.
// In SYNC_REPLICATION mode, it writes to all main storages and collects errors.
func (f *FileClient) PutObject(ctx context.Context, storeBox, fileName string, reader io.Reader) error {
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
			return fmt.Errorf("[async] PutObject failed on all  main storages")
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

		return nil

	case SYNC_REPLICATION:
		var errs []error
		for _, storage := range mains {
			if err := storage.PutObject(ctx, storeBox, fileName, bytes.NewReader(buf)); err != nil {
				errs = append(errs, fmt.Errorf("[sync] PutObject failed on %T: %w", storage, err))
			}
		}
		if len(errs) == 0 {
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

func (f *FileClient) GetObject(ctx context.Context, storeBox, fileName string) (io.ReadCloser, error) {
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
		log.Printf("sono qui")
		var strategy loadbalancing.Strategy
		switch f.lbStrategy {
		case CLASSIC:
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

	return obj, nil

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
		return nil
	}

	if len(errs) == len(mainStorages) {
		return fmt.Errorf("RemoveObject failed on all main storages: %w", errors.Join(errs...))
	}

	return fmt.Errorf("RemoveObject partially failed on %d/%d storages: %w", len(errs), len(f.storages), errors.Join(errs...))
}

func toLB(storages []filestorage.FileStorage) []loadbalancing.Client {
	var clients []loadbalancing.Client
	for _, s := range storages {
		clients = append(clients, s)
	}
	return clients
}
