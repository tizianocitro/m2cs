package m2cs

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"m2cs/pkg/filestorage"
	"sync"
)

type FileClient struct {
	storages        []filestorage.FileStorage
	replicationMode ReplicationMode
}

func NewFileClient(replicationMode ReplicationMode, storages ...filestorage.FileStorage) *FileClient {
	return &FileClient{
		storages:        storages,
		replicationMode: replicationMode,
	}
}

func (f *FileClient) PutObject(ctx context.Context, storeBox string, fileName string, reader io.Reader) error {
	var mainStorages []filestorage.FileStorage

	for _, storage := range f.storages {
		if storage.GetConnectionProperties().IsMainInstance {
			mainStorages = append(mainStorages, storage)
		}
	}

	if len(mainStorages) == 0 {
		return errors.New("no main instance found for PutObject operation")
	}

	buf, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read input stream: %w", err)
	}

	var errs []error

	if f.replicationMode == SYNC_REPLICATION {
		for _, storage := range mainStorages {
			raw := bytes.NewReader(buf)
			var obj io.Reader = raw
			var toClose []io.Closer

			props := storage.GetConnectionProperties()

			// Optional compression
			if props.SaveCompress {
				compressed, err := compressReader(obj)
				if err != nil {
					errs = append(errs, fmt.Errorf("compression failed on storage %T: %w", storage, err))
					continue
				}
				toClose = append(toClose, compressed)
				obj = compressed
			}

			// Optional encryption
			if props.SaveEncrypt {
				encrypted, err := encryptReader("key", obj)
				if err != nil {
					errs = append(errs, fmt.Errorf("encryption failed on storage %T: %w", storage, err))
					continue
				}
				obj = encrypted
			}

			if err := storage.PutObject(ctx, storeBox, fileName, obj); err != nil {
				errs = append(errs, fmt.Errorf("PutObject failed on storage %T: %w", storage, err))
			}

			for _, closer := range toClose {
				_ = closer.Close()
			}
		}

		if len(errs) == 0 {
			return nil
		}

		if len(errs) == len(mainStorages) {
			return fmt.Errorf("PutObject failed on all storages: %w", errors.Join(errs...))
		}

		return fmt.Errorf("PutObject partially failed on %d/%d storages: %w", len(errs), len(mainStorages), errors.Join(errs...))
	}
	if f.replicationMode == ASYNC_REPLICATION {
		var successfulStorage filestorage.FileStorage
		var success bool

		// Try to put the object on the first available main storage
		for _, storage := range mainStorages {
			raw := bytes.NewReader(buf)
			var obj io.Reader = raw
			var toClose []io.Closer

			props := storage.GetConnectionProperties()

			// Optional compression
			if props.SaveCompress {
				compressed, err := compressReader(obj)
				if err != nil {
					continue
				}
				toClose = append(toClose, compressed)
				obj = compressed
			}

			// Optional encryption
			if props.SaveEncrypt {
				encrypted, err := encryptReader("key", obj)
				if err != nil {
					continue
				}
				obj = encrypted
			}

			err := storage.PutObject(ctx, storeBox, fileName, obj)
			if err != nil {
				continue
			}

			successfulStorage = storage
			success = true

			for _, c := range toClose {
				_ = c.Close()
			}
			break
		}

		if !success {
			return fmt.Errorf("PutObject failed on all main storages")
		}

		// Launch async operations on remaining storages
		for _, storage := range mainStorages {
			if storage == successfulStorage {
				continue
			}

			go func(s filestorage.FileStorage) {
				raw := bytes.NewReader(buf)
				var obj io.Reader = raw
				var toClose []io.Closer

				props := s.GetConnectionProperties()

				// Optional compression
				if props.SaveCompress {
					compressed, err := compressReader(obj)
					if err != nil {
						log.Printf("[async] compression failed on storage %T: %v", s, err)
						return
					}
					toClose = append(toClose, compressed)
					obj = compressed
				}

				// Optional encryption
				if props.SaveEncrypt {
					encrypted, err := encryptReader("key", obj)
					if err != nil {
						log.Printf("[async] encryption failed on storage %T: %v", s, err)
						return
					}
					obj = encrypted
				}

				localCtx := context.Background()

				if err := s.PutObject(localCtx, storeBox, fileName, obj); err != nil {
					log.Printf("[async] PutObject failed on %T: %v", s, err)
				}

				for _, c := range toClose {
					_ = c.Close()
				}
			}(storage)
		}
	}

	return nil
}

// RemoveObject deletes an object from all main storages in parallel.
// Errors are collected across storages and aggregated:
//  - If all storages fail, the function returns a consolidated error.
//  - If some storages fail, a partial error is returned with details.
//  - If no errors occur, the function returns nil.
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

func compressReader(input io.Reader) (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer gw.Close()

		_, err := io.Copy(gw, input)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("compression failed: %w", err))
		}
	}()

	return pr, nil
}

func decompressReader(input io.Reader) (io.ReadCloser, error) {
	gr, err := gzip.NewReader(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	return gr, nil
}

func encryptReader(key string, plaintext io.Reader) (io.Reader, error) {
	hashedKey := sha256.Sum256([]byte(key))

	block, err := aes.NewCipher(hashedKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	plainBytes, err := io.ReadAll(plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	ciphertext := aead.Seal(nil, nonce, plainBytes, nil)

	final := append(nonce, ciphertext...)
	return bytes.NewReader(final), nil
}

func decryptReader(key string, encrypted io.Reader) (io.Reader, error) {
	hashedKey := sha256.Sum256([]byte(key))

	block, err := aes.NewCipher(hashedKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	cipherBytes, err := io.ReadAll(encrypted)
	if err != nil {
		return nil, fmt.Errorf("failed to read encrypted input: %w", err)
	}

	if len(cipherBytes) < aead.NonceSize() {
		return nil, fmt.Errorf("invalid ciphertext: too short")
	}

	nonce := cipherBytes[:aead.NonceSize()]
	ciphertext := cipherBytes[aead.NonceSize():]

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	return bytes.NewReader(plaintext), nil
}
