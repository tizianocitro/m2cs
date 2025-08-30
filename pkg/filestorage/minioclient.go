package filestorage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"io"
	common "m2cs/pkg"
	"m2cs/pkg/transform"
	"strings"
)

// MinioClient is a client for interacting with MinIO storage.
// It implements the common.FileStorage interface.
type MinioClient struct {
	client     *minio.Client
	properties common.ConnectionProperties
}

// NewMinioClient creates a MinioClient, which is a cu stom client from the m2cs package.
// This method initializes the custom client by wrapping an original MinIO client and
// adding connection properties. The resulting client can then be used within the
// context of the m2cs library.
func NewMinioClient(client *minio.Client, properties common.ConnectionProperties) (*MinioClient, error) {
	if client == nil {
		return nil, fmt.Errorf("failed to create MinIO client: client is nil")
	}

	_, err := client.ListBuckets(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MinIO: %w", err)
	}

	return &MinioClient{
		client:     client,
		properties: properties,
	}, nil
}

// GetClient returns the underlying MinIO client.
func (m *MinioClient) GetClient() *minio.Client {
	return m.client
}

// MakeBucket creates a new bucket in MinioClient.
func (m *MinioClient) MakeBucket(ctx context.Context, bucketName string) error {
	if m.client == nil {
		return fmt.Errorf("client is not initialized")
	}

	err := m.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return err
	}

	return nil
}

// ListBuckets lists all buckets in MinioClient.
func (m *MinioClient) ListBuckets(ctx context.Context) ([]string, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client is not initialized")
	}

	buckets, err := m.client.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	var bucketNames []string
	for _, bucket := range buckets {
		bucketNames = append(bucketNames, fmt.Sprintf("Name: %s, CreatedOn: %s", bucket.Name, bucket.CreationDate))
	}

	return bucketNames, nil
}

// RemoveBucket removes a bucket from MinioClient.
func (m *MinioClient) RemoveBucket(ctx context.Context, bucketName string) error {
	if m.client == nil {
		return fmt.Errorf("client is not initialized")
	}

	err := m.client.RemoveBucket(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to remove bucket: %w", err)
	}

	return nil
}

// GetObject retrieves an object from the specified bucket and file name in MinioClient.
func (m *MinioClient) GetObject(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error) {
	pipe, err := transform.Factory{}.BuildRPipelineDecryptDecompress(m.properties, m.properties.EncryptKey)
	if err != nil {
		return nil, fmt.Errorf("build read pipeline: %w", err)
	}

	object, err := m.client.GetObject(context.Background(), storeBox, fileName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get the object from MinIO client: %w", err)
	}

	obj, err := pipe.Apply(object)
	if err != nil {
		return nil, fmt.Errorf("fail to transform reader: %w", err)
	}

	return obj, nil
}

// PutObject uploads an object to the specified bucket and file name in MinioClient.
func (m *MinioClient) PutObject(ctx context.Context, storeBox string, fileName string, reader io.Reader) error {
	if reader == nil {
		return fmt.Errorf("reader is nil")
	}

	var size int64

	pipe, err := transform.Factory{}.BuildWPipelineCompressEncrypt(m.properties, m.properties.EncryptKey)
	if err != nil {
		return fmt.Errorf("build write pipeline: %w", err)
	}

	obj, closer, err := pipe.Apply(reader)
	if err != nil {
		return fmt.Errorf("apply write pipeline: %w", err)
	}

	if closer != nil {
		defer closer.Close()
	}

	obj, size, err = getSizeFromReader(obj)
	
	_, err = m.client.PutObject(ctx, storeBox, fileName, obj, size, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put the object into minio bucket: %w", err)
	}

	return nil
}

// RemoveObject removes an object from the specified bucket in MinioClient.
func (m *MinioClient) RemoveObject(ctx context.Context, storeBox string, fileName string) error {
	opts := minio.RemoveObjectOptions{}

	_, err := m.client.StatObject(context.Background(), storeBox, fileName, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove object from minio bucket: %w", err)
	}

	err = m.client.RemoveObject(context.Background(), storeBox, fileName, opts)
	if err != nil {
		return fmt.Errorf("failed to remove object from minio bucket: %w", err)
	}

	return nil
}

func (m *MinioClient) GetConnectionProperties() common.ConnectionProperties {
	return m.properties
}

// getSizeFromReader ensures that the reader has a known size.
// If the reader is seekable or supports Len(), it reuses it.
// Otherwise it materializes into memory and returns a *bytes.Reader.
func getSizeFromReader(r io.Reader) (io.Reader, int64, error) {
	switch v := r.(type) {
	case *bytes.Reader:
		return v, int64(v.Len()), nil
	case *bytes.Buffer:
		return v, int64(v.Len()), nil
	case *strings.Reader:
		return v, int64(v.Len()), nil
	}

	if seeker, ok := r.(io.Seeker); ok {

		cur, _ := seeker.Seek(0, io.SeekCurrent)

		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, 0, fmt.Errorf("getSizeFromReader: seek end: %w", err)
		}

		if _, err := seeker.Seek(cur, io.SeekStart); err != nil {
			return nil, 0, fmt.Errorf("getSizeFromReader: rewind: %w", err)
		}

		if cur != 0 {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return nil, 0, fmt.Errorf("getSizeFromReader: seek start: %w", err)
			}
		}
		return r, end, nil
	}

	buf, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, fmt.Errorf("getSizeFromReader: materialize: %w", err)
	}
	br := bytes.NewReader(buf)

	return br, int64(len(buf)), nil
}
