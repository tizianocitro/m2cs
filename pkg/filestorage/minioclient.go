package filestorage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"io"
	common "m2cs/pkg"
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
	object, err := m.client.GetObject(context.Background(), storeBox, fileName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get the object from MinIO client: %w", err)
	}

	return object, nil
}

// PutObject uploads an object to the specified bucket and file name in MinioClient.
func (m *MinioClient) PutObject(ctx context.Context, storeBox string, fileName string, reader io.Reader) error {
	var size int64

	switch r := reader.(type) {
	case *bytes.Reader:
		size = int64(r.Len())
	case *strings.Reader:
		size = int64(r.Len())
	case *bytes.Buffer:
		size = int64(r.Len())
	default:
		size = getSizeFromReader(reader)
	}

	if size == 0 {
		return fmt.Errorf("failed to determine size of input reader (type: %T)", reader)
	}

	_, err := m.client.PutObject(ctx, storeBox, fileName, reader, size, minio.PutObjectOptions{})
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

func getSizeFromReader(reader io.Reader) int64 {
	seeker, ok := reader.(io.Seeker)
	if !ok {
		return 0
	}
	end, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return 0
	}
	_, err = seeker.Seek(0, io.SeekStart)
	if err != nil {
		return 0
	}
	return end
}
