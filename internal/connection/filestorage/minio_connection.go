package connfilestorage

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"m2cs/internal/connection"
	common "m2cs/pkg"
	"m2cs/pkg/filestorage"
	"os"
	"strings"
)

// CreateMinioConnection creates a new MinioClient.
// It takes an endpoint, an AuthConfig, and optional MinIO options.
// It returns a MinioClient or an error if the connection could not be established.
func CreateMinioConnection(endpoint string, config *connection.AuthConfig, minioOptions *minio.Options) (*filestorage.MinioClient, error) {
	if minioOptions == nil {
		minioOptions = &minio.Options{
			Secure: false,
		}
	}

	if endpoint == "" || endpoint == "default" {
		endpoint = "localhost:9000"
	}

	if strings.Contains(endpoint, "http://") {
		endpoint = strings.Replace(endpoint, "http://", "", 1)
	} else if strings.Contains(endpoint, "https://") {
		endpoint = strings.Replace(endpoint, "https://", "", 1)
	}

	switch config.GetConnectType() {
	case "withCredential":
		if config.GetAccessKey() == "" || config.GetSecretKey() == "" {
			return nil, fmt.Errorf("access key and/or secret key not set")
		}
		minioOptions.Creds = credentials.NewStaticV4(config.GetAccessKey(), config.GetSecretKey(), "")
	case "withEnv":
		accessKey := os.Getenv("MINIO_ACCESS_KEY")
		secretKey := os.Getenv("MINIO_SECRET_KEY")
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("environment variables MINIO_ACCESS_KEY and/or MINIO_SECRET_KEY are not set")
		}
		minioOptions.Creds = credentials.NewStaticV4(accessKey, secretKey, "")

	default:
		return nil, fmt.Errorf("invalid connection type for MinIO: %s", config.GetConnectType())
	}

	minioClient, err := minio.New(endpoint, minioOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	_, err = minioClient.ListBuckets(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MinIO: %w", err)
	}

	conn, err := filestorage.NewMinioClient(minioClient, common.ConnectionProperties{
		IsMainInstance: config.GetProperties().IsMainInstance,
		SaveEncrypt:    config.GetProperties().SaveEncrypted,
		SaveCompress:   config.GetProperties().SaveCompressed,
		EncryptKey:     config.GetProperties().EncryptKey})

	return conn, nil
}
