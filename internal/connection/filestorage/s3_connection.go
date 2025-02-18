package connfilestorage

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"m2cs/internal/connection"
	"os"
)

// S3Connection represents a connection to the AWS S3 service.
type S3Connection struct {
	client *s3.Client
	connection.Properties
}

// GetClient returns the AWS S3 client.
func (m *S3Connection) GetClient() *s3.Client {
	return m.client
}

// CreateS3Connection creates a new S3Connection.
// It returns an S3Connection or an error if the connection could not be established.
func CreateS3Connection(endpoint string, config *connection.AuthConfig, awsRegion string) (*S3Connection, error) {
	if endpoint == "default" {
		endpoint = ""
	}

	if awsRegion == "" {
		awsRegion = "no-region"
	}

	conn := &S3Connection{}

	switch config.GetConnectType() {
	case "withCredential":
		if config.GetAccessKey() == "" || config.GetSecretKey() == "" {
			return nil, fmt.Errorf("access key and/or secret key not set")
		}

		staticProvider := credentials.NewStaticCredentialsProvider(
			config.GetAccessKey(),
			config.GetSecretKey(),
			"",
		)
		awsCfg, err := s3config.LoadDefaultConfig(context.TODO(),
			s3config.WithCredentialsProvider(staticProvider),
			s3config.WithRegion(awsRegion),
		)
		if err != nil {
			return nil, fmt.Errorf("cannot load the AWS configuration: %s", err)
		}

		if endpoint == "" {
			conn.client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
				o.UsePathStyle = true
			})
		} else {
			conn.client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String(endpoint)
			})
		}
	case "withEnv":
		accountName := os.Getenv("AWS_ACCESS_KEY_ID")
		accountKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if accountName == "" || accountKey == "" {
			return nil, fmt.Errorf("environment variables AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY are not set")
		}

		awsCfg, err := s3config.LoadDefaultConfig(context.TODO(),
			s3config.WithRegion(awsRegion),
		)
		if err != nil {
			return nil, fmt.Errorf("cannot load the AWS configuration: %s", err)
		}

		if endpoint == "" {
			conn.client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
				o.UsePathStyle = true
			})
		} else {
			conn.client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String(endpoint)
			})
		}
	default:
		return nil, fmt.Errorf("invalid connection type for AWS S3: %s", config.GetConnectType())
	}
	if conn.client == nil {
		return nil, fmt.Errorf("client is not initialized")
	}

	_, err := conn.client.ListBuckets(context.TODO(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to AWS S3: %w", err)
	}

	return conn, nil
}
