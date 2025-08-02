package s3

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	_ "github.com/testcontainers/testcontainers-go/wait"
	"io"
	"log"
	common "m2cs/pkg"
	"m2cs/pkg/filestorage"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	s3Container *localstack.LocalStackContainer
	s3User      = "m2csUser"
	s3Password  = "m2csPassword"
	s3Endpoint  string
	s3Client    *s3.Client            // S3 client
	testClient  *filestorage.S3Client // M2CS custom client
)

// TestMain sets up the LocalStack S3 container as a test dependency.
// The container is started using Testcontainers with the necessary setup
// (environment variables). Once the tests are run,
// the container is terminated to ensure proper cleanup.
func TestMain(m *testing.M) {
	ctx := context.Background()

	runAndPopulateS3Container(ctx)
	defer func() {
		if err := testcontainers.TerminateContainer(s3Container); err != nil {
			log.Printf("failed to terminate LocalStack S3 container: %s", err)
		}
	}()

	code := m.Run()
	os.Exit(code)
}

// TestS3Client_NewS3Client_ClientNil verifies that the NewS3Client function
// returns an error when a nil client is passed. It ensures that the function handles
// this invalid input gracefully by returning an appropriate error message.
func TestS3Client_NewS3Client_ClientNil(t *testing.T) {
	client, err := filestorage.NewS3Client(nil, common.ConnectionProperties{})

	require.Nil(t, client)
	require.Error(t, err, "expected error for nil client, got nil")
	assert.ErrorContains(t, err, "failed to create S3Client: client is nil")
}

// TestS3Client_NewS3Client_Success verifies that NewS3Client correctly returns a S3Client,
// which is a wrapper around the original S3 client. If the client is active and functional,
// it creates the wrapper with the appropriate properties, ensuring that the S3Client
// (the wrapper) can be used properly with the m2cs components.
func TestS3Client_NewS3Client_Success(t *testing.T) {
	client, err := filestorage.NewS3Client(s3Client, common.ConnectionProperties{})
	require.NoError(t, err)
	assert.NotNil(t, client)

	// Verify that the test bucket exists
	buckets, err := client.ListBuckets(context.TODO())
	require.NoError(t, err)

	found := false
	for _, b := range buckets {
		if strings.Contains(b, "test-bucket") {
			found = true
			break
		}
	}
	require.True(t, found, "expected to find 'test-bucket' in bucket list")
}

// TestS3Client_CreateBucket_S3Error verifies that the CreateBucket method
// of the S3Client wrapper correctly returns errors from the original S3 client.
// in this scenario, it simulates an S3 error by trying to create a bucket with
// an invalid name for S3 service.
func TestS3Client_CreateBucket_S3Error(t *testing.T) {
	err := testClient.CreateBucket(context.TODO(), "TestBucket")

	require.Error(t, err, "expected error for S3 error, got nil")
	assert.ErrorContains(t, err, "InvalidBucketName")
}

// TestS3Client_CreateBucket_Success verifies that the CreateBucket method
// of the S3Client wrapper correctly creates a bucket in the S3 service.
func TestS3Client_CreateBucket_Success(t *testing.T) {
	err := testClient.CreateBucket(context.TODO(), "test-bucket-2")
	require.NoError(t, err, "expected no error when creating bucket, got error")

	// Verify that the bucket was created
	bucket, err := s3Client.ListBuckets(context.TODO(), nil)
	require.NoError(t, err, "expected no error when listing buckets, got error")

	found := false

	for _, b := range bucket.Buckets {
		if *b.Name == "test-bucket-2" {
			found = true
			break
		}
	}

	require.True(t, found, "expected to find 'test-bucket-2' in bucket list")
}

// TestS3Client_ListBuckets_Success verifies that the ListBuckets method
// of the S3Client wrapper correctly lists buckets in the S3 service.
// It checks that the test bucket created in the setup phase is present in the list.
func TestS3Client_ListBuckets_Success(t *testing.T) {
	buckets, err := testClient.ListBuckets(context.TODO())
	require.NoError(t, err, "expected no error when listing buckets, got error")

	found := false
	for _, b := range buckets {
		if strings.Contains(b, "test-bucket") {
			found = true
			break
		}
	}

	require.True(t, found, "expected to find 'test-bucket' in bucket list")
}

// TestS3Client_RemoveBucket_S3Error verifies that the RemoveBucket method
// of the S3Client wrapper correctly returns errors from the original S3 client.
// In this scenario, it simulates an S3 error by trying to remove a not empty bucket.
func TestS3Client_RemoveBucket_S3Error(t *testing.T) {
	err := testClient.RemoveBucket(context.TODO(), "test-bucket")

	require.Error(t, err, "expected error for bucket not empty, got nil")
	assert.ErrorContains(t, err, "The bucket you tried to delete is not empty")
}

// TestS3Client_RemoveBucket_Success verifies that the RemoveBucket method
// of the S3Client wrapper correctly removes a bucket in the S3 service.
func TestS3Client_RemoveBucket_Success(t *testing.T) {
	// create a new bucket to remove
	_, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String("test-bucket-to-remove")})
	require.NoError(t, err, "expected no error when creating bucket, got error")

	err = testClient.RemoveBucket(context.TODO(), "test-bucket-to-remove")
	require.NoError(t, err, "expected no error when removing bucket, got error")

	// Verify that the bucket was removed
	buckets, err := s3Client.ListBuckets(context.TODO(), nil)
	require.NoError(t, err, "expected no error when listing buckets, got error")

	found := false
	for _, b := range buckets.Buckets {
		if *b.Name == "test-bucket-to-remove" {
			found = true
			break
		}
	}

	require.False(t, found, "expected 'test-bucket-to-remove' to be removed from bucket list")
}

// TestS3Client_GetObject_S3Error verifies that the GetObject method
// of the S3Client wrapper correctly returns errors from the original S3 client.
// This test uses the scenario where the bucket name provided does not exist in S3.
func TestS3Client_GetObject_S3Error(t *testing.T) {

	reader, err := testClient.GetObject(context.TODO(), "non-existent-bucket", "object.txt")

	require.Error(t, err, "expected error for S3 error, got nil")
	assert.ErrorContains(t, err, "NoSuchBucket")
	assert.Nil(t, reader, "expected nil reader for non-existent bucket")
}

// TestS3Client_GetObject_Success verifies that the GetObject method
// of the S3Client wrapper correctly retrieves an object from the S3 service.
func TestS3Client_GetObject_Success(t *testing.T) {

	reader, err := testClient.GetObject(context.TODO(), "test-bucket", "object.txt")
	require.NoError(t, err, "expected no error when getting object, got error")
	require.NotNil(t, reader, "expected non-nil reader for existing object")

	buf := make([]byte, int64(len("test")))
	for {
		_, err := reader.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
	}

	assert.Contains(t, string(buf), "test", "expected object content to be 'test'")
}

// TestS3Client_PutObject_S3Error verifies that the PutObject method
// of the S3Client wrapper correctly returns errors from the original S3 client.
// This test uses the scenario where the bucket name provided does not exist in S3.
func TestS3Client_PutObject_S3Error(t *testing.T) {

	err := testClient.PutObject(context.TODO(), "non-existent-bucket", "object.txt", strings.NewReader("test"))

	require.Error(t, err, "expected error for S3 error, got nil")
	assert.ErrorContains(t, err, "NoSuchBucket")
}

// TestS3Client_PutObject_Success verifies that the PutObject method
// of the S3Client wrapper correctly uploads an object to the S3 service.
func TestS3Client_PutObject_Success(t *testing.T) {

	err := testClient.PutObject(context.TODO(), "test-bucket", "object2.txt", strings.NewReader("test2"))
	require.NoError(t, err, "expected no error when putting object, got error")

	// Verify that the object was uploaded
	result, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("object2.txt"),
	})
	require.NoError(t, err, "expected no error when getting object, got error")

	buf := make([]byte, int64(len("test2")))
	for {
		_, err := result.Body.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
	}

	assert.Contains(t, string(buf), "test2", "expected object content to be 'test2'")
}

// TestS3Client_RemoveObject_S3Error verifies that the RemoveObject method
// of the S3Client wrapper correctly returns errors from the original S3 client.
// This test uses the scenario where the bucket name provided does not exist in S3.
func TestS3Client_RemoveObject_S3Error(t *testing.T) {

	err := testClient.RemoveObject(context.TODO(), "non-existent-bucket", "object.txt")

	require.Error(t, err, "expected error for S3 error, got nil")
	assert.ErrorContains(t, err, "NoSuchBucket")
}

// TestS3Client_RemoveObject_Success verifies that the RemoveObject method
// of the S3Client wrapper correctly removes an object from the S3 service.
func TestS3Client_RemoveObject_Success(t *testing.T) {

	err := testClient.RemoveObject(context.TODO(), "test-bucket", "object.txt")
	require.NoError(t, err, "expected no error when removing object, got error")

	// Verify that the object was removed
	_, err = s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("object.txt"),
	})
	require.ErrorContains(t, err, "NoSuchKey")
}

// runAndPopulateS3Container starts the S3 container and populates it with a test bucket.
// The bucket created in this function is used to test methods that require an actual connection,
// verifying that the connections can locate the bucket and that the object is uploaded correctly.
func runAndPopulateS3Container(ctx context.Context) {

	var err error

	s3Container, err = localstack.Run(ctx, "localstack/localstack:latest")

	mappedPort, err := s3Container.MappedPort(ctx, nat.Port("4566/tcp"))
	if err != nil {
		log.Fatalf("failed to retrieve mapped port: %s", err)
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		log.Fatalf("failed to create docker provider: %s", err)
	}
	defer provider.Close()

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		log.Fatalf("failed to retrieve daemon host: %s", err)
	}

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("no-region"),
	)
	if err != nil {
		log.Fatalf("failed to load AWS configuration: %s", err)
	}

	s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + host + ":" + mappedPort.Port())
		o.UsePathStyle = true
	})
	if s3Client == nil {
		log.Fatalf("failed to create S3 client: client is nil")
	}

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("test-bucket")})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) {
			log.Printf("You already own bucket %s.\n", "test-bucket")
			err = owned
		} else if errors.As(err, &exists) {
			log.Printf("Bucket %s already exists.\n", "test-bucket")
			err = exists
		}
	} else {
		err = s3.NewBucketExistsWaiter(s3Client).Wait(
			ctx, &s3.HeadBucketInput{Bucket: aws.String("test-bucket")}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for bucket %s to exist.\n", "test-bucket")
		}
	}

	// insert a test object into the bucket
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("object.txt"),
		Body:   strings.NewReader("test"),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			log.Printf("Error while uploading object to %s. The object is too large.\n"+
				"To upload objects larger than 5GB, use the S3 console (160GB max)\n"+
				"or the multipart upload API (5TB max).", "test-bucket")
		} else {
			log.Printf("Couldn't upload file %v to %v. Here's why: %v\n",
				"object.txt", "test-bucket", err)
		}
	} else {
		err = s3.NewObjectExistsWaiter(s3Client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String("test-bucket"), Key: aws.String("object.txt")}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist.\n", "object.txt")
		}
	}

	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("object.txt"),
	})
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Can't get object %s from bucket %s. No such key exists.\n", "object.txt", "test-bucket")
			err = noKey
		} else {
			log.Printf("Couldn't get object %v:%v. Here's why: %v\n", "test-bucket", "object.txt", err)
		}

	}

	buf := make([]byte, int64(len("test")))
	for {
		_, err := result.Body.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
	}
	if string(buf) != "test" {
		log.Fatalf("expected object content to be 'test', got '%s'", string(buf))
	}

	testClient, err = filestorage.NewS3Client(s3Client, common.ConnectionProperties{})
	if err != nil {
		log.Fatalf("failed to create MinIO client: %s", err.Error())
	}
}
