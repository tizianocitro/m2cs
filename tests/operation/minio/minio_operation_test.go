package minio_operation_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/tizianocitro/m2cs/pkg"
	"github.com/tizianocitro/m2cs/pkg/filestorage"
)

var (
	// MinIO
	minioContainer testcontainers.Container
	minioUser      = "m2csUser"
	minioPassword  = "m2csPassword"
	minioEndpoint  string
	minioClient    *minio.Client            // MinIO client
	testClient     *filestorage.MinioClient // M2CS custom client
)

// TestMain sets up the MinIO container as a test dependency.
// The container is started using Testcontainers with the necessary setup
// (environment variables, commands). Once the tests are run,
// the container is terminated to ensure proper cleanup.
func TestMain(m *testing.M) {
	ctx := context.Background()

	runAndPopulateMinIOContainer(ctx)
	defer func() {
		if err := testcontainers.TerminateContainer(minioContainer); err != nil {
			log.Printf("failed to terminate MinIO container: %s", err)
		}
	}()

	code := m.Run()
	os.Exit(code)
}

// TestMinioClient_NewMinioClient_ClientNil verifies that the NewMinioClient function
// returns an error when a nil client is passed. It ensures that the function handles
// this invalid input gracefully by returning an appropriate error message.
func TestMinioClient_NewMinioClient_ClientNil(t *testing.T) {
	client, err := filestorage.NewMinioClient(nil, common.ConnectionProperties{})

	require.Nil(t, client)
	require.Error(t, err, "expected error for nil client, got nil")
	assert.ErrorContains(t, err, "failed to create MinIO client: client is nil")
}

// TestMinioClient_NewMinioClient_Success verifies that NewMinioClient correctly returns a MinioClient,
// which is a wrapper around the original MinIO client. If the client is active and functional,
// it creates the wrapper with the appropriate properties, ensuring that the MinioClient
// (the wrapper) can be used properly with the m2cs components.
func TestMinioClient_NewMinioClient_Success(t *testing.T) {

	minioClient, err := minio.New(strings.TrimPrefix(minioEndpoint, "http://"), &minio.Options{
		Creds:  credentials.NewStaticV4(minioUser, minioPassword, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("failed to create MinIO client: %v", err)
	}

	client, err := filestorage.NewMinioClient(minioClient, common.ConnectionProperties{})

	require.NoError(t, err)
	assert.NotNil(t, client)

	find := false

	bucketList, err := client.ListBuckets(context.TODO())

	for _, bucket := range bucketList {
		if strings.Contains(bucket, "test-bucket") {
			find = true
			break
		}
	}

	require.True(t, find, "expected to find test-bucket in the list of buckets")
}

// TestMinioClient_MakeBucket_MinioError verifies that the MakeBucket method
// of the MinioClient wrapper correctly returns errors from the original MinIO client.
// This test uses the scenario where the bucket already exists.
func TestMinioClient_MakeBucket_MinioError(t *testing.T) {
	err := testClient.MakeBucket(context.TODO(), "test-bucket")

	require.Error(t, err, "expected error for MinIO error, got nil")
	assert.ErrorContains(t, err, "Your previous request to create the named bucket succeeded and you already own it.")
}

// TestMinioClient_MakeBucket_Success verifies that the MakeBucket method successfully creates
// a new bucket in MinIO, and that the bucket is subsequently detected as existing.
func TestMinioClient_MakeBucket_Success(t *testing.T) {
	err := testClient.MakeBucket(context.TODO(), "test-bucket-2")
	require.NoError(t, err, "expected no error for successful bucket creation, got error")

	test, err := minioClient.BucketExists(context.TODO(), "test-bucket-2")
	require.NoError(t, err, "expected no error for bucket existence check, got error")
	assert.True(t, test, "expected bucket to exist after creation")
}

// TestMinioClient_ListBuckets_Success_WithBuckets ensures that the ListBuckets method returns
// the list of bucket and verify if the test-bucket is present.
func TestMinioClient_ListBuckets_Success(t *testing.T) {
	bucketList, err := testClient.ListBuckets(context.TODO())

	require.NoError(t, err, "expected no error for successful bucket listing, got error")
	assert.NotEmpty(t, bucketList, "expected non-empty bucket list")
	assert.Contains(t, bucketList[0], "test-bucket", "expected test-bucket to be in the list of buckets")
}

// TestMinioClient_RemoveBucket_MinioError verifies that the RemoveBucket method
// of the MinioClient wrapper correctly returns errors from the original MinIO client.
// This test uses the scenario where the bucket does not exist.
func TestMinioClient_RemoveBucket_MinioError(t *testing.T) {
	err := testClient.RemoveBucket(context.TODO(), "non-existent-bucket")

	require.Error(t, err, "expected error for MinIO error, got nil")
	assert.ErrorContains(t, err, "failed to remove bucket: ")
}

// TestMinioClient_RemoveBucket_Success verifies that a bucket can be successfully removed,
// and that its non-existence can be confirmed afterwards.
func TestMinioClient_RemoveBucket_Success(t *testing.T) {
	minioClient.MakeBucket(context.TODO(), "test-remove-bucket", minio.MakeBucketOptions{})

	err := testClient.RemoveBucket(context.TODO(), "test-remove-bucket")
	require.NoError(t, err, "expected no error for successful bucket removal, got error")

	test, err := minioClient.BucketExists(context.TODO(), "test-remove-bucket")
	require.NoError(t, err, "expected no error for bucket existence check, got error")
	assert.False(t, test, "expected bucket to not exist after removal")
}

// TestMinioClient_GetObject_MinioError verifies that the GetObject method
// of the MinioClient wrapper correctly returns errors from the original MinIO client.
// This test uses the scenario where the bucket name provided is invalid for MinIO.
func TestMinioClient_GetObject_MinioError(t *testing.T) {
	reader, err := testClient.GetObject(context.TODO(), "", "object.txt")

	require.Error(t, err, "expected error for MinIO error, got nil")
	assert.ErrorContains(t, err, "failed to get the object from MinIO client:")
	assert.Nil(t, reader, "expected nil reader for non-existent bucket")
}

// TestMinioClient_GetObject_Success verifies that an existing object can be successfully retrieved
// from a known bucket, and that its content matches the expected data.
func TestMinioClient_GetObject_Success(t *testing.T) {
	reader, err := testClient.GetObject(context.TODO(), "test-bucket", "object.txt")

	require.NoError(t, err, "expected no error for successful object retrieval, got error")
	assert.NotNil(t, reader, "expected non-nil reader for existing object")

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

// TestMinioClient_PutObject_MinioError verifies that the PutObject method
// of the MinioClient wrapper correctly returns errors from the original MinIO client.
// This test uses the scenario where an attempt is made to insert an object into a non-existent bucket,
// causing MinIO to return an error.
func TestMinioClient_PutObject_MinioError(t *testing.T) {
	err := testClient.PutObject(context.TODO(), "non-existent-bucket", "object.txt", strings.NewReader("test"))

	require.Error(t, err, "expected error for MinIO error, got nil")
	assert.ErrorContains(t, err, "failed to put the object into minio bucket:")
}

// TestMinioClient_PutObject_Success verifies that a valid object can be uploaded to an existing bucket.
func TestMinioClient_PutObject_Success(t *testing.T) {
	err := testClient.PutObject(context.TODO(), "test-bucket", "put-test.txt", strings.NewReader("put-test"))
	require.NoError(t, err, "expected no error for successful object upload, got error")

	reader, err := minioClient.GetObject(context.TODO(), "test-bucket", "put-test.txt", minio.GetObjectOptions{})
	require.NoError(t, err, "expected no error for successful object retrieval, got error")

	buf := make([]byte, int64(len("put-test")))
	for {
		_, err := reader.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
	}

	assert.Contains(t, string(buf), "put-test", "expected object content to be 'put-test'")
}

// TestMinioClient_PutObject_MinioError verifies that the PutObject method
// of the MinioClient wrapper correctly returns errors from the original MinIO client.
// This test uses the scenario where an attempt is made to insert an object into a non-existent bucket,
// causing MinIO to return an error.
func TestMinioClient_RemoveObject_MinioError(t *testing.T) {
	err := testClient.RemoveObject(context.TODO(), "non-existent-bucket", "object.txt")

	require.Error(t, err, "expected error for MinIO error, got nil")
	assert.ErrorContains(t, err, "failed to remove object from minio bucket:")
}

// TestMinioClient_RemoveObject_Success checks that an existing object can be successfully removed.
func TestMinioClient_RemoveObject_Success(t *testing.T) {
	err := testClient.RemoveObject(context.TODO(), "test-bucket", "object.txt")
	require.NoError(t, err, "expected no error for successful object removal, got error")

	_, err = minioClient.StatObject(context.TODO(), "test-bucket", "object.txt", minio.StatObjectOptions{})
	log.Print(err.Error())
	require.ErrorContains(t, err, "The specified key does not exist.", "expected error for non-existent object, got nil")
}

// runAndPopulateMinIOContainer starts the MinIO container and populates it with a test bucket.
// The bucket created in this function is used to test methods where an actual connection is made,
// to see if the connections can find the bucket.
func runAndPopulateMinIOContainer(ctx context.Context) {
	req := testcontainers.ContainerRequest{
		Image: "minio/minio:latest",
		Env: map[string]string{
			"MINIO_ROOT_USER":     "m2csUser",
			"MINIO_ROOT_PASSWORD": "m2csPassword",
		},
		Cmd: []string{"server", "/data", "--console-address", ":9001"},
	}

	var err error
	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("Error while starting the MinIO container: %s", err)
	}

	minioEndpoint, err = minioContainer.Endpoint(ctx, "http")
	if err != nil {
		log.Fatalf("failed to get minio endpoint: %s", err)
	}

	minioClient, err = minio.New(strings.Replace(minioEndpoint, "http://", "", 1), &minio.Options{
		Creds:  credentials.NewStaticV4(minioUser, minioPassword, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("failed to create MinIO client: %s", err.Error())
	}

	err = minioClient.MakeBucket(ctx, "test-bucket", minio.MakeBucketOptions{})
	if err != nil {
		log.Fatalf("failed to create the minio bucket for test: %s\n", err)
	}

	// insert a test object into the bucket
	_, err = minioClient.PutObject(ctx, "test-bucket", "object.txt", strings.NewReader("test"), int64(len("test")), minio.PutObjectOptions{})
	if err != nil {
		log.Fatalf("failed to put object into minio bucket: %s\n", err)
	}

	obj, err := minioClient.GetObject(ctx, "test-bucket", "object.txt", minio.GetObjectOptions{})
	if err != nil {
		log.Fatalf("failed to get object from minio bucket: %s\n", err)
	}
	buf := make([]byte, int64(len("test")))
	for {
		_, err := obj.Read(buf)
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

	testClient, err = filestorage.NewMinioClient(minioClient, common.ConnectionProperties{})
	if err != nil {
		log.Fatalf("failed to create MinIO client: %s", err.Error())
	}
}
