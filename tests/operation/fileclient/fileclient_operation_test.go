package fileclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/docker/go-connections/nat"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azurite"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"io"
	"log"
	"m2cs"
	common "m2cs/pkg"
	"m2cs/pkg/filestorage"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	// azurite variables
	azureBlobClient         *azblob.Client
	azuriteEndpoint         string
	azuriteConnectionString string

	// minio variables
	minioClient   *minio.Client
	minioEndpoint string
	minioUser     = "m2csUser"
	minioPassword = "m2csPassword"

	// s3 variables
	s3Client   *s3.Client
	s3Endpoint string

	// containers
	minioContainer   testcontainers.Container
	azuriteContainer *azurite.AzuriteContainer
	s3Container      *localstack.LocalStackContainer
)

// TestMain sets up the test environment by starting Azurite, MinIO, and LocalStack containers,
// populating them with test data, and terminating the containers after tests are done.
func TestMain(m *testing.M) {
	ctx := context.Background()

	runAndPopulateAzuriteContainer(ctx)
	runAndPopulateMinIOContainer(ctx)
	runAndPopulateS3Container(ctx)

	defer func() {
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			log.Printf("failed to terminate azurite container: %s", err)
		}
		if err := testcontainers.TerminateContainer(minioContainer); err != nil {
			log.Printf("failed to terminate minio container: %s", err)
		}
		if err := testcontainers.TerminateContainer(s3Container); err != nil {
			log.Printf("failed to terminate s3 container: %s", err)
		}
	}()

	code := m.Run()
	os.Exit(code)
}

// TestFileClient_PutSYNC_AllClientSuccess tests the PutObject method of the FileClient
// with SYNC replication mode, ensuring that the object is successfully stored
// in all configured storage client (MinIO, Azure Blob Storage, and S3).
func TestFileClient_PutSYNC_AllClientSuccess(t *testing.T) {
	ctx := context.Background()

	minioWrap, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		&minio.Options{},
	)
	if err != nil {
		t.Fatalf("failed to create minio wrapper: %v", err)
	}

	azWrap, _ := m2cs.NewAzBlobConnection(azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString(azuriteConnectionString),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		})

	s3Wrap, _ := m2cs.NewS3Connection(s3Endpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("m2csUser", "m2csPassword"),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		"")

	fileClient := m2cs.NewFileClient(m2cs.SYNC_REPLICATION, m2cs.CLASSIC, minioWrap, azWrap, s3Wrap)

	err = fileClient.PutObject(ctx, "test-box", "putTest", strings.NewReader("test"))
	assert.NoError(t, err, "PutObject should succeed on all clients")

	checkResult := checkObjectExistenceInClients(t, ctx, "test-box", "putTest", "test", minioWrap, azWrap, s3Wrap)
	assert.Equal(t, ExistsInAllWithCorrectContent, checkResult, "Object should exist in all clients with correct content")

}

// TestFileClient_PutSYNC_PartialtSuccess tests the PutObject method of the FileClient
// with SYNC replication mode, simulating a failure in one of the storage client (S3).
// It verifies that the object is successfully stored in the other client (MinIO and Azure Blob Storage)
// and that the error is properly reported.
func TestFileClient_PutSYNC_PartialtSuccess(t *testing.T) {
	ctx := context.Background()

	minioWrap, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		&minio.Options{},
	)
	if err != nil {
		t.Fatalf("failed to create minio wrapper: %v", err)
	}

	azWrap, _ := m2cs.NewAzBlobConnection(azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString(azuriteConnectionString),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		})

	s3Wrap, _ := m2cs.NewS3Connection(s3Endpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("m2csUser", "m2csPassword"),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		"")

	err = minioWrap.MakeBucket(ctx, "partial-fail-box")
	if err != nil {
		t.Fatalf("failed to create minio bucket for partial fail test: %v", err)
	}
	err = azWrap.CreateContainer(ctx, "partial-fail-box")
	if err != nil {
		t.Fatalf("failed to create azurite container for partial fail test: %v", err)
	}

	fileClient := m2cs.NewFileClient(m2cs.SYNC_REPLICATION, m2cs.CLASSIC, minioWrap, azWrap, s3Wrap)
	err = fileClient.PutObject(ctx, "partial-fail-box", "putTest", strings.NewReader("test"))
	assert.Error(t, err, "PutObject should fail because S3 client is not configured properly")
	assert.ErrorContains(t, err, "PutObject partially failed on 1/3 storages", "Error should indicate failure in 1/3 clients")

	checkResult := checkObjectExistenceInClients(t, ctx, "partial-fail-box", "putTest", "test", minioWrap, azWrap)
	assert.Equal(t, ExistsInAllWithCorrectContent, checkResult, "Object should exist in some clients")

	checkResult2 := checkObjectExistenceInClients(t, ctx, "partial-fail-box", "putTest", "test", s3Wrap)
	assert.Equal(t, DoesNotExistInAll, checkResult2, "Object should not exist in S3 client")
}

// TestFileClient_PutSYNC_AllClientFail tests the PutObject method of the FileClient
// with SYNC replication mode, simulating failures in all storage client
// by attempting to store an object in a non-existing bucket.
func TestFileClient_PutSYNC_AllClientFail(t *testing.T) {
	ctx := context.Background()

	minioWrap, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		&minio.Options{},
	)
	if err != nil {
		t.Fatalf("failed to create minio wrapper: %v", err)
	}

	azWrap, _ := m2cs.NewAzBlobConnection(azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString(azuriteConnectionString),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		})

	s3Wrap, _ := m2cs.NewS3Connection(s3Endpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("m2csUser", "m2csPassword"),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		"")

	fileClient := m2cs.NewFileClient(m2cs.SYNC_REPLICATION, m2cs.CLASSIC, minioWrap, azWrap, s3Wrap)
	err = fileClient.PutObject(ctx, "not-existing-box", "putTest", strings.NewReader("test"))
	assert.ErrorContains(t, err, "PutObject failed on all 3 storages", "PutObject should fail on all clients because the bucket does not exist")

}

// TestFileClient_PutSYNC_NoMainInstance tests the PutObject method of the FileClient
// with SYNC replication mode, ensuring that the operation fails when no storage client
// is configured as the main instance (write instance).
func TestFileClient_PutSYNC_NoMainInstance(t *testing.T) {
	ctx := context.Background()

	minioWrap, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.GZIP_COMPRESSION,
			IsMainInstance:   false,
		},
		&minio.Options{},
	)
	if err != nil {
		t.Fatalf("failed to create minio wrapper: %v", err)
	}

	azWrap, _ := m2cs.NewAzBlobConnection(azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString(azuriteConnectionString),
			SaveEncrypt:      m2cs.AES256_ENCRYPTION,
			EncryptKey:       "m2cs",
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   false,
		})

	fileClient := m2cs.NewFileClient(m2cs.SYNC_REPLICATION, m2cs.CLASSIC, minioWrap, azWrap)

	err = fileClient.PutObject(ctx, "test-box", "putTest", strings.NewReader("test"))
	assert.ErrorContains(t, err, "no main instance found for PutObject operation", "PutObject should fail because there is not at least one main instance")

	checkResult := checkObjectExistenceInClients(t, ctx, "test-box", "putTest", "test", minioWrap, azWrap)
	assert.Equal(t, DoesNotExistInAll, checkResult, "Object should not exist in any client")
}

// TestFileClient_Async_FirstSuccessThenFanOut tests the PutObject method of the FileClient
// with ASYNC replication mode, ensuring that the operation returns immediately after the first success,
// while the fan-out to other storage backends happens in the background.
func TestFileClient_Async_FirstSuccessThenFanOut(t *testing.T) {
	ctx := context.Background()

	fast, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		&minio.Options{Secure: false},
	)
	if err != nil {
		t.Fatalf("fast backend: %v", err)
	}

	err = fast.MakeBucket(ctx, "boxasyncfso")
	if err != nil {
		t.Fatalf("failed to create minio bucket for async first success then fan-out test: %v", err)
	}

	az, err := m2cs.NewAzBlobConnection(
		azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(azurite.AccountName, azurite.AccountKey),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
	)
	if err != nil {
		t.Fatalf("az backend: %v", err)
	}

	err = az.CreateContainer(ctx, "boxasyncfso")
	if err != nil {
		t.Fatalf("failed to create azurite container for async first success then fan-out test: %v", err)
	}

	s3w, err := m2cs.NewS3Connection(
		s3Endpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("m2csUser", "m2csPassword"),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		"",
	)
	if err != nil {
		t.Fatalf("s3 backend: %v", err)
	}

	err = s3w.CreateBucket(ctx, "boxasyncfso")
	if err != nil {
		t.Fatalf("failed to create s3 bucket for async first success then fan-out test: %v", err)
	}

	slow1 := slowClient{inner: az, delay: 1500 * time.Millisecond}
	slow2 := slowClient{inner: s3w, delay: 1500 * time.Millisecond}

	fileClient := m2cs.NewFileClient(m2cs.ASYNC_REPLICATION, m2cs.CLASSIC, fast, slow1, slow2)
	if fileClient == nil {
		t.Fatalf("Error in configuraiton test: fileClient is nil")
	}

	err = fileClient.PutObject(ctx, "boxasyncfso", "file", strings.NewReader("test first success then fan-out"))
	assert.NoError(t, err, "PutObject should succeed on fast client")

	time.Sleep(1500 * time.Millisecond)
	checkResult := checkObjectExistenceInClients(t, ctx, "boxasyncfso", "file", "test first success then fan-out", fast, az, s3w)
	assert.Equal(t, ExistsInAllWithCorrectContent, checkResult, "Object should exist in all clients with correct content")
}

// TestFileClient_Async_PartialSuccess tests the PutObject method of the FileClient
// with ASYNC replication mode, simulating a failure all client.
func TestFileClient_Async_AllFail(t *testing.T) {
	ctx := context.Background()

	minio, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		&minio.Options{Secure: false},
	)
	if err != nil {
		t.Fatalf("fail to create minio wrapper: %v", err)
	}

	az, err := m2cs.NewAzBlobConnection(
		azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(azurite.AccountName, azurite.AccountKey),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
	)
	if err != nil {
		t.Fatalf("failed to create azurite wrapper: %v", err)
	}

	s3, err := m2cs.NewS3Connection(
		s3Endpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("m2csUser", "m2csPassword"),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		"",
	)
	if err != nil {
		t.Fatalf("fail to create s3 wrapper: %v", err)
	}

	fileClient := m2cs.NewFileClient(m2cs.ASYNC_REPLICATION, m2cs.CLASSIC, minio, az, s3)
	if fileClient == nil {
		t.Fatalf("Error in configuraiton test: fileClient is nil")
	}

	err = fileClient.PutObject(ctx, "boxasyncaf", "file", strings.NewReader("test all fail"))
	assert.ErrorContains(t, err, "[async] PutObject failed on all main storages", "PutObject should fail on all clients because the bucket does not exist")
}

// TestFileClient_Sync_ZeroLenghtObject tests the PutObject method of the FileClient
// with SYNC replication mode, ensuring that zero-length objects are handled correctly
func TestFileClient_Sync_ZeroLenghtObject(t *testing.T) {
	ctx := context.Background()

	minioWrap, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		&minio.Options{},
	)
	if err != nil {
		t.Fatalf("failed to create minio wrapper: %v", err)
	}

	azWrap, err := m2cs.NewAzBlobConnection(azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString(azuriteConnectionString),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		})

	if err != nil {
		t.Fatalf("failed to create azurite wrapper: %v", err)
	}

	fileClient := m2cs.NewFileClient(m2cs.SYNC_REPLICATION, m2cs.CLASSIC, minioWrap, azWrap)

	err = fileClient.PutObject(ctx, "test-box", "putTest", bytes.NewReader(nil))
	assert.NoError(t, err, "PutObject should succeed on all clients")
}

// TestFileClient_Sync_BigSizeObject tests the PutObject method of the FileClient
// with SYNC replication mode, ensuring that large objects are handled correctly
func TestFileClient_Sync_BigSizeObject(t *testing.T) {
	ctx := context.Background()

	// create a large temporary file with known content and SHA-256 hash
	f, _ := os.CreateTemp(t.TempDir(), "payload-*.bin")
	defer f.Close()

	h := sha256.New()
	size := int64(32 * 1024 * 1024)
	chunk := make([]byte, 1024*1024)
	for off := int64(0); off < size; off += int64(len(chunk)) {
		for i := range chunk {
			chunk[i] = byte((int(off) + i) % 251)
		}
		_, _ = f.Write(chunk)
		_, _ = h.Write(chunk)
	}
	expectedHash := h.Sum(nil)
	_, _ = f.Seek(0, io.SeekStart)

	minioWrap, err := m2cs.NewMinIOConnection(
		minioEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
			SaveEncrypt:      m2cs.AES256_ENCRYPTION,
			EncryptKey:       "m2cs",
			SaveCompress:     m2cs.NO_COMPRESSION,
			IsMainInstance:   true,
		},
		&minio.Options{},
	)
	if err != nil {
		t.Fatalf("failed to create minio wrapper: %v", err)
	}

	azWrap, err := m2cs.NewAzBlobConnection(azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString(azuriteConnectionString),
			SaveCompress:     m2cs.GZIP_COMPRESSION,
			IsMainInstance:   true,
		})
	if err != nil {
		t.Fatalf("failed to create azurite wrapper: %v", err)
	}

	fileClient := m2cs.NewFileClient(m2cs.SYNC_REPLICATION, m2cs.CLASSIC, minioWrap, azWrap)

	err = fileClient.PutObject(ctx, "test-box", "putTest", f)
	assert.NoError(t, err, "PutObject should succeed on all clients")

	// verify that the object exists in both backends with correct size and hash
	clients := []filestorage.FileStorage{minioWrap, azWrap}

	for _, client := range clients {
		rc, err := client.GetObject(ctx, "test-box", "putTest")
		if err != nil {
			t.Fatalf("GetObject failed on %T: %v", clients, err)
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("ReadAll failed on %T: %v", clients, err)
		}

		gotHash := sha256.Sum256(data)
		assert.Equal(t, size, int64(len(data)),
			"size mismatch on %T: got=%d want=%d", clients, len(data), size)
		assert.Equal(t, expectedHash, gotHash[:],
			"hash mismatch on %T", clients)
	}
}

//==============================================================================
// Utility functions and structs for setting up test
//==============================================================================

// runAndPopulateAzuriteContainer create an Azurite container
// starts a connection to it, and populates it with a test bucket and object.
func runAndPopulateAzuriteContainer(ctx context.Context) {
	var err error

	azuriteContainer, err = azurite.Run(
		ctx,
		"mcr.microsoft.com/azure-storage/azurite:latest",
		azurite.WithInMemoryPersistence(64),
	)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
		return
	}

	azuriteEndpoint = fmt.Sprintf("%s/%s", azuriteContainer.MustServiceURL(ctx, azurite.BlobService), azurite.AccountName)
	azuriteConnectionString = fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;", azurite.AccountName, azurite.AccountKey, azuriteEndpoint)

	azureBlobClient, err = azblob.NewClientFromConnectionString(azuriteConnectionString, nil)
	if err != nil {
		log.Fatalf("failed to create the Azurite client: %s", err)
	}

	_, err = azureBlobClient.CreateContainer(context.TODO(), "test-box", nil)
	if err != nil {
		log.Fatalf("failed to create the azurite container for test: %s\n", err)
	}

	_, err = azureBlobClient.UploadStream(context.TODO(), "test-box", "test-get-object", strings.NewReader("test"), nil)
	if err != nil {
		log.Fatalf("failed to upload the test object: %s\n", err)
	}

	resp, err := azureBlobClient.DownloadStream(context.TODO(), "test-box", "test-get-object", nil)
	if err != nil {
		log.Fatalf("failed to download the test object: %s\n", err)
	}

	buf := new(strings.Builder)
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		log.Fatalf("failed to read the test object: %s\n", err)
	}

	if buf.String() != "test" {
		log.Fatalf("expected object content to be 'test', got '%s'", buf.String())
	}
}

// runAndPopulateMinIOContainer creates a MinIO container,
// starts a connection to it, and populates it with a test bucket and object.
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

	minioContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
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

	err = minioClient.MakeBucket(ctx, "test-box", minio.MakeBucketOptions{})
	if err != nil {
		log.Fatalf("failed to create the minio bucket for test: %s\n", err)
	}

	// insert a test object into the bucket
	_, err = minioClient.PutObject(ctx, "test-box", "object.txt", strings.NewReader("test"), int64(len("test")), minio.PutObjectOptions{})
	if err != nil {
		log.Fatalf("failed to put object into minio bucket: %s\n", err)
	}

	obj, err := minioClient.GetObject(ctx, "test-box", "object.txt", minio.GetObjectOptions{})
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
}

// runAndPopulateS3Container creates a LocalStack container with S3 service,
// starts a connection to it, and populates it with a test bucket and object.
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

	s3Endpoint = fmt.Sprintf("http://%s:%s", host, mappedPort.Port())

	s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
	})
	if s3Client == nil {
		log.Fatalf("failed to create S3 client: client is nil")
	}

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("test-box")})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) {
			log.Printf("You already own bucket %s.\n", "test-box")
			err = owned
		} else if errors.As(err, &exists) {
			log.Printf("Bucket %s already exists.\n", "test-box")
			err = exists
		}
	} else {
		err = s3.NewBucketExistsWaiter(s3Client).Wait(
			ctx, &s3.HeadBucketInput{Bucket: aws.String("test-box")}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for bucket %s to exist.\n", "test-box")
		}
	}

	// insert a test object into the bucket
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("test-box"),
		Key:    aws.String("object.txt"),
		Body:   strings.NewReader("test"),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			log.Printf("Error while uploading object to %s. The object is too large.\n"+
				"To upload objects larger than 5GB, use the S3 console (160GB max)\n"+
				"or the multipart upload API (5TB max).", "test-box")
		} else {
			log.Printf("Couldn't upload file %v to %v. Here's why: %v\n",
				"object.txt", "test-box", err)
		}
	} else {
		err = s3.NewObjectExistsWaiter(s3Client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String("test-box"), Key: aws.String("object.txt")}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist.\n", "object.txt")
		}
	}

	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-box"),
		Key:    aws.String("object.txt"),
	})
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Can't get object %s from bucket %s. No such key exists.\n", "object.txt", "test-box")
			err = noKey
		} else {
			log.Printf("Couldn't get object %v:%v. Here's why: %v\n", "test-box", "object.txt", err)
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
}

// checkObjectExistenceInClients checks if an object exists in multiple file storage clients
// and verifies its content against the expected value.
func checkObjectExistenceInClients(
	t *testing.T, ctx context.Context,
	storeBox string, fileName string, expect string,
	clients ...filestorage.FileStorage,
) ExistenceCheckResult {
	t.Helper()

	var finded = 0
	var total = len(clients)
	var correct = 0

	for _, client := range clients {
		rc, err := client.GetObject(ctx, storeBox, fileName)
		if err != nil {
			continue
		}
		finded++
		data, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			return UnknownError
		}
		if string(data) == expect {
			correct++
		}
	}

	if finded == 0 {
		return DoesNotExistInAll
	} else if finded == total && correct == total {
		return ExistsInAllWithCorrectContent
	} else if finded == total && correct != total {
		return ExistsInAllWithIncorrectContent
	} else if finded < total {
		return ExistsInSome
	}

	return UnknownError
}

type ExistenceCheckResult int

const (
	ExistsInAllWithCorrectContent ExistenceCheckResult = iota
	DoesNotExistInAll
	ExistsInAllWithIncorrectContent
	ExistsInSome
	UnknownError
)

// slowClient decorates a filestorage.FileStorage adding a delay on Operation.
// It is used to simulate slow clients in tests.
type slowClient struct {
	inner filestorage.FileStorage
	delay time.Duration
}

func (s slowClient) GetConnectionProperties() common.ConnectionProperties {
	return s.inner.GetConnectionProperties()
}

func (s slowClient) PutObject(ctx context.Context, storeBox, fileName string, r io.Reader) error {
	time.Sleep(s.delay)
	return s.inner.PutObject(ctx, storeBox, fileName, r)
}

func (s slowClient) GetObject(ctx context.Context, storeBox, fileName string) (io.ReadCloser, error) {
	time.Sleep(s.delay)
	return s.inner.GetObject(ctx, storeBox, fileName)
}

func (s slowClient) RemoveObject(ctx context.Context, storeBox, fileName string) error {
	time.Sleep(s.delay)
	return s.inner.RemoveObject(ctx, storeBox, fileName)
}
