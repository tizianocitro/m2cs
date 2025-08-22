package connection_test

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/docker/go-connections/nat"
	"github.com/minio/minio-go/v7"
	minioCredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azurite"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"log"
	"m2cs"
	"m2cs/internal/connection"
	"os"
	"strings"
	"testing"
)

var (
	// MinIO
	minioContainer testcontainers.Container
	minioUser      = "m2csUser"
	minioPassword  = "m2csPassword"
	minioEndpoint  string
	// S3
	localstackContainer testcontainers.Container
	localstackEndpoint  string
	awsRegion           = "us-east-1"
	// Azure Blob
	azuriteContainer        testcontainers.Container
	azuriteEndpoint         string
	azuriteConnectionString string
)

// TestMain sets up test dependencies.
func TestMain(m *testing.M) {
	ctx := context.Background()

	runAndPopulateMinIOContainer(ctx)
	runAndPopulateLocalStackContainer(ctx)
	runAndPopulateAzuriteContainer(ctx)
	defer func() {
		if err := testcontainers.TerminateContainer(minioContainer); err != nil {
			log.Printf("failed to terminate MinIO container: %s", err)
		}
		if err := testcontainers.TerminateContainer(localstackContainer); err != nil {
			log.Printf("failed to terminate LocalStack container: %s", err)
		}
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			log.Printf("failed to terminate Azurite container: %s", err)
		}
	}()

	code := m.Run()
	os.Exit(code)
}

// =====================================================================================================================
// Tests for Azure Blob connection

// TestNewAzBlobConnection_ConnectionType_MethodNil tests the behavior of NewAzBlobConnection when a nil connection
// method is provided. It ensures the function returns an appropriate error message indicating the nil method.
func TestNewAzBlobConnection_ConnectionType_MethodNil(t *testing.T) {
	conn, err := m2cs.NewAzBlobConnection(
		azuriteEndpoint,
		m2cs.ConnectionOptions{})
	require.Error(t, err)
	assert.EqualError(t, err, "connectionMethod cannot be nil")
	require.Nil(t, conn)
}

// TestNewAzBlobConnection_ConnectionType_InvalidConnType tests the behavior of NewAzBlobConnection when an invalid
// connection type is provided. It ensures the function returns an appropriate error message indicating the invalid
func TestNewAzBlobConnection_ConnectionType_InvalidConnType(t *testing.T) {
	cfg := &connection.AuthConfig{}
	cfg.SetConnectType("InvalidConnectionTypeForAzblob")

	conn, err := m2cs.NewAzBlobConnection(
		azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: cfg,
		})
	require.Error(t, err)
	assert.EqualError(t, err, "invalid connection method for Azure Blob; use: ConnectWithCredentials, ConnectWithEnvCredentials or ConnectWithConnectionString")
	require.Nil(t, conn)
}

// TestNewAzBlobConnection_WithCredentials_Success tests the creation of a new Azure Blob connection with credentials.
// The test checks if the connection is created successfully and if it finds the test-container.
func TestNewAzBlobConnection_WithCredentials_Success(t *testing.T) {
	conn, err := m2cs.NewAzBlobConnection(
		azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials(azurite.AccountName, azurite.AccountKey),
			IsMainInstance:   true,
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.GZIP_COMPRESSION,
		})
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.True(t, conn.GetConnectionProperties().IsMainInstance)
	assert.IsType(t, m2cs.NO_ENCRYPTION, conn.GetConnectionProperties().SaveEncrypt)
	assert.IsType(t, m2cs.GZIP_COMPRESSION, conn.GetConnectionProperties().SaveCompress)

	pager := conn.GetClient().NewListContainersPager(&azblob.ListContainersOptions{
		Include: azblob.ListContainersInclude{Metadata: true},
	})

	find := false
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to list containers: %s", err)
		}

		for _, container := range resp.ContainerItems {
			if *container.Name == "test-container" {
				find = true
				break
			}
		}
	}

	assert.True(t, find, "no test-container found")
	require.NoError(t, err)
}

// TestNewAzBlobConnection_WithEnvCredentials_Success tests the creation of a new Azure Blob connection with environment
// credentials. The test checks if the connection is created successfully and if it finds the test-container.
func TestNewAzBlobConnection_WithEnvCredentials_Success(t *testing.T) {
	originalAccessKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME")
	originalSecretKey := os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")

	os.Setenv("AZURE_STORAGE_ACCOUNT_NAME", azurite.AccountName)
	os.Setenv("AZURE_STORAGE_ACCOUNT_KEY", azurite.AccountKey)
	defer func() {
		os.Setenv("AZURE_STORAGE_ACCOUNT_NAME", originalAccessKey)
		os.Setenv("AZURE_STORAGE_ACCOUNT_KEY", originalSecretKey)
	}()

	conn, err := m2cs.NewAzBlobConnection(
		azuriteEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
		})
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.False(t, conn.GetConnectionProperties().IsMainInstance)
	assert.IsType(t, m2cs.NO_COMPRESSION, conn.GetConnectionProperties().SaveCompress)
	assert.IsType(t, m2cs.NO_ENCRYPTION, conn.GetConnectionProperties().SaveEncrypt)

	pager := conn.GetClient().NewListContainersPager(&azblob.ListContainersOptions{
		Include: azblob.ListContainersInclude{Metadata: true},
	})

	find := false
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to list containers: %s", err)
		}

		for _, container := range resp.ContainerItems {
			if *container.Name == "test-container" {
				find = true
				break
			}
		}
	}

	assert.True(t, find, "no test-container found")
	require.NoError(t, err)
}

// TestNewAzBlobConnection_WithConnectionString_Success tests the creation of a new Azure Blob connection with connection
// string. The test checks if the connection is created successfully and if it finds the test-container.
func TestNewAzBlobConnection_WithConnectionString_Success(t *testing.T) {
	conn, err := m2cs.NewAzBlobConnection(
		"",
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString(azuriteConnectionString),
			IsMainInstance:   true,
			SaveEncrypt:      m2cs.AES256_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
		})
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.True(t, conn.GetConnectionProperties().IsMainInstance)
	assert.IsType(t, m2cs.AES256_ENCRYPTION, conn.GetConnectionProperties().SaveEncrypt)
	assert.IsType(t, m2cs.NO_COMPRESSION, conn.GetConnectionProperties().SaveCompress)

	pager := conn.GetClient().NewListContainersPager(&azblob.ListContainersOptions{
		Include: azblob.ListContainersInclude{Metadata: true},
	})

	find := false
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to list containers: %s", err)
		}

		for _, container := range resp.ContainerItems {
			if *container.Name == "test-container" {
				find = true
				break
			}
		}
	}

	assert.True(t, find, "no test-container found")
	require.NoError(t, err)
}

// =====================================================================================================================
// Tests for MinIO connection

// TestNewMinIOConnection_ConnectionMethod_MethodNil verifies that  providing a nil connection  as the method results
// in an expected error.The function ensures that the connection is nil and the correct error message is returned.
func TestNewMinIOConnection_ConnectionMethod_MethodNil(t *testing.T) {
	conn, err := m2cs.NewMinIOConnection(minioEndpoint, m2cs.ConnectionOptions{
		ConnectionMethod: nil,
	}, nil)
	assert.EqualError(t, err, "connectionMethod cannot be nil")
	require.Nil(t, conn)
}

// TestNewMinIOConnection_ConnectionMethod_InvalidConnectionMethod verifies that  providing an invalid connection
// method results in an expected error.The function ensures that the connection is nil and the correct error
// message is returned.
func TestNewMinIOConnection_ConnectionMethod_InvalidConnectionMethod(t *testing.T) {
	conn, err := m2cs.NewMinIOConnection(minioEndpoint, m2cs.ConnectionOptions{
		ConnectionMethod: m2cs.ConnectWithConnectionString(""),
	}, nil)
	assert.EqualError(t, err, "invalid connection method for MinIO; use: ConnectWithCredentials or ConnectWithEnvCredentials")
	require.Nil(t, conn)
}

// TestNewMinIOConnection_WithCredentials_Success tests the creation of a new MinIO connection with credentials.
// The test checks if the connection is created successfully and if it finds the test-bucket.
func TestNewMinIOConnection_WithCredentials_Success(t *testing.T) {
	conn, err := m2cs.NewMinIOConnection(minioEndpoint, m2cs.ConnectionOptions{
		ConnectionMethod: m2cs.ConnectWithCredentials(minioUser, minioPassword),
		IsMainInstance:   false,
		SaveEncrypt:      m2cs.NO_ENCRYPTION,
		SaveCompress:     m2cs.NO_COMPRESSION,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.False(t, conn.GetConnectionProperties().IsMainInstance)
	assert.IsType(t, m2cs.NO_COMPRESSION, conn.GetConnectionProperties().SaveCompress)
	assert.IsType(t, m2cs.NO_ENCRYPTION, conn.GetConnectionProperties().SaveEncrypt)

	exist, err := conn.GetClient().BucketExists(context.Background(), "test-bucket")
	require.NoError(t, err)
	assert.True(t, exist, "no test-bucket found")
}

// TestNewMinioConnection_WithEnvCredentials_Success tests the creation of a new MinIO connectio with environment
// credentials. The test check if the connection is created successfully ad if it finds the test-bucket,
func TestNewMinioConnection_WithEnvCredentials_Success(t *testing.T) {
	originalAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	originalSecretKey := os.Getenv("MINIO_SECRET_KEY")

	os.Setenv("MINIO_ACCESS_KEY", "m2csUser")
	os.Setenv("MINIO_SECRET_KEY", "m2csPassword")
	defer func() {
		os.Setenv("MINIO_ACCESS_KEY", originalAccessKey)
		os.Setenv("MINIO_SECRET_KEY", originalSecretKey)
	}()

	conn, err := m2cs.NewMinIOConnection(minioEndpoint, m2cs.ConnectionOptions{
		ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
		IsMainInstance:   false,
		SaveEncrypt:      m2cs.AES256_ENCRYPTION,
		SaveCompress:     m2cs.GZIP_COMPRESSION,
	}, &minio.Options{Region: "no-region"})
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.False(t, conn.GetConnectionProperties().IsMainInstance)
	assert.IsType(t, m2cs.AES256_ENCRYPTION, conn.GetConnectionProperties().SaveEncrypt)
	assert.IsType(t, m2cs.GZIP_COMPRESSION, conn.GetConnectionProperties().SaveCompress)

	exist, err := conn.GetClient().BucketExists(context.Background(), "test-bucket")
	require.NoError(t, err)
	assert.True(t, exist, "no test-bucket found")
}

// =====================================================================================================================
// Tests for Azure S3 connection

// TestNewS3Connection_ConnectionType_MethodNil tests the behavior of NewS3Connection when a nil connection
// method is provided. It ensures the function returns an appropriate error message indicating the nil method.
func TestNewS3Connection_ConnectionType_MethodNil(t *testing.T) {
	conn, err := m2cs.NewS3Connection(
		localstackEndpoint,
		m2cs.ConnectionOptions{}, "")
	require.Error(t, err)
	assert.EqualError(t, err, "connectionMethod cannot be nil")
	require.Nil(t, conn)
}

// TestNewS3Connection_ConnectionType_InvalidConnType tests the behavior of NewS3Connection when an invalid
// connection type is provided. It ensures the function returns an appropriate error message indicating the invalid
func TestNewS3Connection_ConnectionType_InvalidConnType(t *testing.T) {
	conn, err := m2cs.NewS3Connection(
		localstackEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString("randomstring"),
		}, "")
	require.Error(t, err)
	assert.EqualError(t, err, "invalid connection method for AWS S3; use: ConnectWithCredentials or ConnectWithEnvCredentials")
	require.Nil(t, conn)
}

// TestNewS3Connection_WithCredentials_Success tests the creation of a new S3 connection with credentials.
// The test checks if the connection is created successfully and if it finds the test-bucket.
func TestNewS3Connection_WithCredentials_Success(t *testing.T) {
	conn, err := m2cs.NewS3Connection(
		localstackEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("accesskey", "secretkey"),
			IsMainInstance:   false,
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
		}, awsRegion)
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.False(t, conn.GetConnectionProperties().IsMainInstance)
	assert.IsType(t, m2cs.NO_COMPRESSION, conn.GetConnectionProperties().SaveCompress)
	assert.IsType(t, m2cs.NO_ENCRYPTION, conn.GetConnectionProperties().SaveEncrypt)

	client := conn.GetClient()
	bucketList, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	require.NoError(t, err)

	find := false
	for _, bucket := range bucketList.Buckets {
		if *bucket.Name == "test-bucket" {
			find = true
			break
		}
	}
	require.True(t, find, "no test-bucket found")
}

// TestNewS3Connection_WithEnvCredentials_Success tests the creation of a new S3 connection with environment
// credentials. The test checks if the connection is created successfully and if it finds the test-bucket.
func TestNewS3Connection_WithEnvCredentials_Success(t *testing.T) {
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	os.Setenv("AWS_ACCESS_KEY_ID", "accesskey")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "secretkey")
	defer func() {
		os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
	}()

	conn, err := m2cs.NewS3Connection(
		localstackEndpoint,
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
			IsMainInstance:   true,
			SaveEncrypt:      m2cs.AES256_ENCRYPTION,
			SaveCompress:     m2cs.GZIP_COMPRESSION,
		}, "")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.True(t, conn.GetConnectionProperties().IsMainInstance)
	assert.IsType(t, m2cs.AES256_ENCRYPTION, conn.GetConnectionProperties().SaveEncrypt)
	assert.IsType(t, m2cs.GZIP_COMPRESSION, conn.GetConnectionProperties().SaveCompress)

	client := conn.GetClient()
	bucketList, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	require.NoError(t, err)

	find := false
	for _, bucket := range bucketList.Buckets {
		if *bucket.Name == "test-bucket" {
			find = true
			break
		}
	}
	require.True(t, find, "no test-bucket found")
}

// =====================================================================================================================
// Container setup functions

// runAndPopulateAzuriteContainer starts the Azurite container and populates it with a test container.
// The container created in this function is used to test methods where an actual connection is made,
// to see if the connections can find the container.
func runAndPopulateAzuriteContainer(ctx context.Context) {
	azuriteContainer, err := azurite.Run(
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

	client, err := azblob.NewClientFromConnectionString(azuriteConnectionString, nil)
	if err != nil {
		fmt.Printf("failed to create the Azurite client: %s", err)
	}

	_, err = client.CreateContainer(context.TODO(), "test-container", nil)
	if err != nil {
		fmt.Printf("failed to create the azurite container for test: %s\n", err)
	}
}

// runAndPopulateLocalStackContainer starts the LocalStack container and populates it with a test bucket.
// The bucket created in this function is used to test methods where an actual connection is made,
// to see if the connections can find the bucket.
func runAndPopulateLocalStackContainer(ctx context.Context) {
	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:latest",
		testcontainers.WithEnv(map[string]string{
			"SERVICES": "lambda,s3",
		}))
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
		return
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		log.Fatalf("failed to create docker provider: %s", err)
	}

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		log.Fatalf("failed to retrieve deamon host: %s", err)
	}

	mappedPort, err := localstackContainer.MappedPort(ctx, nat.Port("4566"))
	if err != nil {
		log.Fatalf("failed to retrieve mapped port: %s", err)
	}

	localstackEndpoint = fmt.Sprintf("http://%s:%d", host, mappedPort.Int())

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("accesskey", "secretkey", "")),
	)
	if err != nil {
		log.Fatalf("failed to load AWS configuration: %s", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(localstackEndpoint)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(context.TODO(), &s3.CreateBucketInput{Bucket: aws.String("test-bucket")})
	if err != nil {
		log.Fatalf("failed to create the aws s3 bucket for test: %s\n", err)
	}
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

	minioClient, err := minio.New(strings.Replace(minioEndpoint, "http://", "", 1), &minio.Options{
		Creds:  minioCredentials.NewStaticV4(minioUser, minioPassword, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("failed to create MinIO client: %s", err.Error())
	}

	err = minioClient.MakeBucket(ctx, "test-bucket", minio.MakeBucketOptions{})
	if err != nil {
		log.Fatalf("failed to create the minio bucket for test: %s\n", err)
	}
}
