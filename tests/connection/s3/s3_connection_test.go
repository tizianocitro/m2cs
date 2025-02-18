package s3_connection_test

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"log"
	"m2cs/internal/connection"
	connfilestorage "m2cs/internal/connection/filestorage"
	"os"
	"testing"
)

var s3ServiceUrl string

// TestMain sets up the LocalStack container as a test dependency.
// Once the tests are run, the container is terminated to ensure proper cleanup.
func TestMain(m *testing.M) {
	ctx := context.Background()

	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:latest",
		testcontainers.WithEnv(map[string]string{
			"SERVICES": "lambda,s3",
		}))
	defer func() {
		if err := testcontainers.TerminateContainer(localstackContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		log.Printf("failed to create docker provider: %s", err)
	}

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		log.Printf("failed to retrive deamon host: %s", err)
	}

	mappedPort, err := localstackContainer.MappedPort(ctx, nat.Port("4566"))
	if err != nil {
		log.Printf("failed to retrive mapped port: %s", err)
	}

	s3ServiceUrl = fmt.Sprintf("http://%s:%d", host, mappedPort.Int())

	code := m.Run()
	os.Exit(code)
}

// TestCreateS3Connection_Endpoint_InvalidEndpoint tests the behavior of CreateS3Connection when
// an invalid endpoint is provided. It ensures the function returns an appropriate error message
// indicating the failure to connect to AWS S3.
func TestCreateS3Connection_Endpoint_InvalidEndpoint(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("m2csUser")
	config.SetSecretKey("m2csPassword")

	conn, err := connfilestorage.CreateS3Connection("invalidHost", config, "")
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to connect to AWS S3:")
	require.Nil(t, conn)
}

// TestCreateS3Connection_ConnectionType_InvalidConnType tests the behavior of CreateS3Connection function when
// an invalid connection type is provided. It ensures the function returns an appropriate error message indicating
// the invalid connection type for AWS S3.
func TestCreateS3Connection_ConnectionType_InvalidConnType(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("InvalidConnectionTypeForS3")
	config.SetAccessKey("m2csUser")
	config.SetSecretKey("m2csPassword")

	conn, err := connfilestorage.CreateS3Connection(s3ServiceUrl, config, "")
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid connection type for AWS S3:")
	require.Nil(t, conn)
}

// TestCreateS3Connection_WithCredentials_MissingCredentials tests the behavior of CreateS3Connection function when
// access or secret keys are missing. It ensures the function returns an appropriate error message indicating the
// missing credentials.
func TestCreateS3Connection_WithCredentials_MissingCredentials(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("")
	config.SetSecretKey("m2csPassword")

	conn, err := connfilestorage.CreateS3Connection(s3ServiceUrl, config, "")
	require.Error(t, err)
	assert.EqualError(t, err, "access key and/or secret key not set")
	require.Nil(t, conn)
}

// TestCreateS3Connection_WithEnv_NoEnvCredentialSetup tests the behavior of CreateS3Connection function when
// environment variables are not set. It ensures the function returns an appropriate error message indicating the
// missing environment credentials.
func TestCreateS3Connection_WithEnv_NoEnvCredentialSetup(t *testing.T) {
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	os.Unsetenv("AWS_ACCESS_KEY_ID")
	os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	defer func() {
		os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
	}()

	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateS3Connection(s3ServiceUrl, config, "")
	require.Error(t, err, "expected error for no environment credential found, got nil")
	assert.EqualError(t, err, "environment variables AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY are not set")
	require.Nil(t, conn)
}

// TestCreateS3Connection_WithCredentials_Success tests the behavior of CreateS3Connection function
// when valid credentials are provided. It ensures the function returns a valid S3Connection.
func TestCreateS3Connection_WithCredentials_Success(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("m2csUser")
	config.SetSecretKey("m2csPassword")

	conn, err := connfilestorage.CreateS3Connection(s3ServiceUrl, config, "")
	require.NoError(t, err)
	require.NotNil(t, conn)
}

// TestCreateS3Connection_WithEnv_Success tests the behavior of CreateS3Connection function
// when environment credentials are provided. It ensures the function returns a valid S3Connection.
func TestCreateS3Connection_WithEnv_Success(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateS3Connection(s3ServiceUrl, config, "")
	require.NoError(t, err)
	require.NotNil(t, conn)
}
