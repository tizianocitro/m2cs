package connfilestorage_test

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"m2cs/internal/connection"
	connfilestorage "m2cs/internal/connection/filestorage"
	"os"
	"strings"
	"testing"
)

var (
	minioContainer testcontainers.Container
	httpEndpoint   string
	httpsEndpoint  string
)

// TestMain sets up the MinIO container as a test dependency.
// The container is started using Testcontainers with the necessary setup
// (environment variables, commands). Once the tests are run,
// the container is terminated to ensure proper cleanup.
func TestMain(m *testing.M) {
	ctx := context.Background()

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

	httpEndpoint, err = minioContainer.Endpoint(ctx, "http")
	if err != nil {
		log.Fatalf("Error while getting the MinIO container http endpoint: %s", err)
	}
	httpEndpoint, _ = strings.CutPrefix(httpEndpoint, "http://")

	httpsEndpoint, err = minioContainer.Endpoint(ctx, "https")
	if err != nil {
		log.Fatalf("Error while getting the MinIO container https endpoint: %s", err)
	}
	httpsEndpoint, _ = strings.CutPrefix(httpsEndpoint, "https://")

	code := m.Run()

	err = minioContainer.Terminate(ctx)
	if err != nil {
		log.Fatalf("Error while terminating the container: %v", err)
	}

	os.Exit(code)
}

// TestCreateMinioConnection_Endpoint_InvalidEndpoint verifies that CreateMinioConnection
// correctly handles an invalid endpoint by returning an error specifying the host issue.
// It ensures that no connection is initialized when the endpoint is unreachable.
func TestCreateMinioConnection_Endpoint_InvalidEndpoint(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("m2csUser")
	config.SetSecretKey("m2csPassword")

	conn, err := connfilestorage.CreateMinioConnection("invalidHost", config, nil)
	if err == nil {
		t.Fatal("expected error for missing keys, got nil")
	}
	if !strings.Contains(err.Error(), "no such host") {
		t.Fatalf("expected error message: ... no such host,\n but obtained: %s", err.Error())
	}
	if conn != nil {
		t.Fatal("the connection was initialized but it should not have been with an invalid connType")
	}
}

// TestCreateMinioConnection_ConnectionType_InvalidConnType verifies that CreateMinioConnection
// correctly handles invalid connection types by returning an error specifying the invalid type.
// It ensures that no connection is established when the connection type is unsupported.
func TestCreateMinioConnection_ConnectionType_InvalidConnType(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("InvalidConnectionTypeForMinio")
	config.SetAccessKey("m2csUser")
	config.SetSecretKey("m2csPassword")

	conn, err := connfilestorage.CreateMinioConnection(httpEndpoint, config, nil)
	if err == nil {
		t.Fatal("expected error for missing keys, got nil")
	}
	if !strings.Contains(err.Error(), "invalid connection type for MinIO") {
		t.Fatalf("expected error message: invalid connection type for MinIO: ...,\n but obtained: %s", err.Error())
	}
	if conn != nil {
		t.Fatal("the connection was initialized but it should not have been with an invalid connType")
	}
}

// TestCreateMinioConnection_WithCredential_MissingCredential tests the behavior of CreateMinioConnection when access and secret keys are missing.
// It ensures the function returns an appropriate error message indicating missing credentials.
func TestCreateMinioConnection_WithCredential_MissingCredential(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("")
	config.SetSecretKey("")

	conn, err := connfilestorage.CreateMinioConnection(httpEndpoint, config, nil)
	if err == nil {
		t.Fatal("expected error for missing keys, got nil")
	}
	if err.Error() != "access key and/or secret key not set" {
		t.Fatalf("expected error message: access key and/or secret key not set,\n but obtained: %s", err.Error())
	}
	if conn != nil {
		t.Fatal("the connection was initialized but it should not have been with an invalid connType")
	}
}

// TestCreateMinioConnection_WithCredential_InvalidCredential tests the behavior of CreateMinioConnection
// when the provided access and secret keys are invalid.
// It verifies that the function correctly identifies invalid credentials and returns a descriptive error message.
func TestCreateMinioConnection_WithCredential_InvalidCredential(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("wrongUser")
	config.SetSecretKey("wrongPassword")

	conn, err := connfilestorage.CreateMinioConnection(httpEndpoint, config, nil)
	if err == nil {
		t.Fatal("expected error for wrong credentials, git nil")
	}
	if err.Error() != "failed to connect to MinIO: The Access Key Id you provided does not exist in our records." {
		t.Fatalf("expected error message:failed to connect to MinIO: The Access Key Id you provided does not exist in our records."+
			"\n but obtained: %s", err.Error())
	}
	if conn != nil {
		t.Fatal("the connection was initialized but it should not have been with an invalid connType")
	}
}

// TestCreateMinioConnection_WithEnv_NoEnvCredentialSetup verifies behavior
// when environment variables for credentials are not set.
func TestCreateMinioConnection_WithEnv_NoEnvCredentialSetup(t *testing.T) {
	originalAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	originalSecretKey := os.Getenv("MINIO_SECRET_KEY")

	os.Unsetenv("MINIO_ACCESS_KEY")
	os.Unsetenv("MINIO_SECRET_KEY")
	defer func() {
		os.Setenv("MINIO_ACCESS_KEY", originalAccessKey)
		os.Setenv("MINIO_SECRET_KEY", originalSecretKey)
	}()

	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateMinioConnection(httpEndpoint, config, nil)
	if err == nil {
		t.Fatal("expected error for wrong credentials, git nil")
	}
	if err.Error() != "environment variables MINIO_ACCESS_KEY and/or MINIO_SECRET_KEY are not set" {
		t.Fatalf("environment variables MINIO_ACCESS_KEY and/or MINIO_SECRET_KEY are not set"+
			"\n but obtained: %s", err.Error())
	}
	if conn != nil {
		t.Fatal("the connection was initialized but it should not have been with an invalid connType")
	}
}

// TestCreateMinioConnection_WithEnv_InvalidCredential tests the behavior of CreateMinioConnection
// when invalid credentials are provided via environment variables ("MINIO_ACCESS_KEY" and "MINIO_SECRET_KEY").
// It ensures that the function returns a descriptive error message indicating the provided credentials can't be used to connect.
func TestCreateMinioConnection_WithEnv_InvalidCredential(t *testing.T) {
	originalAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	originalSecretKey := os.Getenv("MINIO_SECRET_KEY")

	os.Setenv("MINIO_ACCESS_KEY", "invalidUser")
	os.Setenv("MINIO_SECRET_KEY", "invalidPassword")
	defer func() {
		os.Setenv("MINIO_ACCESS_KEY", originalAccessKey)
		os.Setenv("MINIO_SECRET_KEY", originalSecretKey)
	}()

	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateMinioConnection(httpEndpoint, config, nil)
	if err == nil {
		t.Fatal("expected error for wrong credentials, git nil")
	}
	if err.Error() != "failed to connect to MinIO: The Access Key Id you provided does not exist in our records." {
		t.Fatalf("expected error message:failed to connect to MinIO: The Access Key Id you provided does not exist in our records."+
			"\n but obtained: %s", err.Error())
	}
	if conn != nil {
		t.Fatal("the connection was initialized but it should not have been with an invalid connType")
	}
}

// TestCreateMinioConnection_WithEnv_Success tests the successful connection to MinIO
// using credentials provided via environment variables. It temporarily sets environment
// variables ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"), initializes the connection, and
// validates that a connection was established without errors and a valid object was returned.
func TestCreateMinioConnection_WithEnv_Success(t *testing.T) {
	originalAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	originalSecretKey := os.Getenv("MINIO_SECRET_KEY")

	os.Setenv("MINIO_ACCESS_KEY", "m2csUser")
	os.Setenv("MINIO_SECRET_KEY", "m2csPassword")
	defer func() {
		os.Setenv("MINIO_ACCESS_KEY", originalAccessKey)
		os.Setenv("MINIO_SECRET_KEY", originalSecretKey)
	}()

	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateMinioConnection(httpEndpoint, config, nil)
	if err != nil {
		t.Fatalf("connection with environment variables should succeed, but returned error: %v", err)
	}
	if conn == nil {
		t.Fatal("the connection is nil, a valid object was expected")
	}
}

// TestCreateMinioConnection_WithCredential_Success tests the behavior of CreateMinioConnection
// when provided access and secret keys are valid. It ensures a successful connection is established
// and a valid connection object is returned without any error.
func TestCreateMinioConnection_WithCredential_Success(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("m2csUser")
	config.SetSecretKey("m2csPassword")

	conn, err := connfilestorage.CreateMinioConnection(httpEndpoint, config, nil)
	if err != nil {
		t.Fatalf("connection with credential should succeed, but returned error: %v", err)
	}
	if conn == nil {
		t.Fatal("the connection is nil, a valid object was expected")
	}
}
