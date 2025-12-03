package azblob_connection_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azurite"
	"github.com/tizianocitro/m2cs/internal/connection"
	connfilestorage "github.com/tizianocitro/m2cs/internal/connection/filestorage"
	"log"
	"os"
	"testing"
)

var blobServiceURL string
var connectionString string

func TestMain(m *testing.M) {
	ctx := context.Background()
	azuriteContainer, err := azurite.Run(
		ctx,
		"mcr.microsoft.com/azure-storage/azurite:3.33.0",
		azurite.WithInMemoryPersistence(64),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}

	blobServiceURL = fmt.Sprintf("%s/%s", azuriteContainer.MustServiceURL(ctx, azurite.BlobService), azurite.AccountName)
	connectionString = fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;", azurite.AccountName, azurite.AccountKey, blobServiceURL)

	code := m.Run()
	os.Exit(code)
}

func TestCreateAzBlobConnection_ConnectionType_InvalidConnType(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("InvalidConnectionTypeForAzblob")
	config.SetAccessKey(azurite.AccountName)
	config.SetSecretKey(azurite.AccountKey)

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.Error(t, err, "expected error for invalid connection type, got nil")
	assert.ErrorContains(t, err, "invalid connection type for azure blob:")
	require.Nil(t, conn)
}

func TestCreateAzBlobConnection_WithCredentials_MissingCredentials(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("")
	config.SetSecretKey("")

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.Error(t, err, "expected error for missing credential, got nil")
	assert.EqualError(t, err, "access key and/or secret key not set")
	require.Nil(t, conn)
}

func TestCreateAzBlobConnection_WithCredentials_InvalidCredentials(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey("wrongUser")
	config.SetSecretKey(azurite.AccountKey)

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.Error(t, err, "expected error for invalid credentials, got nil")
	assert.ErrorContains(t, err, "failed to connect to azure blob: ")
	require.Nil(t, conn)
}

func TestCreateAzBlobConnection_WithEnv_NoEnvCredentialSetup(t *testing.T) {
	originalAccessKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME")
	originalSecretKey := os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")

	os.Unsetenv("AZURE_STORAGE_ACCOUNT_NAME")
	os.Unsetenv("AZURE_STORAGE_ACCOUNT_KEY")
	defer func() {
		os.Setenv("AZURE_STORAGE_ACCOUNT_NAME", originalAccessKey)
		os.Setenv("AZURE_STORAGE_ACCOUNT_KEY", originalSecretKey)
	}()

	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.Error(t, err, "expected error for no environment credential found, got nil")
	assert.EqualError(t, err, "environment variables AZURE_STORAGE_ACCOUNT_NAME and/or AZURE_STORAGE_ACCOUNT_KEY are not set")
	require.Nil(t, conn)
}

func TestCreateAzBlobConnection_WithEnv_InvalidCredential(t *testing.T) {
	originalAccessKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME")
	originalSecretKey := os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")

	os.Setenv("AZURE_STORAGE_ACCOUNT_NAME", "invalidUser")
	os.Setenv("AZURE_STORAGE_ACCOUNT_KEY", azurite.AccountKey)
	defer func() {
		os.Setenv("AZURE_STORAGE_ACCOUNT_NAME", originalAccessKey)
		os.Setenv("AZURE_STORAGE_ACCOUNT_KEY", originalSecretKey)
	}()

	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.Error(t, err, "expected error for invalid credentials, got nil")
	assert.ErrorContains(t, err, "failed to connect to azure blob: ")
	require.Nil(t, conn)
}

func TestCreateAzBlobConnection_WithConnString_InvalidString(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withConnectionString")
	config.SetConnectionString("invalidstring")

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.Error(t, err, "expected error for invalid connection string, got nil")
	assert.ErrorContains(t, err, "failed to create Azure Blob Storage client")
	require.Nil(t, conn)
}

func TestCreateAzBlobConnection_WithCredentials_Success(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withCredential")
	config.SetAccessKey(azurite.AccountName)
	config.SetSecretKey(azurite.AccountKey)

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.NoError(t, err)
	require.NotNil(t, conn)
}

// TestCreateConnection_WithEnv_Success verify creating an Azure Blob connection
// using environment variables for authentication. The test verifies that the
// connection is successfully created,
func TestCreateAzBlobConnection_WithEnv_Success(t *testing.T) {
	originalAccessKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME")
	originalSecretKey := os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")

	os.Setenv("AZURE_STORAGE_ACCOUNT_NAME", azurite.AccountName)
	os.Setenv("AZURE_STORAGE_ACCOUNT_KEY", azurite.AccountKey)
	defer func() {
		os.Setenv("AZURE_STORAGE_ACCOUNT_NAME", originalAccessKey)
		os.Setenv("AZURE_STORAGE_ACCOUNT_KEY", originalSecretKey)
	}()

	config := &connection.AuthConfig{}
	config.SetConnectType("withEnv")

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestCreateAzBlobConnection_WithConnString_Success(t *testing.T) {
	config := &connection.AuthConfig{}
	config.SetConnectType("withConnectionString")
	config.SetConnectionString(connectionString)

	conn, err := connfilestorage.CreateAzBlobConnection(blobServiceURL, config)
	require.NoError(t, err)
	require.NotNil(t, conn)
}
