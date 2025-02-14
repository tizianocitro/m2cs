package connfilestorage

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"m2cs/internal/connection"
	"os"
)

// AzBlobConnection represents a connection to an Azure Blob Storage.
type AzBlobConnection struct {
	client *azblob.Client
	connection.Properties
}

// GetClient returns the Azure Blob Storage client.
func (a *AzBlobConnection) GetClient() *azblob.Client {
	return a.client
}

// CreateAzBlobConnection creates a new AzBlobConnection.
// It returns a AzBlobConnection or an error if the connection could not be established.
func CreateAzBlobConnection(endpoint string, config *connection.AuthConfig) (*AzBlobConnection, error) {
	if config == nil {
		return nil, fmt.Errorf("AuthConfig cannot be nil")
	}

	conn := &AzBlobConnection{
		Properties: config.GetProperties(),
	}

	switch config.GetConnectType() {
	case "withCredential":
		if config.GetAccessKey() == "" || config.GetSecretKey() == "" {
			return nil, fmt.Errorf("access key and/or secret key not set")
		}

		credential, err := azblob.NewSharedKeyCredential(config.GetAccessKey(), config.GetSecretKey())
		if err != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %v", err)
		}

		var accountURL string
		if endpoint == "" || endpoint == "default" {
			accountURL = fmt.Sprintf("https://%s.blob.core.windows.net", config.GetAccessKey())
		} else {
			accountURL = endpoint
		}

		client, err := azblob.NewClientWithSharedKeyCredential(accountURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %v", err)
		}

		conn.client = client
	case "withEnv":
		accountName := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME")
		accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")
		if accountName == "" || accountKey == "" {
			return nil, fmt.Errorf("environment variables AZURE_STORAGE_ACCOUNT_NAME and/or AZURE_STORAGE_ACCOUNT_KEY are not set")
		}

		credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %v", err)
		}

		var accountURL string
		if endpoint == "" || endpoint == "default" {
			accountURL = fmt.Sprintf("https://%s.blob.core.windows.net", config.GetAccessKey())
		} else {
			accountURL = endpoint
		}

		client, err := azblob.NewClientWithSharedKeyCredential(accountURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %v", err)
		}

		conn.client = client
	case "withConnectionString":
		client, err := azblob.NewClientFromConnectionString(config.GetConnectionString(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %v", err)
		}

		conn.client = client
	default:
		return nil, fmt.Errorf("invalid connection type for azure blob: %s", config.GetConnectType())
	}
	if conn.client == nil {
		return nil, fmt.Errorf("client is not initialized")
	}

	pager := conn.client.NewListContainersPager(nil)
	_, err := pager.NextPage(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to azure blob: %w", err)
	}

	return conn, nil
}
