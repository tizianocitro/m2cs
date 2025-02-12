package connfilestorage

import (
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
func CreateAzBlobConnection(config *connection.AuthConfig) (*AzBlobConnection, error) {

	if config == nil {
		return nil, fmt.Errorf("AuthConfig cannot be nil")
	}

	conn := &AzBlobConnection{
		Properties: config.GetProperties(),
	}

	switch config.GetConnectType() {
	case "withCredential":
		credential, err := azblob.NewSharedKeyCredential(config.GetAccessKey(), config.GetSecretKey())
		if err != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %v", err)
		}

		accountURL := fmt.Sprintf("https://%s.blob.core.windows.net", config.GetAccessKey())

		client, err := azblob.NewClientWithSharedKeyCredential(accountURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %v", err)
		}

		conn.client = client
		break
	case "withEnv":
		accountName, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_NAME")
		if !ok {
			panic("AZURE_STORAGE_ACCOUNT_NAME not found")
		}

		accountKey, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_KEY")
		if !ok {
			panic("AZURE_STORAGE_ACCOUNT_KEY not found")
		}

		credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %v", err)
		}

		accountURL := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)

		client, err := azblob.NewClientWithSharedKeyCredential(accountURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %v", err)
		}

		conn.client = client
		break
	case "withConnectionString":
		client, err := azblob.NewClientFromConnectionString(config.GetConnectionString(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %v", err)
		}

		conn.client = client
		break
	default:
		return nil, fmt.Errorf("invalid connection type: %s", config.GetConnectType())
	}

	return conn, nil
}
