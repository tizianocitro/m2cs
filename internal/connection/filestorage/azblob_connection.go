package connfilestorage

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/tizianocitro/m2cs/internal/connection"
	common "github.com/tizianocitro/m2cs/pkg"
	"github.com/tizianocitro/m2cs/pkg/filestorage"
	"os"
)

// CreateAzBlobConnection creates a new AzBlobClient.
// It returns an AzBlobClient or an error if the connection could not be established.
func CreateAzBlobConnection(endpoint string, config *connection.AuthConfig) (*filestorage.AzBlobClient, error) {
	if config == nil {
		return nil, fmt.Errorf("AuthConfig cannot be nil")
	}

	var azClient *azblob.Client = nil

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

		azClient = client
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

		azClient = client
	case "withConnectionString":
		client, err := azblob.NewClientFromConnectionString(config.GetConnectionString(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %v", err)
		}

		azClient = client
	default:
		return nil, fmt.Errorf("invalid connection type for azure blob: %s", config.GetConnectType())
	}
	if azClient == nil {
		return nil, fmt.Errorf("client is not initialized")
	}

	pager := azClient.NewListContainersPager(nil)
	_, err := pager.NextPage(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to azure blob: %w", err)
	}

	conn, err := filestorage.NewAzBlobClient(azClient, common.ConnectionProperties{
		IsMainInstance: config.GetProperties().IsMainInstance,
		SaveEncrypt:    config.GetProperties().SaveEncrypted,
		SaveCompress:   config.GetProperties().SaveCompressed,
		EncryptKey:     config.GetProperties().EncryptKey})

	return conn, nil
}
