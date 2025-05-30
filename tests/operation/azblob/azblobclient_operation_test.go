package azblob

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azurite"
	"io"
	"log"
	common "m2cs/pkg"
	"m2cs/pkg/filestorage"
	"os"
	"strings"
	"testing"
)

var (
	azureBlobClient         *azblob.Client
	testClient              *filestorage.AzBlobClient
	azuriteContainer        testcontainers.Container
	azuriteEndpoint         string
	azuriteConnectionString string
)

// TestMain sets up the Azurite container as a test dependency.
// The container is started using Testcontainers with the necessary setup
// (environment variables, commands). Once the tests are run,
// the container is terminated to ensure proper cleanup.
func TestMain(m *testing.M) {
	ctx := context.Background()

	runAndPopulateAzuriteContainer(ctx)
	defer func() {
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			log.Printf("failed to terminate MinIO container: %s", err)
		}
	}()

	code := m.Run()
	os.Exit(code)
}

// TestAzBlobClient_NewAzBlobClient_ClientNil verifies that the NewAzBlobClient function
// of the AzBlobClient returns an error when a nil client is passed. It ensures that
// the function handles this invalid input gracefully by returning an appropriate error message.
func TestAzBlobClient_NewAzBlobClient_ClientNil(t *testing.T) {
	client, err := filestorage.NewAzBlobClient(nil, common.ConnectionProperties{})

	require.Nil(t, client)
	require.Error(t, err, "expected error for nil client, got nil")
	assert.ErrorContains(t, err, "failed to create AzBlobClient: client is nil")
}

// TestAzBlobClient_NewAzBlobClient_Success verifies that the NewAzBlobClient function correctly
// returns an AzBlobClient, which is a wrapper around the original Azure Blob Storage client.
// If the client is active and functional, it creates the wrapper with the appropriate properties,
// ensuring that the AzBlobClient(the wrapper) can be used correctly with the m2cs components.
func TestAzBlobClient_NewAzBlobClient_Success(t *testing.T) {
	client, err := azblob.NewClientFromConnectionString(azuriteConnectionString, nil)
	if err != nil {
		t.Fatalf("failed to create Azure Blob client: %v", err)
	}

	azBlobClient, err := filestorage.NewAzBlobClient(client, common.ConnectionProperties{})
	require.NoError(t, err)
	assert.NotNil(t, azBlobClient)

	find := false

	containers, err := azBlobClient.ListContainers()
	require.NoError(t, err, "failed to list containers")

	for _, container := range containers {
		if strings.Contains(container, "test-container") {
			find = true
			break
		}
	}

	assert.True(t, find, "test-container should be present in the list of containers")
}

// TestAzBlobClient_CreateContainer_AzureError verifies that the CreateBucket method
// of the AzBlobClient correctly returns errors from the original azure blob client.
// This test uses the scenario where the container already exists.
func TestAzBlobClient_CreateContainer_AzureError(t *testing.T) {
	err := testClient.CreateContainer(context.TODO(), "test-container")

	require.Error(t, err, "expected error for MinIO error, got nil")
	assert.ErrorContains(t, err, "ContainerAlreadyExists")
}

// TestAzBlobClient_CreateContainer_Success verifies that the CreateContainer method
// of the AzBlobClient successfully creates a new container in Azure Blob Storage.
func TestAzBlobClient_CreateContainer_Success(t *testing.T) {
	err := testClient.CreateContainer(context.TODO(), "new-test-container")
	require.NoError(t, err, "expected no error when creating a new container")

	find := false

	pager := azureBlobClient.NewListContainersPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		require.NoError(t, err, "failed to list containers")

		for _, container := range resp.ContainerItems {
			if container.Name != nil && *container.Name == "new-test-container" {
				find = true
			}
		}
	}

	assert.True(t, find, "new-test-container should be present in the list of containers")
}

// TestAzBlobClient_ListContainer_Success ensures that the ListContainers method returns
// the list of container and verify if the test-bucket is present.
func TestAzBlobClient_ListContainer_Success(t *testing.T) {
	buckets, err := testClient.ListContainers()
	require.NoError(t, err, "expected no error when listing buckets")

	find := false
	for _, bucket := range buckets {
		if strings.Contains(bucket, "test-container") {
			find = true
			break
		}
	}

	assert.True(t, find, "test-container should be present in the list of buckets")
}

// TestAzBlobClient_PutObject_AzureError verifies that the PutObject method
// of the AzBlobClient correctly returns errors from the original azure blob client.
func TestAzBlobClient_PutObject_AzureError(t *testing.T) {
	err := testClient.PutObject(context.TODO(), "non-existing-container", "test-object", strings.NewReader("test"))

	require.Error(t, err, "expected error for non-existing container, got nil")
	assert.ErrorContains(t, err, "ContainerNotFound")
}

// TestAzBlobClient_PutObject_Success verifies that the PutObject method
// of the AzBlobClient successfully uploads an object to an existing container in Azure Blob Storage.
func TestAzBlobClient_PutObject_Success(t *testing.T) {
	err := testClient.PutObject(context.TODO(), "test-container", "test-object", strings.NewReader("test"))
	require.NoError(t, err, "expected no error when uploading an object")

	object, err := azureBlobClient.DownloadStream(context.TODO(), "test-container", "test-object", nil)

	retryReader := object.NewRetryReader(context.TODO(), &azblob.RetryReaderOptions{})

	buf := make([]byte, int64(len("test")))
	for {
		_, err := retryReader.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
	}

	assert.Contains(t, string(buf), "test", "expected object content to be 'test'")
}

// TestAzBlobClient_GetObject_AzureError verifies that the GetObject method
// of the AzBlobClient correctly returns errors from the original azure blob client.
// This test uses the scenario where the container does not exist.
func TestAzBlobClient_GetObject_AzureError(t *testing.T) {
	reader, err := testClient.GetObject(context.TODO(), "non-existing-container", "test-object")

	require.Error(t, err, "expected error for non-existing container, got nil")
	assert.ErrorContains(t, err, "ContainerNotFound")
	require.Nil(t, reader, "expected nil reader for non-existing container")
}

// TestAzBlobClient_GetObject_Success verifies that the GetObject method
// of the AzBlobClient successfully retrieves an object from an existing container in Azure Blob Storage.
func TestAzBlobClient_GetObject_Success(t *testing.T) {
	reader, err := testClient.GetObject(context.TODO(), "test-container", "test-get-object")
	require.NoError(t, err, "expected no error when getting an object")

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

	assert.Equal(t, "test", string(buf), "expected object content to be 'test'")
}

// TestAzBlobClient_RemoveObject_AzureError verifies that the RemoveObject method
// of the AzBlobClient correctly returns errors from the original azure blob client.
// This test uses the scenario where the container does not exist.
func TestAzBlobClient_RemoveObject_AzureError(t *testing.T) {
	err := testClient.RemoveObject(context.TODO(), "non-existing-container", "test-object")

	require.Error(t, err, "expected error for non-existing container, got nil")
	assert.ErrorContains(t, err, "ContainerNotFound")
}

// TestAzBlobClient_RemoveObject_Success verifies that the RemoveObject method
// of the AzBlobClient successfully removes an object from an existing container in Azure Blob Storage.
func TestAzBlobClient_RemoveObject_Success(t *testing.T) {
	err := testClient.RemoveObject(context.TODO(), "test-container", "test-get-object")
	require.NoError(t, err, "expected no error when removing an object")

	_, err = azureBlobClient.DownloadStream(context.TODO(), "test-container", "test-get-object", nil)
	require.Error(t, err, "expected error when trying to get a removed object")
	assert.ErrorContains(t, err, "BlobNotFound")
}

// TestAzBlobClient_DeleteContainer_AzureError verifies that the DeleteContainer method
// of the AzBlobClient correctly returns errors from the original azure blob client.
// This test uses the scenario where the container does not exist.
func TestAzBlobClient_DeleteContainer_AzureError(t *testing.T) {
	err := testClient.DeleteContainer(context.TODO(), "non-existing-container")

	require.Error(t, err, "expected error for non-existing container, got nil")
	assert.ErrorContains(t, err, "ContainerNotFound")
}

// TestAzBlobClient_DeleteContainer_Success verifies that the DeleteContainer method
// of the AzBlobClient successfully deletes an existing container in Azure Blob Storage.
func TestAzBlobClient_DeleteContainer_Success(t *testing.T) {
	err := testClient.DeleteContainer(context.TODO(), "test-container")
	require.NoError(t, err, "expected no error when deleting an existing container")

	find := false

	pager := azureBlobClient.NewListContainersPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		require.NoError(t, err, "failed to list containers")

		for _, container := range resp.ContainerItems {
			if container.Name != nil && *container.Name == "test-container" {
				find = true
			}
		}
	}

	assert.False(t, find, "test-container should not be present in the list of containers")
}

// runAndPopulateAzuriteContainer starts the Azurite container and populates it with a test container.
// The container created in this function is used to test the AzBlobClient operations.
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

	azureBlobClient = client

	_, err = azureBlobClient.UploadStream(context.TODO(), "test-container", "test-get-object", strings.NewReader("test"), nil)
	if err != nil {
		fmt.Printf("failed to upload the test object: %s\n", err)
	}

	testClient, err = filestorage.NewAzBlobClient(client, common.ConnectionProperties{})
	if err != nil {
		log.Fatalf("failed to create MinIO client: %s", err.Error())
	}
}
