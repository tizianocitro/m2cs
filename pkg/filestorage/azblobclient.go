package filestorage

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	common "github.com/tizianocitro/m2cs/pkg"
	"github.com/tizianocitro/m2cs/pkg/transform"
)

type AzBlobClient struct {
	client     *azblob.Client
	properties common.ConnectionProperties
}

func NewAzBlobClient(client *azblob.Client, properties common.ConnectionProperties) (*AzBlobClient, error) {
	if client == nil {
		return nil, fmt.Errorf("failed to create AzBlobClient: client is nil")
	}

	pager := client.NewListContainersPager(nil)
	_, err := pager.NextPage(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to azure blob: %w", err)
	}

	return &AzBlobClient{
		client:     client,
		properties: properties,
	}, nil
}

func (a *AzBlobClient) GetClient() *azblob.Client {
	return a.client
}

func (a *AzBlobClient) CreateContainer(ctx context.Context, containerName string) error {
	_, err := a.client.CreateContainer(ctx, containerName, nil)
	if err != nil {
		return err
	}

	return nil
}

func (a *AzBlobClient) DeleteContainer(ctx context.Context, containerName string) error {
	_, err := a.client.DeleteContainer(ctx, containerName, nil)
	if err != nil {
		return err
	}

	return nil
}

func (a *AzBlobClient) ListContainers() ([]string, error) {
	pager := a.client.NewListContainersPager(&azblob.ListContainersOptions{
		Include: azblob.ListContainersInclude{Metadata: true},
	})

	var containers []string
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		for _, container := range resp.ContainerItems {
			containers = append(containers, fmt.Sprintf("Name: %s, CreatedOn: %s", *container.Name, container.Properties.LastModified))
		}
	}
	return containers, nil
}

func (a *AzBlobClient) GetObject(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error) {

	pipe, err := transform.Factory{}.BuildRPipelineDecryptDecompress(a.properties, a.properties.EncryptKey)
	if err != nil {
		return nil, fmt.Errorf("build read pipeline: %w", err)
	}

	get, err := a.client.DownloadStream(ctx, storeBox, fileName, nil)
	if err != nil {
		return nil, err
	}

	retryReader := get.NewRetryReader(ctx, &azblob.RetryReaderOptions{})

	obj, err := pipe.Apply(retryReader)
	if err != nil {
		return nil, fmt.Errorf("fail to transform reader: %w", err)
	}

	return obj, nil
}

func (a *AzBlobClient) PutObject(ctx context.Context, storeBox, fileName string, reader io.Reader) error {
	if reader == nil {
		return fmt.Errorf("reader is nil")
	}

	pipe, err := transform.Factory{}.BuildWPipelineCompressEncrypt(a.properties, a.properties.EncryptKey)
	if err != nil {
		return fmt.Errorf("build write pipeline: %w", err)
	}

	obj, closer, err := pipe.Apply(reader)
	if err != nil {
		return fmt.Errorf("apply write pipeline: %w", err)
	}

	if closer != nil {
		defer closer.Close()
	}

	_, err = a.client.UploadStream(ctx, storeBox, fileName, obj, nil)
	if err != nil {
		return fmt.Errorf("azure upload stream: %w", err)
	}

	return nil
}

func (a *AzBlobClient) RemoveObject(ctx context.Context, storeBox string, fileName string) error {
	_, err := a.client.DeleteBlob(ctx, storeBox, fileName, nil)
	if err != nil {
		return err
	}

	return nil
}

func (a *AzBlobClient) GetConnectionProperties() common.ConnectionProperties {
	return a.properties
}

func (a *AzBlobClient) ExistObject(ctx context.Context, storeBox string, fileName string) (bool, error) {
	pager := a.client.NewListBlobsFlatPager(storeBox, &azblob.ListBlobsFlatOptions{
		Prefix: &fileName,
	})
	if pager == nil {
		return false, fmt.Errorf("failed to create blob pager")
	}

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to list blobs: %w", err)
		}
		for _, blob := range resp.Segment.BlobItems {
			if blob.Name != nil && *blob.Name == fileName {
				return true, nil
			}
		}
	}
	return false, nil
}

func (a *AzBlobClient) ListObjects(ctx context.Context, storeBox string) ([]string, error) {
	pager := a.client.NewListBlobsFlatPager(storeBox, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})

	if pager == nil {
		return nil, fmt.Errorf("failed to create blob pager")
	}

	var blobs []string

	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("failed to list blobs: %w", err)
		}

		for _, blob := range resp.Segment.BlobItems {
			blobs = append(blobs, fmt.Sprintf("Name: %s, LastModified: %s", *blob.Name, blob.Properties.LastModified))
		}
	}
	return blobs, nil
}
