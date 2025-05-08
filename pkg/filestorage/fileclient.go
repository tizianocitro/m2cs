package filestorage

import (
	"context"
	"io"
)

type FileStorage interface {
	GetObject(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error)
	PutObject(ctx context.Context, objectName string, reader io.Reader) error
	RemoveObject(ctx context.Context, storeBox string, fileName string) error
	ListObjects(ctx context.Context, storeBox string) ([]string, error)
}
