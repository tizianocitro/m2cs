package filestorage

import (
	"context"
	"io"
	common "m2cs/pkg"
)

type FileStorage interface {
	GetObject(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error)
	PutObject(ctx context.Context, storeBox string, fileName string, reader io.Reader) error
	RemoveObject(ctx context.Context, storeBox string, fileName string) error
	GetConnectionProperties() common.ConnectionProperties
}
