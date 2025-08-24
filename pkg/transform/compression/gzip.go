package compression

import (
	"compress/gzip"
	"fmt"
	"io"
)

type GzipCompress struct{}

func (g *GzipCompress) Name() string { return "gzip" }

func (g *GzipCompress) Apply(reader io.Reader) (io.Reader, io.Closer, error) {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)
	go func() {
		defer gw.Close()
		defer pw.Close()
		if _, err := io.Copy(gw, reader); err != nil {
			_ = pw.CloseWithError(fmt.Errorf("gzip: %w", err))
		}
	}()
	return pr, pr, nil
}

type GzipDecompress struct{}

func (GzipDecompress) Name() string { return "gzip-decompress" }

func (GzipDecompress) Apply(readerCloser io.ReadCloser) (io.ReadCloser, error) {
	gr, err := gzip.NewReader(readerCloser)
	if err != nil {
		_ = readerCloser.Close()
		return nil, fmt.Errorf("gzip: %w", err)
	}

	return gr, nil
}
