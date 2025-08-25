package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

type GzipCompress struct{}

func (*GzipCompress) Name() string { return "gzip-compress" }

func (*GzipCompress) Apply(r io.Reader) (io.Reader, io.Closer, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	if _, err := io.Copy(zw, r); err != nil {
		_ = zw.Close()
		return nil, nil, fmt.Errorf("gzip: copy: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, nil, fmt.Errorf("gzip: close: %w", err)
	}

	return bytes.NewReader(buf.Bytes()), io.NopCloser(nil), nil
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
