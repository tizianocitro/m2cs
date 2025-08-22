package compression

import (
	"compress/gzip"
	"fmt"
	"io"
)

// CompressGzip compresses the input data using gzip and returns a reader for the compressed data.
func CompressGzip(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)

	go func() {
		defer gw.Close()
		_, err := io.Copy(gw, r)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("compression failed: %w", err))
			return
		}
		pw.Close()
	}()

	return pr, nil
}

// DecompressGzip decompresses the input gzip-compressed data and returns a reader for the decompressed data.
func DecompressGzip(r io.Reader) (io.Reader, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	return gr, nil
}
