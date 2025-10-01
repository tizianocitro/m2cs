package transform

import (
	"fmt"
	common "github.com/tizianocitro/m2cs/pkg"
	"github.com/tizianocitro/m2cs/pkg/transform/compression"
	"github.com/tizianocitro/m2cs/pkg/transform/encryption"
	"io"
)

// WriterTransform applies a write-time transformation to reader
type WriterTransform interface {
	Name() string
	Apply(reader io.Reader) (out io.Reader, closer io.Closer, err error)
}

// ReaderTransform applies a read-time inverse transformation to reader
type ReaderTransform interface {
	Name() string
	Apply(readerCloser io.ReadCloser) (out io.ReadCloser, err error)
}

// Pipeline for putObject.
type WritePipeline struct{ steps []WriterTransform }

func NewWritePipeline(steps ...WriterTransform) WritePipeline { return WritePipeline{steps: steps} }

func (p WritePipeline) Apply(reader io.Reader) (io.Reader, io.Closer, error) {
	var closers []io.Closer
	cur := reader
	for _, s := range p.steps {
		out, c, err := s.Apply(cur)
		if err != nil {
			for i := len(closers) - 1; i >= 0; i-- {
				_ = closers[i].Close()
			}
			return nil, nil, err
		}
		if c != nil {
			closers = append(closers, c)
		}
		cur = out
	}
	return cur, multiCloser(closers), nil
}

// Pipeline for read (inverse path).
type ReadPipeline struct{ steps []ReaderTransform }

func NewReadPipeline(steps ...ReaderTransform) ReadPipeline { return ReadPipeline{steps: steps} }

func (p ReadPipeline) Apply(readerCloser io.ReadCloser) (io.ReadCloser, error) {
	cur := readerCloser
	for _, s := range p.steps {
		out, err := s.Apply(cur)
		if err != nil {
			_ = cur.Close()
			return nil, err
		}
		cur = out
	}
	return cur, nil
}

type multiCloser []io.Closer

func (m multiCloser) Close() error {
	var first error
	for i := len(m) - 1; i >= 0; i-- {
		if err := m[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Factory builds write pipelines from backend properties and runtime key material.
type Factory struct{}

// BuildWPipelineCompressEncrypt returns a Pipeline that apply compress and encrypt algoritm to reader.
func (Factory) BuildWPipelineCompressEncrypt(props common.ConnectionProperties, encryptionKey string) (WritePipeline, error) {
	var steps []WriterTransform

	// 1) Compression
	switch props.SaveCompress {
	case common.NO_COMPRESSION:
		// no-op
	case common.GZIP_COMPRESSION:
		steps = append(steps, &compression.GzipCompress{})
	default:
		return WritePipeline{}, fmt.Errorf("unsupported compression algorithm: %v", props.SaveCompress)
	}

	// 2) Encryption
	switch props.SaveEncrypt {
	case common.NO_ENCRYPTION:
		// no-op
	case common.AES256_ENCRYPTION:
		if encryptionKey == "" {
			return WritePipeline{}, fmt.Errorf("missing encryption key for AES256_ENCRYPTION")
		}
		steps = append(steps, &encryption.AESGCMEncrypt{Key: encryptionKey})
	default:
		return WritePipeline{}, fmt.Errorf("unsupported encryption algorithm: %v", props.SaveEncrypt)
	}

	return NewWritePipeline(steps...), nil
}

func (Factory) BuildRPipelineDecryptDecompress(props common.ConnectionProperties, decryptionKey string) (ReadPipeline, error) {
	var steps []ReaderTransform

	// 1) Decryption
	switch props.SaveEncrypt {
	case common.NO_ENCRYPTION:
		// no-op
	case common.AES256_ENCRYPTION:
		if decryptionKey == "" {
			return ReadPipeline{}, fmt.Errorf("missing decryption key for AES256_ENCRYPTION")
		}
		steps = append(steps, &encryption.AESGCMDecrypt{Key: decryptionKey})
	default:
		return ReadPipeline{}, fmt.Errorf("unsupported encryption algorithm: %v", props.SaveEncrypt)
	}

	// 2) Decompression
	switch props.SaveCompress {
	case common.NO_COMPRESSION:
		// no-op
	case common.GZIP_COMPRESSION:
		steps = append(steps, &compression.GzipDecompress{})
	default:
		return ReadPipeline{}, fmt.Errorf("unsupported compression algorithm: %v", props.SaveCompress)
	}

	return NewReadPipeline(steps...), nil
}
