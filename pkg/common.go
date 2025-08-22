package common

// ConnectionProperties defines the properties for a connection.
// IsMainInstance indicates if this is the main instance (can read and write).
// SaveEncrypt indicates if data should be saved in an encrypted format.
// SaveCompress indicates if data should be saved in a compressed format.
type ConnectionProperties struct {
	IsMainInstance bool
	SaveEncrypt    EncryptionAlgorithm
	SaveCompress   CompressionAlgorithm
}

type CompressionAlgorithm int

const (
	NO_COMPRESSION CompressionAlgorithm = iota
	GZIP_COMPRESSION
)

type EncryptionAlgorithm int

const (
	NO_ENCRYPTION EncryptionAlgorithm = iota
	AES256_ENCRYPTION
)

type Properties struct {
	IsMainInstance bool
	SaveEncrypted  EncryptionAlgorithm
	SaveCompressed CompressionAlgorithm
}
