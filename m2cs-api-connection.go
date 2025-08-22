package m2cs

import (
	"fmt"
	"github.com/minio/minio-go/v7"
	"m2cs/internal/connection"
	connfilestorage "m2cs/internal/connection/filestorage"
	common "m2cs/pkg"
	"m2cs/pkg/filestorage"
)

// ConnectionOptions holds the options for creating a connection.
// parameters:
// - ConnectionMethod: The method used to establish the connection.
// - IsMainInstance:Indicates if this is the main instance.
// - SaveEncrypt: Indicates if the data should be saved with encryption.
// - SaveCompress: Indicates if the data should be saved with compression.
type ConnectionOptions struct {
	ConnectionMethod connectionFunc
	IsMainInstance   bool
	SaveEncrypt      EncryptionAlgorithm
	SaveCompress     CompressionAlgorithm
}

type connectionFunc *connection.AuthConfig

// NewMinIOConnection creates a new MinIO connection.
// It takes an endpoint, connection options, and optional MinIO options.
// It returns a MinioConnection or an error if the connection could not be established.
func NewMinIOConnection(endpoint string, connectionOptions ConnectionOptions, minioOptions *minio.Options) (*filestorage.MinioClient, error) {
	var authConfing *connection.AuthConfig = connectionOptions.ConnectionMethod
	if authConfing == nil {
		return nil, fmt.Errorf("connectionMethod cannot be nil")
	}

	if authConfing.GetConnectType() != "withCredential" && authConfing.GetConnectType() != "withEnv" {
		return nil, fmt.Errorf("invalid connection method for MinIO; use: ConnectWithCredentials or ConnectWithEnvCredentials")
	}

	authConfing.SetProperties(common.Properties{
		IsMainInstance: connectionOptions.IsMainInstance,
		SaveEncrypted:  connectionOptions.SaveEncrypt,
		SaveCompressed: connectionOptions.SaveCompress,
	})

	minioConn, err := connfilestorage.CreateMinioConnection(endpoint, authConfing, minioOptions)
	if err != nil {
		return nil, err
	}

	return minioConn, nil
}

func NewAzBlobConnection(endpoint string, connectionOptions ConnectionOptions) (*filestorage.AzBlobClient, error) {
	var authConfing *connection.AuthConfig = connectionOptions.ConnectionMethod
	if authConfing == nil {
		return nil, fmt.Errorf("connectionMethod cannot be nil")
	}

	if authConfing.GetConnectType() != "withCredential" &&
		authConfing.GetConnectType() != "withEnv" &&
		authConfing.GetConnectType() != "withConnectionString" {
		return nil, fmt.Errorf("invalid connection method for Azure Blob; " +
			"use: ConnectWithCredentials, ConnectWithEnvCredentials or ConnectWithConnectionString")
	}

	authConfing.SetProperties(common.Properties{
		IsMainInstance: connectionOptions.IsMainInstance,
		SaveEncrypted:  connectionOptions.SaveEncrypt,
		SaveCompressed: connectionOptions.SaveCompress,
	})

	azBlobConn, err := connfilestorage.CreateAzBlobConnection(endpoint, authConfing)
	if err != nil {
		return nil, err
	}

	return azBlobConn, nil
}

func NewS3Connection(endpoint string, connectionOptions ConnectionOptions, awsRegion string) (*filestorage.S3Client, error) {
	var authConfing *connection.AuthConfig = connectionOptions.ConnectionMethod
	if authConfing == nil {
		return nil, fmt.Errorf("connectionMethod cannot be nil")
	}

	if authConfing.GetConnectType() != "withCredential" &&
		authConfing.GetConnectType() != "withEnv" {
		return nil, fmt.Errorf("invalid connection method for AWS S3; " +
			"use: ConnectWithCredentials or ConnectWithEnvCredentials")
	}

	authConfing.SetProperties(common.Properties{
		IsMainInstance: connectionOptions.IsMainInstance,
		SaveEncrypted:  connectionOptions.SaveEncrypt,
		SaveCompressed: connectionOptions.SaveCompress,
	})

	s3Conn, err := connfilestorage.CreateS3Connection(endpoint, authConfing, awsRegion)
	if err != nil {
		return nil, err
	}

	return s3Conn, nil
}

// ConnectWithCredentials returns a connectionFunc configured with the provided credentials.
func ConnectWithCredentials(identity string, secretAccessKey string) connectionFunc {
	authConfig := connection.NewAuthConfig() // Usa la funzione per creare l'oggetto
	authConfig.SetConnectType("withCredential")
	authConfig.SetAccessKey(identity)
	authConfig.SetSecretKey(secretAccessKey)
	return authConfig
}

// ConnectWithEnvCredentials returns a connectionFunc configured to use environment credentials.
func ConnectWithEnvCredentials() connectionFunc {
	authConfig := &connection.AuthConfig{}
	authConfig.SetConnectType("withEnv")
	return authConfig
}

// ConnectWithEnvCredentials returns a connectionFunc configured with the connection string.
func ConnectWithConnectionString(connectionString string) connectionFunc {
	authConfig := &connection.AuthConfig{}
	authConfig.SetConnectType("withConnectionString")
	authConfig.SetConnectionString(connectionString)
	return authConfig
}

// ReplicationMode defines the replication modes for file storage.
// SYNC_REPLICATION indicates that the replication is synchronous.
// ASYNC_REPLICATION indicates that the replication is asynchronous.
type ReplicationMode int

const (
	SYNC_REPLICATION ReplicationMode = iota
	ASYNC_REPLICATION
)

// Re-export types (type alias)
type CompressionAlgorithm = common.CompressionAlgorithm
type EncryptionAlgorithm = common.EncryptionAlgorithm

// Re-export constants
const (
	NO_COMPRESSION   = common.NO_COMPRESSION
	GZIP_COMPRESSION = common.GZIP_COMPRESSION

	NO_ENCRYPTION     = common.NO_ENCRYPTION
	AES256_ENCRYPTION = common.AES256_ENCRYPTION
)
