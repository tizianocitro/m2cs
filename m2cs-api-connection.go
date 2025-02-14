package m2cs

import (
	"fmt"
	"github.com/minio/minio-go/v7"
	"m2cs/internal/connection"
	connfilestorage "m2cs/internal/connection/filestorage"
)

// ConnectionOptions holds the options for creating a connection.
// parameters:
// - ConnectionMethod: The method used to establish the connection.
// - IsMainInstance:Indicates if this is the main instance.
// - SaveEncrypt:  Indicates if the data should be saved with encryption.
// - SaveCompress: Indicates if the data should be saved with compression.
type ConnectionOptions struct {
	ConnectionMethod connectionFunc
	IsMainInstance   bool
	SaveEncrypt      bool
	SaveCompress     bool
}

type connectionFunc *connection.AuthConfig

// NewMinIOConnection creates a new MinIO connection.
// It takes an endpoint, connection options, and optional MinIO options.
// It returns a MinioConnection or an error if the connection could not be established.
func NewMinIOConnection(endpoint string, connectionOptions ConnectionOptions, minioOptions *minio.Options) (*connfilestorage.MinioConnection, error) {
	var authConfing *connection.AuthConfig = connectionOptions.ConnectionMethod
	if authConfing == nil {
		return nil, fmt.Errorf("ConnectionMethod cannot be nil")
	}

	if authConfing.GetConnectType() != "withCredential" && authConfing.GetConnectType() != "withEnv" {
		return nil, fmt.Errorf("invalid connection method for MinIO; use: ConnectWithCredentials or ConnectWithEnvCredentials")
	}

	authConfing.SetProperties(connection.Properties{
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

func NewAzBlobConnection(connectionOptions ConnectionOptions) (*connfilestorage.AzBlobConnection, error) {
	var authConfing *connection.AuthConfig = connectionOptions.ConnectionMethod
	if authConfing == nil {
		return nil, fmt.Errorf("ConnectionMethod cannot be nil")
	}

	if authConfing.GetConnectType() != "withCredential" &&
		authConfing.GetConnectType() != "withEnv" &&
		authConfing.GetConnectType() != "withConnectionString" {
		return nil, fmt.Errorf("invalid connection method for Azure Blob; " +
			"use: ConnectWithCredentials, ConnectWithEnvCredentials or ConnectWithConnectionString")
	}

	authConfing.SetProperties(connection.Properties{
		IsMainInstance: connectionOptions.IsMainInstance,
		SaveEncrypted:  connectionOptions.SaveEncrypt,
		SaveCompressed: connectionOptions.SaveCompress,
	})

	azBlobConn, err := connfilestorage.CreateAzBlobConnection(authConfing)

	if err != nil {
		return nil, err
	}

	return azBlobConn, nil
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
