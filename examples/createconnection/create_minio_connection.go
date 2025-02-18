package main

import (
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"log"
	"m2cs"
)

func main() {
	endpoint := "localhost:9000"
	accessKeyID := "m2csUser"
	secretAccessKey := "m2csPassword"
	useSSL := true

	//================================================================================================
	// Create a new connection with credentials
	conn, err := m2cs.NewMinIOConnection(endpoint, m2cs.ConnectionOptions{
		ConnectionMethod: m2cs.ConnectWithCredentials(accessKeyID, secretAccessKey),
		IsMainInstance:   true,
		SaveEncrypt:      false,
		SaveCompress:     false,
	}, &minio.Options{
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("%#v\n", conn) // minio connection is now setup

	//================================================================================================
	// create a new connection with environment variables
	// in this case, the access key and secret key are set in the environment variables
	// MINIO_ACCESS_KEY and MINIO_SECRET_KEY
	conn, err = m2cs.NewMinIOConnection(endpoint, m2cs.ConnectionOptions{
		ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
		IsMainInstance:   false,
		SaveEncrypt:      true,
		SaveCompress:     false,
	}, &minio.Options{
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("%#v\n", conn) // minio connection is now setup

	//================================================================================================
	// create a connection without specifying minio.Options
	// in this case, the connection will be created with the default options (Secure: false)
	conn, err = m2cs.NewMinIOConnection(endpoint, m2cs.ConnectionOptions{
		ConnectionMethod: m2cs.ConnectWithCredentials(accessKeyID, secretAccessKey),
		IsMainInstance:   true,
		SaveEncrypt:      false,
		SaveCompress:     false,
	}, nil)
}

// that is the standard way to create a minio connection without using the m2cs
func standardMinioConnection() {
	endpoint := "play.min.io"
	accessKeyID := "your-access-key-id"
	secretAccessKey := "your-secret-access"
	useSSL := true

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", minioClient) // minioClient is now setup
}
