package main

import (
	"log"
	"m2cs"
)

func main() {
	//================================================================================================
	// Create a new connection with credentials
	conn, err := m2cs.NewS3Connection("default", // With "default" or "" it uses the default S3 endpoint
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("accessKeyId", "secretKeyId"),
			IsMainInstance:   true,
			SaveEncrypt:      false,
			SaveCompress:     true,
		},
		"") // "" is equivalent to "no-region"
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%#v\n", conn) // AWS S3 connection is now set up

	//================================================================================================
	// Create a new connection with environment variables
	// In this case, the access key and secret key are set in the environment variables
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	conn, err = m2cs.NewS3Connection("https://customEndpoint:4566",
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
			IsMainInstance:   true,
			SaveEncrypt:      false,
			SaveCompress:     true,
		},
		"us-east-1")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%#v\n", conn)
}
