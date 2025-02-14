package main

import (
	"log"
	"m2cs"
)

func main() {
	//================================================================================================
	// Create a new connection with credentials
	conn, err := m2cs.NewAzBlobConnection("", // Use default url (https://%s.blob.core.windows.net)
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("accountName", "secretKey"),
			IsMainInstance:   true,
			SaveEncrypt:      false,
			SaveCompress:     false})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", conn) // azure blob connection is now setup

	//================================================================================================
	// Create a new connection with environment variables
	// in this case, the access key and secret key are set in the environment variables
	// AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY
	conn, err = m2cs.NewAzBlobConnection("default", // Use default url (https://%s.blob.core.windows.net)
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
			IsMainInstance:   true,
			SaveEncrypt:      false,
			SaveCompress:     false})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", conn) // azure blob connection is now setup

	//================================================================================================
	// Create a new connection with environment azure blob connection string
	conn, err = m2cs.NewAzBlobConnection("http://localhost:10000/accountName", // With connection string the endpoint is not considered
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithConnectionString("yourConnectionString"),
			IsMainInstance:   true,
			SaveEncrypt:      false,
			SaveCompress:     false})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", conn) // azure blob connection is now setup

}
