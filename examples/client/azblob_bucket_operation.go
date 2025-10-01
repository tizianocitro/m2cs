package main

//
//import (
//	"context"
//	"log"
//	"m2cs"
//	"m2cs/pkg/filestorage"
//	"strings"
//)
//
//func main() {
//	ctx := context.Background()
//	azClient := createAzBlobClient()
//
//	// Create a new container
//	err := azClient.CreateContainer(ctx, "my-new-container")
//	if err != nil {
//		log.Fatalf("Failed to create container: %v", err)
//	}
//	log.Println("Container created successfully")
//
//	// List all containers
//	containers, err := azClient.ListContainers()
//	if err != nil {
//		log.Fatalf("Failed to list containers: %v", err)
//	}
//	log.Println("Containers:")
//	for _, container := range containers {
//		log.Println(container)
//	}
//
//	// Upload a file to the container
//	err = azClient.PutObject(ctx, "my-new-container", "my-object", strings.NewReader("test"))
//	if err != nil {
//		log.Fatalf("Failed to upload object: %v", err)
//	}
//	log.Println("Object uploaded successfully")
//
//	// Download the file from the container
//	reader, err := azClient.GetObject(ctx, "my-new-container", "my-object")
//	if err != nil {
//		log.Fatalf("Failed to download object: %v", err)
//	}
//	defer reader.Close()
//	log.Println("Object downloaded successfully")
//
//	// Delete the object from the container
//	err = azClient.RemoveObject(ctx, "my-new-container", "my-object")
//	if err != nil {
//		log.Fatalf("Failed to delete object: %v", err)
//	}
//	log.Println("Object deleted successfully")
//
//	// Delete the container
//	err = azClient.DeleteContainer(ctx, "my-new-container")
//	if err != nil {
//		log.Fatalf("Failed to delete container: %v", err)
//	}
//	log.Println("Container deleted successfully")
//
//}
//
//func createAzBlobClient() *filestorage.AzBlobClient {
//	client, err := m2cs.NewAzBlobConnection(
//		"",
//		m2cs.ConnectionOptions{
//			ConnectionMethod: m2cs.ConnectWithConnectionString("your-connection-string-here"),
//			SaveEncrypt:      false,
//			SaveCompress:     false,
//			IsMainInstance:   true,
//		})
//
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	return client
//}
