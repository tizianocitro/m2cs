package main

import (
	"context"
	"github.com/minio/minio-go/v7"
	"log"
	"m2cs"
	"m2cs/pkg/filestorage"
	"strings"
)

func main() {
	ctx := context.Background()
	minioClient := createMinioClient()

	// Create a new bucket
	err := minioClient.MakeBucket(ctx, "my-new-bucket")
	if err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}
	log.Println("Bucket created successfully")

	// List all buckets
	buckets, err := minioClient.ListBuckets(ctx)
	if err != nil {
		log.Fatalf("Failed to list buckets: %v", err)
	}
	log.Println("Buckets:")
	for _, bucket := range buckets {
		log.Println(bucket)
	}

	// Upload a file to the bucket
	err = minioClient.PutObject(ctx, "my-new-bucket", "my-object", strings.NewReader("test"))
	if err != nil {
		log.Fatalf("Failed to upload object: %v", err)
	}
	log.Println("Object uploaded successfully")

	// Download the file from the bucket
	reader, err := minioClient.GetObject(ctx, "my-new-bucket", "my-object")
	if err != nil {
		log.Fatalf("Failed to download object: %v", err)
	}
	defer reader.Close()
	log.Println("Object downloaded successfully")

	// Delete the object from the bucket
	err = minioClient.RemoveObject(ctx, "my-new-bucket", "my-object")
	if err != nil {
		log.Fatalf("Failed to delete object: %v", err)
	}
	log.Println("Object deleted successfully")

	// Delete the bucket
	err = minioClient.RemoveBucket(ctx, "my-new-bucket")
	if err != nil {
		log.Fatalf("Failed to delete bucket: %v", err)
	}
	log.Println("Bucket deleted successfully")
}

func createMinioClient() *filestorage.MinioClient {
	client, err := m2cs.NewMinIOConnection(
		"",
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("m2csUser", "m2csPassword"),
			SaveEncrypt:      false,
			SaveCompress:     false,
			IsMainInstance:   true,
		},
		&minio.Options{},
	)

	if err != nil {
		log.Fatalln(err)
	}

	return client
}
