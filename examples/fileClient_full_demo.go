package examples

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/tizianocitro/m2cs"
	"github.com/tizianocitro/m2cs/pkg/filestorage"
)

const (
	BucketName = "m2cs-demo-bucket"
	FileName   = "example_document.txt"
	FileBody   = "Hello, this is a test content for M2CS multi-cloud storage!"
)

func main() {
	ctx := context.Background()

	// --- 1. Initialize Individual Backend Clients ---

	// Setup MinIO
	minioClient := setupMinio()

	// Setup S3
	s3Client := setupS3()

	// Setup Azure Blob
	azClient := setupAzure()

	// --- 2. Prepare the Environment (Create Buckets/Containers) ---
	// Since M2CS wraps the SDKs, we can use the backend-specific methods
	// to ensure buckets exist before writing files.

	fmt.Println(">> Ensuring buckets/containers exist...")

	if err := minioClient.MakeBucket(ctx, BucketName); err != nil {
		log.Printf("MinIO Bucket check: %v", err) // Log but continue (might already exist)
	}

	if err := s3Client.CreateBucket(ctx, BucketName); err != nil {
		log.Printf("S3 Bucket check: %v", err)
	}

	if err := azClient.CreateContainer(ctx, BucketName); err != nil {
		log.Printf("Azure Container check: %v", err)
	}

	// --- 3. Initialize the Orchestrator (FileClient) ---
	// We use ASYNC_REPLICATION for speed and ROUND_ROBIN for reading.
	fmt.Println(">> Initializing FileClient Orchestrator...")

	fileClient := m2cs.NewFileClient(
		m2cs.ASYNC_REPLICATION,
		m2cs.ROUND_ROBIN,
		minioClient,
		s3Client,
		azClient,
	)

	// --- Configure a cache (optional) ---
	fileClient.ConfigureCache(
		m2cs.CacheOptions{
			Enabled:   true,
			MaxSizeMB: 500,
			TTL:       600,
			MaxItems:  10,
			ValidationStrategy: m2cs.SamplingValidationStrategy(
				10, 50),
		},
	)

	// --- 4. Upload a File (PutObject) ---
	fmt.Printf(">> Uploading file: %s\n", FileName)

	data := []byte(FileBody)
	err := fileClient.PutObject(ctx, BucketName, FileName, bytes.NewReader(data))
	if err != nil {
		log.Fatalf("Upload failed: %v", err)
	}
	fmt.Println(">> Upload initiated successfully (Async replication in progress).")

	// Wait a moment to ensure async replication finishes for this demo
	time.Sleep(2 * time.Second)

	// --- 5. Check Existence (ExistObject) ---
	exists, err := fileClient.ExistsObject(ctx, BucketName, FileName)
	if err != nil {
		log.Printf("Error checking existence: %v", err)
	}
	fmt.Printf(">> Does file exist? %v\n", exists)

	// --- 6. Download File (GetObject) ---
	// The Load Balancer (Round Robin) will pick one provider to download from.
	// If the file is encrypted or compressed on that specific provider,
	// M2CS handles the decoding transparently.
	fmt.Println(">> Downloading file...")

	reader, err := fileClient.GetObject(ctx, BucketName, FileName)
	if err != nil {
		log.Fatalf("Download failed: %v", err)
	}
	defer reader.Close()

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		log.Fatalf("Reading stream failed: %v", err)
	}
	fmt.Printf(">> Content Retrieved: %s\n", string(downloadedData))

	// --- 7. Clean Up (RemoveObject) ---
	fmt.Println(">> Deleting file from all providers...")
	err = fileClient.RemoveObject(ctx, BucketName, FileName)
	if err != nil {
		log.Fatalf("Deletion failed: %v", err)
	}
	fmt.Println(">> File deleted.")

	// -- Clear Cache --
	fileClient.ClearCache()

	// --- Disable Cache ---
	fileClient.DisableCache()
}

// --- Helper Functions ---

func setupMinio() *filestorage.MinioClient {
	// MinIO Configuration: Read-Only, No Encryption, No Compression
	client, err := m2cs.NewMinIOConnection(
		"localhost:9000", // Default endpoint
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("minioadmin", "minioadmin"),
			IsMainInstance:   false,
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.NO_COMPRESSION,
		},
		&minio.Options{Secure: false},
	)
	if err != nil {
		log.Fatalf("MinIO Connection failed: %v", err)
	}
	return client
}

func setupS3() *filestorage.S3Client {
	// S3 Configuration: GZIP Compression enabled
	client, err := m2cs.NewS3Connection(
		"", // Empty string uses default AWS config
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
			IsMainInstance:   true,
			SaveEncrypt:      m2cs.NO_ENCRYPTION,
			SaveCompress:     m2cs.GZIP_COMPRESSION, // Compress data on S3
		},
		"us-east-1",
	)
	if err != nil {
		log.Fatalf("S3 Connection failed: %v", err)
	}
	return client
}

func setupAzure() *filestorage.AzBlobClient {
	// Azure Configuration: AES256 Encryption enabled
	encryptionKey := "your encryption key "

	client, err := m2cs.NewAzBlobConnection(
		"default",
		m2cs.ConnectionOptions{
			ConnectionMethod: m2cs.ConnectWithCredentials("myAccount", "myKey"),
			IsMainInstance:   true,
			SaveEncrypt:      m2cs.AES256_ENCRYPTION,
			EncryptKey:       encryptionKey, // Required for Encryption
			SaveCompress:     m2cs.NO_COMPRESSION,
		},
	)
	if err != nil {
		log.Fatalf("Azure Connection failed: %v", err)
	}
	return client
}
