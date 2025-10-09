# M²CS: A library for multi/hybrid cloud storage

M²CS is a GoLang library designed to simplify the management and development of applications and services that integrate storage solutions in multi-cloud and hybrid-cloud environments. 

---

### Features
- Multi-provider support
- Unified API for CRUD operations
- Automatic data replication across backends
- Load Balancing strategy 
- Simplified management for creating connections to different backends
- Data transformation

---

### Installation

From your project directory:
```bash
go get github.com/tizianocitro/m2cs
```

---

### Supported Storage Services

Currently, M²CS supports three major object storage backends:

| Provider    | SDK                                                                 | Documentation                                                                                  |
|-------------|----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| Amazon S3   | [aws‑sdk‑go‑v2](https://github.com/aws/aws-sdk-go-v2)               | [docs.aws.amazon.com](https://docs.aws.amazon.com/sdk-for-go/)                                |
| Azure Blob  | [azure‑sdk‑for‑go](https://github.com/Azure/azure-sdk-for-go)       | [pkg.go.dev/azblob](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob)  |
| MinIO       | [minio‑go](https://github.com/minio/minio-go)                       | [docs.min.io](https://min.io/docs/minio/linux/developers/go/minio-go.html)                   |
---

### API Overview

M²CS exposes a unified interface to interact with multiple storage providers.
You can configure connections, choose replication and load balancing strategies,
and perform operations like uploading, downloading, and deleting files.

For the full API and advanced configuration options, see: 📄 [docs/api.md](./docs/api.md)

---

### Quick Example
```go
package main

import (
	"context"
	"log"

	"github.com/tizianocitro/m2cs"
)

func main() {
	// Initialize the FileClient with SYNC_REPLICATION and ROUND_ROBIN load balancing
	fileClient := m2cs.NewFileClient(
		m2cs.SYNC_REPLICATION,
		m2cs.ROUND_ROBIN,

		// ✅ Amazon S3 configuration
		m2cs.NewS3Connection(
			endpoint,
			m2cs.ConnectionOptions{
				ConnectionMethod: m2cs.ConnectWithCredentials("ACCESS_KEY", "SECRET_KEY"),
				IsMainInstance:   true,
			}),

		// ✅ Azure Blob Storage configuration
		m2cs.NewAzBlobConnection(
			endpoint,
			m2cs.ConnectionOptions{
				ConnectionMethod: m2cs.ConnectWithConnectionString("yourConnectionString"), 
				IsMainInstance:   true,
				SaveEncrypt:      m2cs.NO_ENCRYPTION,
				SaveCompress:     m2cs.NO_COMPRESSION,
			}),

		// ✅ MinIO configuration (e.g., local or on-premises)	
		m2cs.NewMinIOConnection(
			endpoint,
			m2cs.ConnectionOptions{
				ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
				IsMainInstance:   true,
				SaveEncrypt:      m2cs.AES256_ENCRYPTION,
				SaveCompress:     m2cs.GZIP_COMPRESSION,
			}),
	)

	// Upload a file (replicated across all providers)
	err := fileClient.PutObject(context.TODO(), "containers", "file name", Reader)
	if err != nil {
		log.Fatalf("Failed to upload file: %v", err)
	}

	log.Println("File successfully replicated across all providers!")s
}
```

---
### 📚 Additional Documentation

- [API Reference](docs/api.md)
- [Replication Strategies](docs/replication.md) 
- [Load Balancing Strategies](docs/loadbalancing.md)
- [ConnectionOptions ](docs/config.md) 
- [Examples](examples/) 

