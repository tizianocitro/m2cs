# M²CS API Reference

This document provides a complete reference for all public methods exposed by the M²CS library.

---
## Table of Contents

### Backend Connection Setup
- [`NewAzBlobConnection()`](#newazblobconnection)
- [`NewMinIOConnection()`](#newminioconnection)
- [`NewS3Connection()`](#news3connection)
- [`NewFileClient()`](#newfileclient)

### Backend-Specific Client APIs
#### AzBlobClient
- [`CreateContainer()`](#createcontainer)
- [`DeleteContainer()`](#deletecontainer)
- [`ListContainers()`](#listcontainers)

#### MinioClient
- [`MakeBucket()`](#makebucket)
- [`RemoveBucket()`](#removebucket)
- [`ListBuckets()`](#listbuckets-minio)

#### S3Client
- [`CreateBucket()`](#createbucket)
- [`RemoveBucket()`](#removebucket-s3)
- [`ListBuckets()`](#listbuckets-s3)

### Common File Operations
These methods are exposed by all storage clients (S3, Azure, MinIO) and by the high-level `FileClient`:
- [`PutObject()`](#putobject)
- [`GetObject()`](#getobject)
- [`RemoveObject()`](#removeobject)

---

## Backend Connection Setup
M²CS offers two levels of abstraction to connect to storage providers:
- High-level connection functions, which simplify connection setup with defaults and sensible fallbacks.
- Low-level constructor functions, for advanced users who want to provide their own pre-configured SDK clients.


⚠️ **Important:**
All connection functions need a `ConnectionOptions` struct to configure how the backend should be accessed, stored, and transformed.


For a full explanation of available fields, see: [ConnectionOptions Reference](./config.md)

### High-Level Constructors
These are the main connection functions that configure clients for you based on provided `endpoint` and `ConnectionOptions`:

#### NewAzBlobConnection(...)

```go
func NewAzBlobConnection(endpoint string, options ConnectionOptions) (*filestorage.AzBlobClient, error)
```

Creates a new Azure Blob connection and wraps it in a `filestorage.AzBlobClient`.

If `endpoint` is empty or set to `"default"`, M²CS automatically uses:
```go
https://<accountName>.blob.core.windows.net
```

**Example:**
```go
func main() {
	
    azBlobClient, err := m2cs.NewAzBlobConnection("default",
        m2cs.ConnectionOptions{
            ConnectionMethod: m2cs.ConnectWithCredentials("accountName", "secretKey"),
            IsMainInstance:   true,
            SaveEncrypt:      m2cs.NO_ENCRYPTION,
            SaveCompress:     m2cs.GZIP_COMPRESSION})
	
    if err != nil {
        log.Fatalf("Failed to connect to Azure Blob Service: %v", err)
    }
}

```

#### NewMinIOConnection(...)
```go
func NewMinIOConnection(endpoint string, options ConnectionOptions, minioOptions *minio.Options) (*filestorage.MinioClient, error)
```
Creates a new MinIO connection and wraps it in a `filestorage.MinioClient`.

If `endpoint` is empty or set to `"default"`, M²CS defaults to:
```go
http://localhost:9000
```
and uses `minioOptions.Secure` to decide HTTP/HTTPS.

**Example:**
```go
func main() {
    minioClient, err := m2cs.NewMinIOConnection("",
        m2cs.ConnectionOptions{
            ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
            IsMainInstance:   true,
            SaveEncrypt:      m2cs.NO_ENCRYPTION,
            SaveCompress:     m2cs.NO_COMPRESSION},
            &minio.Options{Secure: false})
    
    if err != nil {
    log.Fatalf("Failed to connect to MinIO: %v", err)
    }
}
```

#### NewS3Connection(...)
```go
func NewS3Connection(endpoint string, options ConnectionOptions, awsRegion string) (*filestorage.S3Client, error)
```
Creates a new AWS S3 connection and wraps it in a `filestorage.S3Client`.

If `endpoint` is empty or set to `"default"`, M²CS uses default AWS configuration and region to build the client.

**Example:**
```go
func main() {
    s3Client, err := m2cs.NewS3Connection("",
        m2cs.ConnectionOptions{
            ConnectionMethod: m2cs.ConnectWithEnvCredentials(),
            IsMainInstance:   true,
            SaveEncrypt:      m2cs.AES256_ENCRYPTION,
            SaveCompress:     m2cs.NO_COMPRESSION},
            "us-east-1")
    
    if err != nil {
    log.Fatalf("Failed to connect to S3: %v", err)
    }
}
```
#### NewFileClient(...)
```go
func NewFileClient(replication m2cs.ReplicationStrategy, loadBalancing m2cs.LoadBalancingStrategy, clients ...) (*FileClient, error)
```
Creates a new high-level `FileClient` orchestrator that wraps multiple backends.

**Example:**
```go
fileClient := m2cs.NewFileClient(
    m2cs.SYNC_REPLICATION,
    m2cs.ROUND_ROBIN,
    s3Client, minioClient, azBlobClient)
```

The client will write and replicate files across backends according to the specified strategies. For more details:
- See [Replication Strategies](./replication.md) for how data is propagated.
- See [Load Balancing](./loadbalancing.md) Strategies for how read/write requests are distributed.



### Low-Level SDK Injection
If you already have a fully configured client instance from the underlying SDKs (Azure Blob, MinIO, S3), you can directly wrap it using the corresponding constructor from the `filestorage` package:
```go
NewAzBlobClient(client *azblob.Client, properties common.ConnectionProperties) (*AzBlobClient, error)
NewMinioClient(client *minio.Client, properties common.ConnectionProperties) (*MinioClient, error)
NewS3Client(client *s3.Client, properties common.ConnectionProperties) (*S3Client, error)
```
This allows complete control over:
- Custom retry policies, HTTP proxy, or middleware
- Credential sourcing (e.g., STS, IAM roles, token refresh)
- Transport-level tuning (custom http.RoundTripper, TLS config, etc.)

**Example (MinIO):**
```go
sdkClient, err := minio.New("localhost:9000", 
	&minio.Options{
	    Creds:  credentials.NewStaticV4("accessKey", "secretKey", ""), 
		Secure: false })

if err != nil {
    log.Fatal(err)
}

client, err := filestorage.NewMinioClient(sdkClient, 
	common.ConnectionProperties{
        IsMainInstance: true,
        SaveEncrypt:    m2cs.NO_ENCRYPTION,
        SaveCompress:   m2cs.GZIP_COMPRESSION })
```
---
## Backend-Specific Client APIs

### AzBlobClient

#### CreateContainer(...)

`CreateContainer(ctx context.Context, containerName string) error `

Creates a new container in Azure Blob Storage.

| Param        | Type              | Description                               |
|--------------|-------------------|-------------------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.         |
| `containerName` | `string`        | The name of the container to be created.  |

**Example:**
```go
err := azBlobClient.CreateContainer(context.Background(), "mycontainer")
if err != nil {
    log.Fatalf("Failed to create container: %v", err)
}
```
#### DeleteContainer(...)

`DeleteContainer(ctx context.Context, containerName string) error`: 

Deletes an existing container from Azure Blob Storage.

| Param        | Type              | Description                               |
|--------------|-------------------|-------------------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.         |
| `containerName` | `string`        | The name of the container to be deleted. |
|

**Example:**
```go
err := azBlobClient.DeleteContainer(context.Background(), "mycontainer")
if err != nil {
    log.Fatalf("Failed to delete container: %v", err)
}
```

#### ListContainers(...)

`ListContainers() ([]string, error) `: 

Lists all containers in Azure Blob Storage, returning a vector of container names and creation dates in the format `Name: %s CreatedOn: %s`.

**Example:**
```go
containers, err := azBlobClient.ListContainers()
if err != nil {
    log.Fatalf("Failed to list containers: %v", err)
}

for _, container := range containers {
    fmt.Println("Container:", container)
}
```

### MinioClient

#### MakeBucket(...)

`MakeBucket(ctx context.Context, bucketName string) error`: 

Creates a new bucket in MinIO.

| Param        | Type              | Description                           |
|--------------|-------------------|---------------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.     |
| `bucketName` | `string`        | The name of the bucket to be created. |

**Example:**
```go
err := minioClient.MakeBucket(context.Background(), "mybucket")
if err != nil {
    log.Fatalf("Failed to create bucket: %v", err)
}
```

#### RemoveBucket(...)

`RemoveBucket(ctx context.Context, bucketName string) error `: 

Removes an existing bucket in MinIO.

| Param        | Type              | Description                          |
|--------------|-------------------|--------------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.    |
| `bucketName` | `string`        | The name of the bucket to be remove. |

#### ListBuckets(...)

`ListBuckets(ctx context.Context) ([]string, error)`: 

Lists all containers in MinIO, returning a vector of container names and creation dates in the format `Name: %s CreatedOn: %s`.

| Param        | Type              | Description                          |
|--------------|-------------------|--------------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.    |

**Example:**
```go
buckets, err := minioClient.ListBuckets(context.Background())
if err != nil {
    log.Fatalf("Failed to list buckets: %v", err)
}

for _, bucket := range buckets {
    fmt.Println("Bucket:", bucket)
}
```

### S3Client

#### CreateBucket(...)

`CreateBucket(ctx context.Context, bucketName string) error`: 

Creates a new bucket in Amazon S3.

| Param        | Type              | Description                      |
|--------------|-------------------|----------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.|
| `bucketName` | `string`        | The name of the bucket to be created. |  

**Example:**
```go
err := s3Client.CreateBucket(context.Background(), "mybucket")
if err != nil {
    log.Fatalf("Failed to create bucket: %v", err)
}
```

#### RemoveBucket(...)

`RemoveBucket(ctx context.Context, bucketName string) error `: 

Removes an existing bucket from Amazon S3.

| Param        | Type              | Description                           |
|--------------|-------------------|---------------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.     |
| `bucketName` | `string`        | The name of the bucket to be removed. |

**Example:**
```go
err := s3Client.RemoveBucket(context.Background(), "mybucket")
if err != nil {
    log.Fatalf("Failed to remove bucket: %v", err)
}
```

#### ListBuckets(...)

`ListBuckets(ctx context.Context) ([]string, error)`: 

Lists all buckets in Amazon S3, returning a vector of container names and creation dates in the format `Name: %s CreatedOn: %s`.

| Param        | Type              | Description                          |
|--------------|-------------------|--------------------------------------|
| `ctx`        | `context.Context`  | Context for timeout/cancellation.    |

**Example:**
```go
buckets, err := s3Client.ListBuckets(context.Background())
if err != nil {
    log.Fatalf("Failed to list buckets: %v", err)
}

for _, bucket := range buckets {
    fmt.Println("Bucket:", bucket)
}
```

---

## Common File Operations

These operations are exposed by all individual storage clients (S3Client, AzBlobClient, MinioClient) as well as by the high-level orchestrator FileClient.

When invoked through `FileClient`, these methods apply the replication and load balancing strategies configured during client initialization.
- `PutObject(...)` → applies the replication strategy
- `GetObject(...)` → apply the load balancing strategy

For full details, refer to:
- [Replication Strategies](./replication.md) 
- [Load Balancing Strategies](./loadbalancing.md)

### PutObject(...)

```go
PutObject(ctx context.Context, storeBox string, fileName string, reader io.Reader) error
```

Uploads a file to the specified backend. If invoked via `FileClient`, the object will be replicated across multiple storage backends according to the current replication strategy (`SYNC_REPLICATION`, `ASYNC_REPLICATION`, etc.).

| Param      | Type              | Description                                              |
|------------|-------------------|----------------------------------------------------------|
| `ctx`      | `context.Context` | Context for timeout/cancellation.                        |
| `storeBox` | `string`          | Name of the bucket/container where the file is uploaded. |
| `fileName` | `string`          | Name of the file to upload.                              |
| `reader`   | `io.Reader`       | Input stream of file content.                            |


### GetObject(...)

```go
GetObject(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error)
```

Downloads a file from storage.
When used with FileClient, it uses the load balancing strategy to select the appropriate backend for reading the file.

| Param      | Type              | Description                                                |
|------------|-------------------|------------------------------------------------------------|
| `ctx`      | `context.Context` | Context for timeout/cancellation.                          |
| `storeBox` | `string`          | Name of the bucket/container where the file is downloaded. |
| `fileName` | `string`          | Name of the file to download.                              |

### RemoveObject()

```go
RemoveObject(ctx context.Context, storeBox string, fileName string) error
```

Deletes the specified object from storage.

| Param      | Type              | Description                                                |
|------------|-------------------|------------------------------------------------------------|
| `ctx`      | `context.Context` | Context for timeout/cancellation.                          |
| `storeBox` | `string`          | Name of the bucket/container where the file to be deleted. |
| `fileName` | `string`          | Name of the file to be deleted.                            |