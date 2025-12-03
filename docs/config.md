# ConnectionOptions Reference

The `ConnectionOptions` struct is used to configure how M²CS connects to a specific backend and how data is stored (encryption, compression, etc.).

---
### Struct Definition

```go
// ConnectionOptions holds the options for creating a connection.
// parameters:
// - ConnectionMethod: The method used to establish the connection.
// - IsMainInstance: Indicates if this is the main instance.
// - SaveEncrypt: Indicates if the data should be saved with encryption.
// - SaveCompress: Indicates if the data should be saved with compression.
// - EncryptKey: Optional key for encryption, if needed.
type ConnectionOptions struct {
    ConnectionMethod connectionFunc
    IsMainInstance   bool
    SaveEncrypt      EncryptionAlgorithm
    SaveCompress     CompressionAlgorithm
    EncryptKey       string // Optional key for encryption, if needed
}
```
---

### Authentication Methods (`ConnectionMethod`)

Note that not all methods are supported by every backend:

- `m2cs.ConnectWithCredentials(identity string, secretAccessKey string) connectionFunc`
  - Basic access key / secret key authentication
  - Supported Backends: AWS S3, MinIO, Azure Blob
- `m2cs.ConnectWithEnvCredentials() connectionFunc `
  - Uses credentials from environment variables.
  - AWS S3 it looks for `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
  - MinIO it looks for `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`
  - Azure Blob it looks for `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`
- `m2cs.ConnectWithConnectionString(connectionString string) connectionFunc `
  - Use a connection string for creating a connection
  - Supported Backends: Azure Blob

---

### Client Roles: Main vs Read-Only

The connection can be configured in one of two modes using the `IsMainInstance` flag:

| Mode                    | Description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `IsMainInstance = true`  | Client is enabled for **both reading and writing**. Used in primary replication and writes. |
| `IsMainInstance = false` | Client operates in **read-only mode**. It will not perform writes. Useful for replicas or audit-only access. |

This distinction allows you to:
- Use different roles for different clients in a multi-cloud architecture
- Maintain isolation between writer and reader clients

---

### Compression and Encryption Strategies (`SaveCompress`/`SaveEncrypt`)

⚠️ **Important design note:**
In M²CS, compression and encryption logic is delegated to the individual backend client, not the central FileClient.
This means:
- Each connection knows at creation time whether and how it should apply compression and/or encryption.
- Multiple connections to the same backend can coexist with different transformation settings.
- It is the developer’s responsibility to track how files were saved and with which connection configuration.

This design ensures compatibility even in read-only scenarios: a client that didn’t write a file can still read and decode it, as long as it's correctly configured.

#### Compression Strategies
| Value                  | Description                                                    |
|------------------------|----------------------------------------------------------------|
| `m2cs.NO_COMPRESSION ` | No compression applied to the file                                 |
| `mc2c.GZIP_COMPRESSION`  | Applies Gzip compression algorithm to the file |

#### Encryption Strategies
| Value                  | Description                                      |
|------------------------|--------------------------------------------------|
| `m2cs.NO_ENCRYPTION `    | No encryption applied to the file                |
| `m2cs.AES256_ENCRYPTION` | Applies AES-256 encryption algorithm to the file |

If an encryption algorithm is selected, it is necessary to provide an encryption key via the `EncryptKey` parameter.