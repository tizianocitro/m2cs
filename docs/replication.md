# Replication Strategies

M²CS currently supports two replication modes: `SYNC` and `ASYNC`.
Each strategy defines how the `FileClient` propagates writes across multiple backends configured as [`IsMainInstance = true`](#client-roles-main-vs-read-only).

---
### Synchronous Replication (`m2cs.SYNC_REPLICATION`)

In `m2cs.SYNC_REPLICATION` mode, the _`FileClient`_ attempts to write synchronously to all clients with `IsMainInstance = true`
- If all writes succeed: the operation is considered successful. 
- If all writes fail: an aggregated error is returned. 
- If some writes fail: the caller receives a detailed error indicating which backends failed.

This strategy ensures strong consistency, but is more sensitive to delays or failures from any provider.

---
### Asynchronous Replication (`m2cs.ASYNC_REPLICATION`)

In `m2cs.ASYNC_REPLICATION` mode, the `FileClient` ensures at least one successful write by attempting to write synchronously to each client (where `IsMainInstance = true`) until one succeeds. 

As soon as the first write is successful, the operation is marked as successful.

Then, asynchronous background writes are launched to all remaining write-enabled clients.
- You are guaranteed at least one completed replica.
- Failures in background replications are not propagated to the caller but are logged internally. 

This strategy balances low latency with eventual consistency, ensuring data durability without blocking the caller on all writes.

---
You can configure the replication mode during FileClient initialization:
```go
fileClient := m2cs.NewFileClient(
                m2cs.SYNC_REPLICATION, // <- Replication strategy
                m2cs.ROUND_ROBIN, 
                s3Client, azBlobClient, minioClient)
```