# Load Balancing Strategies

M²CS supports configurable load balancing strategies to determine how GetObject requests are dispatched across multiple storage backends. When an object is replicated across different providers, it's important to:
- Minimize perceived latency 
- Ensure continuity in case of faults

This document describes the currently available load balancing strategies.

---
### `m2cs.READ_REPLICA_FIRST`

This strategy prioritizes non-main replicas when serving read requests.

Mechanism:
- Try all non-main backends first (in order)
- If all fail → fallback to main backends

Use case:
- Reduce contention on the main backend (often under write load)

### `m2cs.ROUND_ROBIN`

Distributes read requests evenly among all non-main backends.

Mechanism:
- Maintain internal rotating index (per storeBox)
- Select the next available non-main backend 
- If all fail → fallback to `READ_REPLICA_FIRST`

Use case:
- Balanced usage of resources 
- Avoids favoring a specific replica repeatedly

---

### Integration in FileClient

These strategies are applied automatically when using `FileClient.GetObject(...)`.

**Example:**
```go
fileClient := m2cs.NewFileClient(
                m2cs.SYNC_REPLICATION,
                m2cs.ROUND_ROBIN, // <- Load balancing strategy
                s3Client, azBlobClient, minioClient,...)

reader, err := fileClient.GetObject(ctx, "mybox", "report.pdf")
```

---

### Notes

- Load balancing applies only to read operations.
- In case of complete failure, the error is propagated to the caller 
- The strategy does not influence PutObject or replication order

For replication strategies, see: [replication.md](.\replication.md)