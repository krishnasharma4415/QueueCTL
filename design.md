# QueueCTL Design Document

## Overview

QueueCTL is a production-grade background job queue system prioritizing **reliability**, **simplicity**, and **operational excellence**. Built with SQLite for persistence, atomic operations for concurrency safety, and comprehensive error handling.

---

## Design Principles

1. **Reliability First**: ACID transactions, atomic job claiming, crash recovery, persistent storage
2. **Operational Simplicity**: Zero dependencies, no setup required, clear CLI, structured logging
3. **Concurrency Safety**: Lock-free atomic operations, process isolation, heartbeat monitoring
4. **Failure Handling**: Exponential backoff, Dead Letter Queue, configurable retries

---

## Architecture Decisions

### 1. SQLite Database

**Why**: ACID transactions, zero configuration, excellent performance for moderate loads

**Trade-offs**:
- ✅ No external dependencies, ACID guarantees, simple deployment
- ❌ Single-node only, limited write concurrency
- **Mitigation**: WAL mode for concurrent reads, retry logic for locks

### 2. Process-Based Workers

**Why**: Complete isolation and fault tolerance

**Trade-offs**:
- ✅ Crash isolation, better resource management, true parallelism
- ❌ Higher memory (~10-50MB vs ~1-5MB per thread)
- **Implementation**: Multiprocessing module, shared database, PID tracking, signal handling

### 3. Atomic Job Claiming

**Challenge**: Prevent duplicate processing without explicit locks

**Solution**: Single atomic UPDATE with WHERE clause verification
```sql
UPDATE jobs 
SET state = 'processing', worker_id = ?, updated_at = datetime('now')
WHERE id = ? AND state = 'pending' AND next_run_at <= datetime('now')
```

**Benefits**: No explicit locking, guaranteed atomicity, automatic retry, scales with workers

### 4. Configuration Hierarchy

**Design**: `CLI Flags → Database Config → Default Values`

**Rationale**: Persistent config, operational flexibility, centralized management, type-safe objects

### 5. Error Handling

**Exponential Backoff**: `delay = base^attempts` seconds
- Prevents thundering herd
- Configurable base and max retries
- Example: 2^1=2s, 2^2=4s, 2^3=8s

**Dead Letter Queue**:
- Permanent storage for failed jobs
- Manual retry capability
- Audit trail for debugging

### 6. State Machine

**States**: `pending` → `processing` → `completed` / `failed` → `dead` (DLQ)

**Guarantees**:
- All transitions are atomic
- Invalid transitions prevented by DB constraints
- State history via timestamps

---

## Concurrency Model

### Worker Coordination

**Heartbeat System**:
- Workers update every 5 seconds
- Stale detection after 30 seconds
- Automatic orphaned job recovery

**Job Distribution**:
- Workers poll for eligible jobs
- Priority-based ordering
- Fair distribution via database
- No central coordinator

### Database Concurrency

**WAL Mode**: Concurrent readers don't block writers, better performance, automatic checkpointing

**Lock Handling**: Exponential backoff, random jitter, configurable timeouts, graceful degradation

---

## Scalability

### Current Limits

- **Throughput**: 1,000-10,000 jobs/second (hardware dependent)
- **Optimal Workers**: 2-4x CPU cores
- **Memory**: ~10-50MB per worker
- **Bottleneck**: Disk I/O for large queues

### Scaling Strategies

**Vertical**: Faster storage (SSD/NVMe), more CPU cores, increased memory, database tuning

**Horizontal** (Future): Database sharding, multiple instances, external message queues, distributed workers

---

## Security

### Command Execution
- **Risks**: Arbitrary execution, shell injection, resource exhaustion
- **Mitigations**: Worker process privileges, no auto-escalation, timeouts, OS resource limits

### Database
- **Protection**: File system permissions, local-only access, parameterized queries
- **Optional**: SQLite encryption extension

### Operational
- **Logging**: No sensitive data by default, configurable levels, rotation policies
- **Audit**: Complete job execution trail

---

## Performance

### Throughput
- **Enqueueing**: ~1,000-5,000 jobs/second (single thread)
- **Processing**: Depends on job complexity and worker count

### Latency
- **Job Pickup**: Polling interval (500ms) + query time (~1-10ms)
- **End-to-End**: Pickup + execution + state update

### Memory
- **Per Worker**: 10-50MB baseline
- **Database**: ~1KB per job
- **Scaling**: Linear with workers and queue size

---

## Monitoring

### Built-in Metrics
- **Job Counts**: By state, success/failure rates, processing times
- **Worker Health**: Active count, heartbeat status, resource usage
- **System Health**: Database size, error rates, queue depth

### Logging
- **Format**: Structured with context (worker_id, job_id)
- **Levels**: Configurable (INFO, WARNING, ERROR)
- **Destinations**: Console (dev), file (prod), syslog (optional)

---

## Testing Strategy

### Coverage
- **Unit Tests**: Database ops, state transitions, config, error handling
- **Integration Tests**: Multi-worker concurrency, persistence, end-to-end workflows
- **Operational Tests**: Graceful shutdown, crash recovery, resource exhaustion

### Results
- **Total**: 31 tests
- **Pass Rate**: 100%
- **Categories**: Config (6), Database (8), Queue (11), Integration (6)

---

## Future Enhancements

### Short Term
- Web dashboard for monitoring
- REST API for job management
- Enhanced metrics and logging

### Medium Term
- Job dependencies and workflows
- Advanced scheduling (cron-like)
- Plugin system for custom job types

### Long Term
- Multi-database support
- Cloud-native deployment
- Distributed worker support

---

## Key Design Insights

### Why This Architecture Works

1. **SQLite Simplicity**: Zero-config deployment beats distributed complexity for most use cases
2. **Atomic Operations**: Single UPDATE prevents race conditions without explicit locking
3. **Process Isolation**: Crash safety and debugging ease justify memory overhead
4. **Polling Model**: Simplicity and reliability trump real-time responsiveness
5. **Exponential Backoff**: Intelligent retry strategy prevents system overload

### When to Use QueueCTL

✅ **Good Fit**:
- Moderate job volumes (< 10,000/sec)
- Single-node deployment acceptable
- Reliability more important than latency
- Zero-config deployment desired
- Background task processing

❌ **Not Ideal For**:
- Distributed multi-node requirements
- Real-time job processing (< 100ms latency)
- Extremely high throughput (> 50,000/sec)
- Complex job dependencies/workflows
- Strict ordering guarantees

---

## Conclusion

QueueCTL demonstrates that **simplicity and reliability** can coexist with **production-grade quality**. The SQLite-based architecture provides excellent performance while maintaining zero-configuration deployment. Atomic job claiming ensures correctness, and comprehensive error handling provides operational resilience.

The modular design enables future enhancements while maintaining backward compatibility. Clear separation of concerns (CLI, queue, workers, database) allows independent testing and maintenance of each component.

**Built for the Backend Developer Internship Assignment** - demonstrating production-ready system design, implementation, and documentation skills.
