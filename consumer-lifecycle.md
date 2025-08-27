# Consumer Lifecycle Use Cases

## 1. Normal Consumer Join

```mermaid
sequenceDiagram
    participant C1 as Consumer 1
    participant NATS as NATS Server
    participant KV as KV Store
    participant C2 as Consumer 2

    Note over C1,NATS: Consumer 1 starts up
    C1->>NATS: Connect to NATS
    C1->>KV: Register worker
    C1->>KV: Claim partitions (0,1,2,3)
    loop Heartbeat every 2s
        C1->>KV: Renew leases for partitions (0,1,2,3)
    end
    
    Note over C2,NATS: Consumer 2 joins (triggers rebalance)
    C2->>NATS: Connect to NATS
    C2->>KV: Register worker
    C2->>KV: Check rebalance (detects 2 workers)
    C1->>KV: Heartbeat with partitions (0,1,2,3)
    C1->>KV: Check rebalance (detects 2 workers)
    
    Note over C1,C2: Rebalance occurs
    C1->>KV: Release partitions (2,3) [~1s]
    C2->>KV: Claim partitions (2,3) [~1s]
    
    loop Heartbeat every 2s
        C1->>KV: Renew leases for partitions (0,1)
        C2->>KV: Renew leases for partitions (2,3)
    end
```

**Timeline:**
- 0s: Consumer 2 starts
- 2s: Rebalance detected
- 3s: Partitions released and claimed
- **Total time for rebalance: ~3 seconds**

---

## 2. Normal Consumer Leave (Graceful Shutdown)

```mermaid
sequenceDiagram
    participant C1 as Consumer 1
    participant NATS as NATS Server
    participant KV as KV Store
    participant C2 as Consumer 2

    Note over C1,NATS: Both consumers running
    loop Heartbeat every 2s
        C1->>KV: Renew leases for partitions (0,1)
        C2->>KV: Renew leases for partitions (2,3)
    end
    
    Note over C1: Consumer 1 receives SIGTERM
    C1->>KV: Release all partitions (0,1) [~1s]
    C1->>KV: Remove worker registration [~100ms]
    C1->>NATS: Drain connection
    
    Note over C2: Consumer 2 detects missing worker
    C2->>KV: Heartbeat
    C2->>KV: Check active workers (sees only itself)
    C2->>KV: Check rebalance (detects 1 worker)
    
    Note over C2: Acquires released partitions
    C2->>KV: Claim partitions (0,1) [~1s]
    
    loop Heartbeat every 2s
        C2->>KV: Renew leases for partitions (0,1,2,3)
    end
```

**Timeline:**
- 0s: Consumer 1 receives SIGTERM
- 1s: Partitions released
- 3s: Consumer 2 detects missing worker
- 4s: Partitions claimed by Consumer 2
- **Total time for failover: ~4 seconds**

---

## 3. Force Consumer Leave (Process Killed)

```mermaid
sequenceDiagram
    participant C1 as Consumer 1
    participant NATS as NATS Server
    participant KV as KV Store
    participant C2 as Consumer 2

    Note over C1,NATS: Both consumers running
    loop Heartbeat every 2s
        C1->>KV: Renew leases for partitions (0,1)
        C2->>KV: Renew leases for partitions (2,3)
    end
    
    Note over C1: Process killed (no graceful shutdown)
    Note over C1: Heartbeats stop
    
    Note over C2: Consumer 2 detects stale worker
    C2->>KV: Heartbeat
    C2->>KV: Check active workers (sees stale worker)
    C2->>KV: Aggressive claim cycle
    
    Note over C2: Claims expired partitions
    loop Every 3s
        C2->>KV: Try claim partitions (0,1) [~1s each attempt]
    end
    
    Note over C2: Successfully claims partitions
    C2->>KV: Claim partitions (0,1) [~5s after heartbeat stops]
    
    loop Heartbeat every 2s
        C2->>KV: Renew leases for partitions (0,1,2,3)
    end
```

**Timeline:**
- 0s: Consumer 1 process killed
- 2s: Consumer 2 detects missing heartbeat
- 5s: First claim attempt
- 8s: Second claim attempt
- 11s: Partitions successfully claimed
- **Total time for failover: ~11 seconds**

---

## 4. Configuration Parameters

```mermaid
graph TD
    A[Configuration] --> B[LEASE_TTL_SEC = 5s]
    A --> C[HEARTBEAT_SEC = 2s]
    A --> D[Aggressive Claim = 3s]
    A --> E[Grace Period = 1s]
    
    B --> F[Lease expires after 5s of no renewal]
    C --> G[Heartbeat sent every 2s]
    D --> H[Check for expired partitions every 3s]
    E --> I[Prevent immediate reclaim by same worker for 1s]
```

## 5. Failover Timing Summary

| Scenario | Detection Time | Claim Time | Total Failover Time |
|----------|---------------|------------|-------------------|
| Normal Leave | Immediate | ~1s | ~1s |
| Force Leave | ~2s | ~9s | ~11s |
| Consumer Join | ~2s | ~1s | ~3s |

### Key Factors:
1. **Heartbeat Interval (2s)**: How often consumers update their status
2. **Lease TTL (5s)**: Time before a lease is considered expired
3. **Aggressive Claim (3s)**: How often to check for expired partitions
4. **Grace Period (1s)**: Prevents immediate reclaim by same worker