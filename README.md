# NATS POC

Proof of concept for NATS JetStream with partitioned consumers and lease-based coordination.

## Prerequisites

- Go 1.24+
- Docker and Docker Compose

## Setup

1. Start NATS server with monitoring UI:
   ```bash
   docker-compose -f docker-compose-with-web-ui.yml up -d
   ```

   This will start:
   - NATS server on port 4222
   - NATS monitoring on port 8222
   - Web UI on port 8080

   You can access the web UI at http://localhost:8080

2. Install dependencies:
   ```bash
   go mod tidy
   ```

## Configuration

The applications use environment variables for configuration. You can set them directly or use a `.env` file.

Default values are provided in the `.env` file:
- `NATS_URL`: NATS server URL
- `PARTITIONS`: Number of partitions
- `SUBJECT_PREFIX`: Prefix for NATS subjects
- `STREAM_NAME`: JetStream stream name
- And more...

## Running the Applications

### Using Makefile (Recommended)

```bash
# Build all applications
make build

# Run publisher
make run-publisher

# Run consumer
make run-consumer

# Run monitor UI (process management)
make run-monitor
```

## Step-by-Step Guide to Run the POC

### 1. Start the NATS Server
```bash
# Start NATS with Docker Compose
docker-compose -f docker-compose-with-web-ui.yml up -d

# Verify NATS is running
docker-compose -f docker-compose-with-web-ui.yml ps
```

### 2. Start the Monitor UI
```bash
# In a new terminal, start the monitor UI
make run-monitor

# Open your browser to http://localhost:8082
```

### 3. Start Consumers
```bash
# In a new terminal, start the first consumer
make run-consumer

# In another terminal, start a second consumer
make run-consumer

# You can start additional consumers in separate terminals
```

### 4. Start the Publisher
```bash
# In a new terminal, start the publisher
make run-publisher
```

### 5. Monitor the System
- Open your browser to http://localhost:8080 to view the monitor UI
- You can see consumers, their status, and message flow
- The "Consumer Status Updates" section shows when consumers acquire/release partitions
- The "Recent Stream Messages" section shows actual messages in the stream

## Testing Rebalancing (POC Rebalancing)

### Adding New Workers (Scaling Up)

1. **Start with existing consumers:**
   - Make sure you have 1-2 consumers running already
   - Check the monitor UI to see which partitions each consumer owns

2. **Add a new worker:**
   ```bash
   # In a new terminal, start an additional consumer
   make run-consumer
   ```

3. **Observe rebalancing:**
   - Watch the monitor UI "Consumer Status Updates" section
   - You should see one of the existing consumers release some partitions
   - The new consumer will acquire those released partitions
   - This demonstrates automatic load balancing when adding workers

### Removing Workers (Scaling Down)

1. **Identify running consumers:**
   - Check the monitor UI "Consumer Processes" section
   - Note the PIDs of the running consumers

2. **Remove a worker:**
   - Either stop a consumer with Ctrl+C
   - Or use the "Kill PID" button in the monitor UI for a specific consumer

3. **Observe rebalancing:**
   - Watch the monitor UI "Consumer Status Updates" section
   - You should see the removed consumer's partitions being released
   - Other consumers will acquire those partitions
   - This demonstrates automatic load redistribution when removing workers

### Expected Behavior During Rebalancing

1. **When adding a consumer:**
   - Existing consumers will voluntarily release some partitions
   - The new consumer acquires released partitions
   - No message loss occurs during this process
   - All consumers continue processing messages

2. **When removing a consumer:**
   - The removed consumer's partitions are released
   - Remaining consumers acquire those partitions
   - Messages continue to be processed by remaining consumers
   - No data is lost in the process

### Monitoring Rebalancing

In the monitor UI, you can observe:
- **Consumer Status Updates**: Shows when consumers acquire or release partitions
- **Stream Information**: Shows message counts and stream health
- **Consumers Table**: Shows which consumer owns which partitions
- **Consumer Processes**: Shows running consumer processes that can be managed

## Memory Monitoring

The consumer application includes built-in memory monitoring that logs memory statistics every 30 seconds during the cleanup cycle:

```
Memory Stats - Alloc: 5 MB, TotalAlloc: 10 MB, Sys: 12 MB, NumGC: 3
```

### Understanding Memory Stats:
- **Alloc**: Memory currently in use (should be stable)
- **TotalAlloc**: Cumulative memory allocated since startup (naturally increases)
- **Sys**: Memory requested from the OS (should be stable)
- **NumGC**: Number of garbage collections performed

### Healthy Memory Usage Indicators:
- ✅ **Alloc stable**: Memory usage is not continuously increasing
- ✅ **Sys stable**: No continuous requests for more memory from OS
- ⚠️ **TotalAlloc increasing**: Normal behavior as it's cumulative

### When to Be Concerned:
- ❌ Alloc continuously increasing without garbage collection
- ❌ Sys continuously increasing (OS constantly allocating more memory)
- ❌ No garbage collection occurring (NumGC not increasing)

The memory monitoring confirms there are no memory leaks in the current implementation, as Alloc and Sys remain stable while the application is running.

## Architecture

- Events are published to partitioned subjects (`subject.events.p.0`, `subject.events.p.1`, etc.)
- Each partition can be consumed by only one consumer at a time using a lease mechanism
- Leases are stored in a NATS KV bucket
- Consumers periodically renew their leases
- When a new consumer joins or an existing one leaves, leases are automatically rebalanced

## Cleanup

To stop all services:
```bash
# Stop NATS services
docker-compose -f docker-compose-with-web-ui.yml down

# Stop any running Go applications with Ctrl+C
```