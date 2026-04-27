# Kafka Broker Internals: Network Model, Producer, Consumer, and Metadata APIs

## Table of Contents

1. [Network Handling Model](#1-network-handling-model)
2. [Produce API (Producer Side)](#2-produce-api-producer-side)
3. [Fetch API (Consumer Side)](#3-fetch-api-consumer-side)
4. [Metadata API](#4-metadata-api)
5. [Request Lifecycle End-to-End](#5-request-lifecycle-end-to-end)

---

## 1. Network Handling Model

### 1.1 Overview

Kafka's broker network layer is built around a **reactor pattern** using Java NIO. It cleanly separates concerns into three layers: accepting new connections, reading/writing bytes, and processing requests. This allows very high throughput while keeping the number of threads bounded.

```
                          ┌──────────────────────────────────────────────────┐
                          │                 SocketServer                     │
                          │                                                  │
  Clients / Brokers  ─────┤  Acceptor Thread  ──► Processor Threads (N)     │
                          │       │                      │                   │
                          │  (accept TCP)          (NIO Selector,           │
                          │                        read/write bytes)        │
                          │                              │                   │
                          │                    RequestChannel (queue)        │
                          │                              │                   │
                          │                    KafkaRequestHandler (M)       │
                          │                    (calls KafkaApis.handle())    │
                          └──────────────────────────────────────────────────┘
```

### 1.2 Two Request Planes

Kafka supports two distinct network planes (see `SocketServer.scala:59-76`):

| Plane | Purpose | Acceptors | Processors | Handlers |
|---|---|---|---|---|
| **Data Plane** | Clients + inter-broker replication | 1 per listener | N (configurable) | M (configurable) |
| **Control Plane** | Controller ↔ broker communication | 1 | 1 | 1 |

The control plane is optional (`control.plane.listener.name` config). If not set, controller traffic shares the data plane.

### 1.3 Acceptor Thread

**File:** `core/src/main/scala/kafka/network/SocketServer.scala`

- One Acceptor per listener endpoint.
- Runs a `java.nio.channels.Selector` in a tight loop.
- When `OP_ACCEPT` fires on the `ServerSocketChannel`, it accepts the TCP connection and **round-robins** it to one of its Processor threads.
- Does **no I/O** beyond accepting; intentionally lightweight.

```
// Simplified pseudo-logic
while (running) {
  selector.select(500ms)
  for each ACCEPT-ready key:
    channel = serverSocketChannel.accept()
    processor = nextProcessor()    // round-robin
    processor.accept(channel)
}
```

### 1.4 Processor Threads

Each Processor owns its own `KafkaChannel` (wrapping NIO `SocketChannel`) and a `Selector` (Kafka's abstraction over Java NIO Selector).

**Responsibilities:**
1. **Register** newly accepted channels from the Acceptor's queue.
2. **Poll** the NIO selector for `OP_READ` and `OP_WRITE` events.
3. **Read** incoming bytes, parse the request frame (4-byte length prefix + payload).
4. **Enqueue** completed `RequestChannel.Request` objects onto the shared `RequestChannel`.
5. **Dequeue** responses from the `RequestChannel` and write them back to the socket.

**Key design decisions:**
- Each Processor is single-threaded, so there is no synchronization on I/O paths.
- Memory for in-flight request buffers comes from a shared `SimpleMemoryPool` (bounded by `queued.max.bytes`). If the pool is exhausted, the Processor mutes the channel (stops reading from it) via `ChannelMuteEvent.MUTE_ON_MEMORY_PRESSURE`.
- Authentication and TLS handshakes happen inside the Processor via `KafkaChannel.prepare()` before any application data is read.

### 1.5 Request Handler Threads (KafkaRequestHandler)

**File:** `core/src/main/scala/kafka/server/KafkaRequestHandler.scala`

- A fixed pool of `num.io.threads` handler threads.
- Each thread loops: call `requestChannel.receiveRequest(300ms)`, then dispatch to `KafkaApis.handle()`.
- After `handle()` returns, the handler puts a `SendResponse` (or `NoOpResponse`, `CloseConnectionResponse`) back onto the `RequestChannel`.
- The originating Processor drains responses from the channel and writes them out.

```scala
// KafkaRequestHandler.run() core loop (simplified)
val req = requestChannel.receiveRequest(300)
threadCurrentRequest.set(req)
apis.handle(req, requestLocal)     // → KafkaApis.handle()
```

### 1.6 RequestChannel: The Hand-off Queue

`RequestChannel` is the decoupling layer between Processors and Handlers. It is a bounded `ArrayBlockingQueue[BaseRequest]`. When a Processor finishes reading a request, it calls `requestChannel.sendRequest()`. When a Handler finishes processing, it calls `requestChannel.sendResponse()`. The Processor then picks up the response in its next poll cycle.

### 1.7 Connection Quotas and Throttling

- `ConnectionQuotas` enforces per-IP and per-listener connection limits.
- Quota violations throw `TooManyConnectionsException` at accept time.
- Bandwidth and request-rate quotas are enforced at the Handler layer (via `QuotaManagers`) by **muting** the processor channel (`StartThrottlingResponse` → `EndThrottlingResponse`) for a calculated delay.

### 1.8 Thread Counts Summary

| Config Key | Default | Thread Role |
|---|---|---|
| `num.network.threads` | 3 | Processor threads (per data-plane listener) |
| `num.io.threads` | 8 | Request handler threads |
| `queued.max.requests` | 500 | Max depth of RequestChannel queue |

---

## 2. Produce API (Producer Side)

### 2.1 Wire Protocol

**API Key:** `0` (PRODUCE)
**Handler:** `KafkaApis.handleProduceRequest()` (`KafkaApis.scala:606`)

A `ProduceRequest` carries:
- `acks` — `0` (fire-and-forget), `1` (leader ack), `-1` (all ISR acks)
- `timeout` — max ms to wait before timing out
- Per-topic, per-partition `MemoryRecords` batches

### 2.2 Request Processing Pipeline

```
ProduceRequest
     │
     ▼
1. Authorization check
   authHelper.filterByAuthorized(WRITE, TOPIC, ...)
     │
     ▼
2. Partition validation
   metadataCache.contains(topicPartition)
     │
     ▼
3. Record format validation
   ProduceRequest.validateRecords(apiVersion, memoryRecords)
     │
     ▼
4. ReplicaManager.handleProduceAppend(...)
     │
     ├──► Log.append() on local leader partition
     │         - Record format conversion if needed
     │         - CRC validation
     │         - Idempotent/transactional producer checks
     │         - Write to segment file (memory-mapped)
     │
     ├── acks == 0?  → sendNoOpResponse immediately
     ├── acks == 1?  → respond after local append
     └── acks == -1? → create DelayedProduce, wait for ISR replication
```

### 2.3 Delayed Produce and Purgatory

**File:** `core/src/main/scala/kafka/server/DelayedProduce.scala`

When `acks=-1`, the broker must wait for all ISR replicas to acknowledge the write before responding. Rather than blocking a handler thread, the request is placed in a **Purgatory**:

1. `ReplicaManager.handleProduceAppend()` creates a `DelayedProduce` operation.
2. `DelayedProduce` tracks `ProducePartitionStatus.acksPending` per partition.
3. The operation is registered in `DelayedOperationPurgatory` keyed by `TopicPartitionOperationKey`.
4. Follower FETCH requests (replication fetches) trigger `ReplicaManager.updateFollowerFetchState()`, which calls `replicaManager.tryCompleteActions()`.
5. When all required ISR replicas have fetched up to the required offset, `DelayedProduce.tryComplete()` returns `true` and the response is sent.
6. If the timeout fires first, `onExpiration()` is called which sends a `REQUEST_TIMED_OUT` error.

```
                  ISR Follower Fetch
                         │
                         ▼
              updateFollowerFetchState()
                         │
                         ▼
              tryCompleteDelayedProduce()  ──► sends ProduceResponse
```

### 2.4 acks=0 Special Case

When `acks=0` the broker sends `NoOpResponse` immediately (no bytes on wire) — unless there is an error, in which case the **socket is closed** (`requestChannel.closeConnection()`). This forces the producer client to detect the error via a broken connection and refresh its metadata.

### 2.5 Throttling

After the append, bandwidth quota (`quotas.produce`) and request-rate quota (`quotas.request`) are both checked. If either is exceeded, the Processor channel is muted for the calculated throttle duration. The `ProduceResponse` includes `throttleTimeMs` so the client knows to back off.

### 2.6 Idempotent and Transactional Producers

- **Idempotent (`enable.idempotence=true`):** Each batch carries `producerId`, `producerEpoch`, and `sequence`. The broker's `ProducerStateManager` (per-partition) deduplicates retries by checking sequence numbers.
- **Transactional:** The broker additionally verifies the `transactionalId` against the Transaction Coordinator (another broker hosting `__transaction_state` partition) via `INIT_PRODUCER_ID`, `ADD_PARTITIONS_TO_TXN`, and `END_TXN` flows.

---

## 3. Fetch API (Consumer Side)

### 3.1 Wire Protocol

**API Key:** `1` (FETCH)
**Handler:** `KafkaApis.handleFetchRequest()` (`KafkaApis.scala:757`)

A `FetchRequest` carries:
- `maxWaitMs` — max time to block waiting for data
- `minBytes` / `maxBytes` — flow control
- Per-partition: `fetchOffset`, `logStartOffset`, `maxPartitionBytes`
- `FetchMetadata` — for incremental fetch sessions (KIP-227)

**Dual use:** The same `FETCH` API is used by both **consumer clients** and **follower brokers** (replication). The broker differentiates them by `fetchRequest.isFromFollower`.

### 3.2 Fetch Session Management (KIP-227)

**File:** `core/src/main/scala/kafka/server/FetchSession.scala`

Consumers often fetch the same partitions repeatedly. Fetch sessions avoid resending the full partition list every time:

1. First fetch: `FetchMetadata.sessionId = INVALID_SESSION_ID` → broker creates a session, returns `sessionId` and `epoch=1`.
2. Subsequent fetches: client sends only *changed* partitions (delta). Broker reconstructs the full fetch set from the session cache.
3. Session cache has a bounded size; eviction is LRU-based.

```
Client                              Broker
  │                                   │
  │── FetchReq(sessionId=0) ─────────►│ create session S1, epoch=1
  │◄──────── FetchResp(S1, epoch=1) ──│
  │                                   │
  │── FetchReq(S1, epoch=2, Δ={}) ───►│ apply delta, fetch full set
  │◄──────── FetchResp(S1, epoch=2) ──│
```

### 3.3 Request Processing Pipeline

```
FetchRequest
     │
     ▼
1. Fetch session context construction
   fetchManager.newContext(version, metadata, isFromFollower, ...)
     │
     ▼
2. Authorization
   - Followers: CLUSTER_ACTION on ClusterResource
   - Consumers: READ on each TOPIC
     │
     ▼
3. Partition classification
   - erroneous: unknown topic/partition ID
   - interesting: valid, authorized partitions
     │
     ▼
4. replicaManager.fetchMessages(...)
     │
     ├──► Log.read() on each partition
     │         - Read from segment file at fetchOffset
     │         - Apply FetchIsolation (READ_COMMITTED for txn consumers)
     │         - Cap at maxPartitionBytes / maxBytes
     │
     ├── Enough data? → respond immediately
     └── Not enough? → create DelayedFetch in purgatory
```

### 3.4 Delayed Fetch and Purgatory

`DelayedFetch` is the consumer equivalent of `DelayedProduce`:

1. If the total bytes fetched < `fetchRequest.minBytes`, a `DelayedFetch` is created with timeout `maxWaitMs`.
2. New produce appends call `replicaManager.tryCompleteActions()` → wakes up waiting fetches on those partitions.
3. When data becomes available or the timeout fires, the fetch response is assembled and sent.

This is critical for **long-polling consumer efficiency**: a consumer with nothing to read sleeps in the purgatory rather than spinning in a polling loop.

### 3.5 Read Isolation Levels

| Consumer Config | FetchIsolation | Behavior |
|---|---|---|
| `read_uncommitted` (default) | `FetchIsolation.HIGH_WATERMARK` | Reads up to HW; includes uncommitted txn records |
| `read_committed` | `FetchIsolation.TXN_COMMITTED` | Reads up to Last Stable Offset (LSO); hides in-flight transactions |

### 3.6 Follower Replication (Same FETCH API)

Follower replicas use the same `FETCH` API but with `replicaId` set to their broker ID. The broker identifies these as follower fetches and:
1. Updates the follower's fetch offset in `Partition.updateFollowerFetchState()`.
2. Advances the HWM if the lagging replica has caught up.
3. Triggers completion of `DelayedProduce` operations waiting for ISR acks.

### 3.7 Down-Conversion

If a consumer sends an older `FetchRequest` version than the on-disk record format magic, the broker performs **lazy down-conversion** (KIP-283): it converts records in chunks just before sending, rather than converting all data upfront. This bounds the memory spike for large partitions.

---

## 4. Metadata API

### 4.1 Wire Protocol

**API Key:** `3` (METADATA)
**Handler:** `KafkaApis.handleTopicMetadataRequest()` (`KafkaApis.scala:1316`)

The `MetadataRequest` can ask for:
- All topics (`isAllTopics = true`)
- Specific topics by name (versions < 12)
- Specific topics by UUID (versions ≥ 12, KIP-516)

The `MetadataResponse` returns:
- **Broker list:** all alive brokers with `nodeId`, `host`, `port`, `rack`
- **Controller ID**
- **Per-topic metadata:** topic UUID, `isInternal`, partition leader, ISR, replicas, error code

### 4.2 MetadataCache

**File:** `core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala` (KRaft mode)
**File:** `core/src/main/scala/kafka/server/metadata/ZkMetadataCache.scala` (ZK mode)

Every broker maintains an in-memory `MetadataCache`. This is the single source of truth the broker uses to answer `MetadataRequest`s without going to ZooKeeper or the controller on every request.

**How it stays current:**

| Mode | Update Mechanism |
|---|---|
| **ZooKeeper mode** | Controller sends `UpdateMetadataRequest` (API key `6`) to all brokers whenever the cluster state changes (leader election, ISR change, etc.). `handleUpdateMetadataRequest()` calls `replicaManager.maybeUpdateMetadataCache()`. |
| **KRaft mode** | Each broker subscribes to the metadata log (the Raft-replicated `__cluster_metadata` topic) via `BrokerMetadataPublisher`. Cache is updated by replaying metadata records. No controller push needed. |

### 4.3 Request Processing Pipeline

```
MetadataRequest
     │
     ▼
1. Topic ID vs. name resolution (v12+)
   metadataCache.getTopicName(topicId)
     │
     ▼
2. Authorization
   authHelper.filterByAuthorized(DESCRIBE, TOPIC, ...)
     │
     ▼
3. Auto-topic creation (if enabled and topic missing)
   autoTopicCreationManager.createTopics(nonExistingTopics)
   → creates topic asynchronously, returns LEADER_NOT_AVAILABLE
     │
     ▼
4. Gather partition metadata
   metadataCache.getPartitionInfo(topic, partition)
   → leader epoch, leader ID, ISR list, replica list
     │
     ▼
5. Listener filtering
   Only return the broker endpoint for the *same listener* as the client
   (e.g., INTERNAL listener clients don't see EXTERNAL listener addresses)
     │
     ▼
6. Assemble MetadataResponse
   - brokers (alive nodes for listener)
   - controllerId
   - per-topic partition metadata
   - throttleTimeMs
```

### 4.4 Listener-Aware Endpoint Filtering

A single broker can listen on multiple listeners (e.g., `INTERNAL:9092,EXTERNAL:9093`). When the broker builds the broker list in `MetadataResponse`, it filters the endpoints to only return the address matching the **listener the client connected through**. This prevents an internal-listener client from receiving external listener addresses (which may not be reachable from inside the datacenter, or vice versa).

### 4.5 Auto Topic Creation

When a `MetadataRequest` arrives for a non-existent topic with `allowAutoTopicCreation=true` and `auto.create.topics.enable=true`:

1. `AutoTopicCreationManager` submits a `CreateTopics` request to the controller.
2. The broker **immediately returns** `LEADER_NOT_AVAILABLE` for the new topic.
3. The producer/consumer client retries the metadata request after a backoff.
4. On retry, the topic exists and the broker returns valid partition metadata.

### 4.6 Controller ID in MetadataResponse

- **ZK mode:** Returns the actual ZK controller's broker ID.
- **KRaft mode:** There is no single "broker" controller visible to clients. The broker returns `getRandomAliveBrokerId` as the controller ID, since the KRaft controller is a separate process and clients don't need to know which specific node it is.

---

## 5. Request Lifecycle End-to-End

Below is the complete path of a single `ProduceRequest` through the broker:

```
Client TCP connection
        │
        ▼
[Acceptor Thread]
  ServerSocketChannel.accept()
  → assign to Processor-N (round-robin)
        │
        ▼
[Processor Thread N]  (NIO Selector loop)
  OP_READ fires
  → KafkaChannel.read() accumulates bytes
  → Complete frame received
  → Parse RequestHeader + body
  → Build RequestChannel.Request
  → requestChannel.sendRequest(req)
        │
        ▼
[RequestChannel Queue]  (ArrayBlockingQueue, max=queued.max.requests)
        │
        ▼
[KafkaRequestHandler Thread M]
  requestChannel.receiveRequest()
  → KafkaApis.handle(request)
      → case ApiKeys.PRODUCE → handleProduceRequest()
          → authorization check
          → replicaManager.handleProduceAppend()
              → Log.append() (write to segment)
              → acks=-1: create DelayedProduce, enqueue in purgatory
              → acks=1:  call sendResponseCallback immediately
              → acks=0:  sendNoOpResponse
        │
        ▼
[Purgatory Timer / ISR replication triggers DelayedProduce.tryComplete()]
        │
        ▼
[KafkaRequestHandler Thread M (or callback thread)]
  sendResponseCallback()
  → requestChannel.sendResponse(request, ProduceResponse, None)
        │
        ▼
[Processor Thread N]  (polls response queue)
  SendResponse dequeued
  → NetworkSend queued on KafkaChannel
  → OP_WRITE fires on NIO selector
  → bytes written to TCP socket
        │
        ▼
Client receives ProduceResponse
```

---

## Key Configuration Reference

| Config | Default | Affects |
|---|---|---|
| `num.network.threads` | 3 | Processor threads per listener |
| `num.io.threads` | 8 | KafkaRequestHandler thread pool size |
| `queued.max.requests` | 500 | Max buffered requests before backpressure |
| `queued.max.bytes` | -1 (unlimited) | Max total in-flight request bytes in memory pool |
| `socket.send.buffer.bytes` | 102400 | TCP SO_SNDBUF |
| `socket.receive.buffer.bytes` | 102400 | TCP SO_RCVBUF |
| `socket.request.max.bytes` | 104857600 | Max single request size (100MB) |
| `replica.fetch.min.bytes` | 1 | Min bytes before follower fetch responds |
| `fetch.purgatory.purge.interval.requests` | 1000 | How often to purge completed fetch ops |
| `producer.purgatory.purge.interval.requests` | 1000 | How often to purge completed produce ops |

---

## Source Code Navigation

| Component | File |
|---|---|
| Network layer | `core/src/main/scala/kafka/network/SocketServer.scala` |
| Request dispatch | `core/src/main/scala/kafka/server/KafkaApis.scala` |
| Request handler threads | `core/src/main/scala/kafka/server/KafkaRequestHandler.scala` |
| Produce purgatory | `core/src/main/scala/kafka/server/DelayedProduce.scala` |
| Fetch purgatory | `core/src/main/scala/kafka/server/DelayedFetch.scala` |
| Replication | `core/src/main/scala/kafka/server/ReplicaManager.scala` |
| Metadata cache (KRaft) | `core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala` |
| Metadata cache (ZK) | `core/src/main/scala/kafka/server/metadata/ZkMetadataCache.scala` |
| Fetch session mgmt | `core/src/main/scala/kafka/server/FetchSession.scala` |
| NIO selector wrapper | `clients/src/main/java/org/apache/kafka/common/network/Selector.java` |
