# Kafka Topic Offsets: Detailed Technical Guide

## Table of Contents
1. [Overview](#overview)
2. [Fundamental Concepts](#fundamental-concepts)
3. [Types of Offsets](#types-of-offsets)
4. [Offset Architecture](#offset-architecture)
5. [Consumer Offset Management](#consumer-offset-management)
6. [Producer Offset Handling](#producer-offset-handling)
7. [Storage and Persistence](#storage-and-persistence)
8. [Offset Reset Strategies](#offset-reset-strategies)
9. [Advanced Concepts](#advanced-concepts)
10. [Operational Considerations](#operational-considerations)

## Overview

**Kafka offsets** are sequential integer identifiers that uniquely identify each message (record) within a topic partition. They serve as the fundamental mechanism for message ordering, consumer progress tracking, and data consistency in Apache Kafka.

### Key Characteristics
- **Sequential**: Offsets increment monotonically within each partition
- **Partition-scoped**: Each partition maintains its own independent offset sequence
- **Immutable**: Once assigned, an offset never changes
- **Dense**: No gaps in the sequence (except during log compaction)
- **64-bit**: Stored as long integers (range: 0 to 2^63 - 1)

## Fundamental Concepts

### 1. Partition-Level Sequencing

```
Topic: "user-events" (3 partitions)

Partition 0: [msg0:0] [msg1:1] [msg2:2] [msg3:3] [msg4:4] ...
Partition 1: [msg0:0] [msg1:1] [msg2:2] [msg3:3] [msg4:4] ...
Partition 2: [msg0:0] [msg1:1] [msg2:2] [msg3:3] [msg4:4] ...
```

**Key Points**:
- Offsets start at 0 for each partition
- Each partition maintains independent offset sequences
- No global ordering across partitions within a topic

### 2. TopicPartition Identification

Based on the Kafka source code (`TopicPartition.java`):

```java
public final class TopicPartition implements Serializable {
    private final int partition;
    private final String topic;

    public TopicPartition(String topic, int partition) {
        this.partition = partition;
        this.topic = topic;
    }
}
```

**Structure**: Messages are uniquely identified by `(topic, partition, offset)` tuples.

### 3. Message Positioning

```
Partition View:
[offset:0] [offset:1] [offset:2] [offset:3] [offset:4] [offset:5] ...
   ↓          ↓          ↓          ↓          ↓          ↓
[record1]  [record2]  [record3]  [record4]  [record5]  [record6] ...

Consumer Position: offset 3 (next fetch will get offset 3, 4, 5...)
Consumer Committed: offset 2 (processed up to offset 1)
```

## Types of Offsets

### 1. Logical End Offset (Log End Offset - LEO)
- **Definition**: The offset of the next message that would be appended
- **Scope**: Per partition
- **Visibility**: Last offset + 1

```java
// Example: If last message has offset 100, LEO = 101
long logEndOffset = partition.logEndOffset(); // Returns 101
```

### 2. High Water Mark (HWM)
- **Definition**: Highest offset of fully replicated messages
- **Purpose**: Ensures consistency across replicas
- **Consumer Visibility**: Only messages below HWM are visible to consumers

```
Replica State Example:
Leader:     [0][1][2][3][4][5]     LEO=6
Follower1:  [0][1][2][3][4]        LEO=5
Follower2:  [0][1][2][3]           LEO=4

High Water Mark = min(4, 5, 6) = 4
Consumer can only read up to offset 3
```

### 3. Low Water Mark (LWM)
- **Definition**: Oldest available offset in the partition
- **Purpose**: Indicates log retention boundary
- **Impact**: Offsets below LWM have been deleted

### 4. Consumer Offsets
- **Current Position**: Next offset to fetch
- **Committed Offset**: Last successfully processed offset
- **Lag**: Difference between HWM and consumer position

### 5. Last Stable Offset (LSO)
- **Definition**: For transactional producers, the offset below which all transactions are complete
- **Purpose**: Isolation level enforcement for read_committed consumers

## Offset Architecture

### 1. Storage Structure

Based on `OffsetIndex.java` analysis:

```java
/**
 * An index that maps offsets to physical file locations for a particular log segment.
 * The index is sparse and supports binary search lookups.
 *
 * File format: series of 8-byte entries:
 * - 4 bytes: relative offset (from base offset)
 * - 4 bytes: physical file position
 */
public class OffsetIndex extends AbstractIndex {
    private static final int ENTRY_SIZE = 8;
    private long lastOffset;

    // Binary search for offset lookup
    public OffsetPosition lookup(long targetOffset) {
        // Implementation details...
    }
}
```

### 2. Log Segment Organization

```
Partition Directory Structure:
/var/kafka/logs/topic-0/
├── 00000000000000000000.log     (base offset: 0)
├── 00000000000000000000.index   (offset index)
├── 00000000000000000000.timeindex (time index)
├── 00000000000000001000.log     (base offset: 1000)
├── 00000000000000001000.index
├── 00000000000000001000.timeindex
└── ...

Each segment contains:
- .log: actual message data
- .index: offset → file position mapping
- .timeindex: timestamp → offset mapping
```

### 3. Offset Assignment Process

```java
// Simplified producer offset assignment flow:
1. Producer sends batch of records to broker
2. Broker assigns consecutive offsets starting from LEO
3. Broker appends to log segment
4. Broker updates LEO
5. Broker responds with assigned offset range

// Example batch assignment:
Batch: [record1, record2, record3]
Current LEO: 1000
Assigned offsets: [1000, 1001, 1002]
New LEO: 1003
```

## Consumer Offset Management

### 1. OffsetAndMetadata Structure

From `OffsetAndMetadata.java`:

```java
public class OffsetAndMetadata implements Serializable {
    private final long offset;           // The committed offset
    private final String metadata;       // Optional user metadata
    private final Integer leaderEpoch;   // Leader epoch for fence detection

    public OffsetAndMetadata(long offset, Optional<Integer> leaderEpoch, String metadata) {
        if (offset < 0)
            throw new IllegalArgumentException("Invalid negative offset");
        this.offset = offset;
        this.leaderEpoch = leaderEpoch.orElse(null);
        this.metadata = metadata == null ? "" : metadata;
    }
}
```

### 2. Consumer Offset Tracking

```java
// Consumer offset management example:
Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

// After processing a batch:
for (ConsumerRecord<K, V> record : records) {
    processRecord(record);  // Application logic

    currentOffsets.put(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset() + 1)  // Next offset to read
    );
}

// Commit offsets:
consumer.commitSync(currentOffsets);
```

### 3. Offset Commit Strategies

#### Automatic Offset Commits
```java
Properties props = new Properties();
props.put("enable.auto.commit", "true");
props.put("auto.commit.interval.ms", "5000");

// Commits happen automatically every 5 seconds
```

#### Manual Offset Commits
```java
// Synchronous commit (blocks until acknowledged)
consumer.commitSync();

// Asynchronous commit (non-blocking)
consumer.commitAsync((offsets, exception) -> {
    if (exception != null) {
        log.error("Commit failed for offsets {}", offsets, exception);
    }
});
```

### 4. Consumer Group Coordination

```
Consumer Group: "analytics-group"

Member 1: Assigned partitions [0, 1]
- topic-0: position=150, committed=145
- topic-1: position=230, committed=225

Member 2: Assigned partitions [2, 3]
- topic-2: position=180, committed=175
- topic-3: position=200, committed=195

Offsets stored in __consumer_offsets topic:
Key: (group_id, topic, partition)
Value: OffsetAndMetadata
```

## Producer Offset Handling

### 1. Offset Assignment During Production

```java
// Producer offset handling:
public class ProducerRecord<K, V> {
    private final String topic;
    private final Integer partition;
    private final K key;
    private final V value;
    // No offset field - assigned by broker
}

// Response contains assigned offsets:
public class RecordMetadata {
    private final long offset;        // Assigned offset
    private final long timestamp;     // Message timestamp
    private final int partition;      // Target partition
}
```

### 2. Idempotent Producer Offsets

```java
// Idempotent producer configuration:
Properties props = new Properties();
props.put("enable.idempotence", "true");

// Ensures exactly-once semantics:
// - Producer ID (PID) assigned by broker
// - Sequence numbers prevent duplicates
// - Offsets remain consistent across retries
```

## Storage and Persistence

### 1. Consumer Offset Storage

**__consumer_offsets Topic**:
- **Purpose**: Stores consumer group offset commits
- **Partitions**: 50 by default (`offsets.topic.num.partitions`)
- **Replication**: 3 by default (`offsets.topic.replication.factor`)
- **Retention**: 7 days by default (`offsets.retention.minutes`)

```
Topic: __consumer_offsets
Key format: (group_id, topic, partition) → hash → partition
Value format: OffsetAndMetadata

Example entries:
Key: ("analytics-group", "user-events", 0)
Value: {"offset": 1000, "metadata": "", "leaderEpoch": 5}
```

### 2. Offset Index Files

```java
// From OffsetIndex.java - file format:
/**
 * Index file structure:
 * - Pre-allocated fixed-size file
 * - 8-byte entries: [4-byte relative offset][4-byte file position]
 * - Sparse index (not every message indexed)
 * - Memory-mapped for fast binary search lookups
 */

// Example index entry:
Base Offset: 1000
Entry: [50, 12340] → Offset 1050 is at file position 12340 in .log file
```

### 3. Offset Recovery and Rebuild

```bash
# Kafka can rebuild offset indices if corrupted:
kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe

# Recovery process:
1. Broker scans .log files
2. Rebuilds .index and .timeindex files
3. Restores offset mappings
4. Updates high water mark
```

## Offset Reset Strategies

### 1. Consumer Reset Policies

```java
// Configuration options:
Properties props = new Properties();

// Available strategies:
props.put("auto.offset.reset", "earliest");  // Start from beginning
props.put("auto.offset.reset", "latest");    // Start from end
props.put("auto.offset.reset", "none");      // Throw exception
```

### 2. Manual Offset Management

```java
// Seek to specific offset:
TopicPartition partition = new TopicPartition("user-events", 0);
consumer.assign(Arrays.asList(partition));
consumer.seek(partition, 1000);  // Start reading from offset 1000

// Seek to beginning/end:
consumer.seekToBeginning(Arrays.asList(partition));
consumer.seekToEnd(Arrays.asList(partition));
```

### 3. Offset Reset Tools

```bash
# Reset consumer group offsets:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group analytics-group --reset-offsets --to-earliest \
    --topic user-events --execute

# Reset to specific offset:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group analytics-group --reset-offsets --to-offset 1000 \
    --topic user-events:0 --execute

# Reset by timestamp:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group analytics-group --reset-offsets \
    --to-datetime 2024-01-01T00:00:00.000 \
    --topic user-events --execute
```

## Advanced Concepts

### 1. Leader Epoch Fencing

```java
// From OffsetAndMetadata.java:
private final Integer leaderEpoch;  // Prevents reading after log truncation

/**
 * Leader epochs prevent inconsistencies:
 * 1. Consumer commits offset 1000 with epoch 5
 * 2. Leader fails, new leader truncates log to offset 800
 * 3. Consumer tries to fetch from offset 1000
 * 4. Broker detects epoch mismatch and forces reset
 */
```

### 2. Transaction Offsets

```java
// Transactional consumer configuration:
Properties props = new Properties();
props.put("isolation.level", "read_committed");

/**
 * Transactional behavior:
 * - LSO (Last Stable Offset) tracks transaction completion
 * - Consumers only see committed transaction data
 * - Aborted transaction offsets are skipped
 */
```

### 3. Log Compaction Impact

```java
/**
 * Compacted topics can have offset gaps:
 *
 * Before compaction: [0][1][2][3][4][5][6][7][8][9]
 * After compaction:  [0]   [2]   [4]      [7][8][9]
 *
 * - Offsets 1, 3, 5, 6 were compacted away
 * - Consumer must handle missing offsets gracefully
 * - LEO remains unchanged (10)
 */
```

### 4. Multi-Tier Storage

```java
/**
 * Tiered storage affects offset accessibility:
 *
 * Hot Tier (Local SSD):    offsets 8000-10000
 * Warm Tier (Remote):      offsets 5000-7999
 * Cold Tier (Archived):    offsets 0-4999
 *
 * - Different retrieval latencies
 * - Cost implications for offset seeks
 */
```

## Operational Considerations

### 1. Offset Monitoring

```java
// Key metrics to monitor:
- Consumer lag: HWM - committed_offset
- Consumer rate: offsets_consumed_per_second
- Commit frequency: commits_per_second
- Reset frequency: offset_resets_per_hour

// JMX metrics:
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*
- records-consumed-rate
- records-lag-max
- records-lag-avg
```

### 2. Offset Performance Optimization

```java
// Optimize offset commits:
Properties props = new Properties();

// Reduce commit frequency for higher throughput:
props.put("enable.auto.commit", "false");  // Manual control
props.put("auto.commit.interval.ms", "30000");  // If auto-commit used

// Batch size optimization:
props.put("max.poll.records", "500");  // Balance latency vs throughput

// Partition assignment strategy:
props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
```

### 3. Offset Troubleshooting

```bash
# Common offset-related issues and diagnostics:

# 1. Consumer lag investigation:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group analytics-group

# 2. Offset out of range errors:
# Check current offset range:
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic user-events --time -1

# 3. Duplicate message investigation:
# Verify commit vs processing patterns
# Check for processing time > session.timeout.ms

# 4. Lost message investigation:
# Verify commit timing
# Check for auto.offset.reset policy triggering
```

### 4. Disaster Recovery

```bash
# Offset backup and restore procedures:

# 1. Export consumer group offsets:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group analytics-group --describe --verbose

# 2. Reset to specific timestamp during outage:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group analytics-group --reset-offsets \
    --to-datetime 2024-01-01T12:00:00.000 \
    --all-topics --execute

# 3. Verify offset recovery:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group analytics-group --describe
```

## Best Practices

### 1. Offset Commit Strategy
- **High Throughput**: Manual commits after processing batches
- **Low Latency**: Auto-commit with short intervals
- **Exactly Once**: Use transactions with idempotent producers

### 2. Consumer Positioning
- **New Applications**: Start with `earliest` for complete data
- **Real-time Systems**: Use `latest` to avoid catch-up lag
- **Critical Systems**: Use `none` and handle explicitly

### 3. Error Handling
- Always handle `OffsetOutOfRangeException`
- Implement offset reset policies for edge cases
- Monitor and alert on consumer lag trends

### 4. Performance Tuning
- Align consumer batch sizes with processing capacity
- Use appropriate commit frequencies for your use case
- Consider async commits for non-critical applications

## Conclusion

Kafka offsets are the cornerstone of message ordering, delivery guarantees, and consumer progress tracking. Understanding their behavior, storage mechanisms, and management strategies is crucial for building reliable, scalable Kafka applications.

The offset system enables:
- **Scalability**: Independent partition processing
- **Reliability**: Exactly-once and at-least-once semantics
- **Flexibility**: Multiple consumer patterns and reset strategies
- **Observability**: Detailed progress tracking and lag monitoring

Proper offset management ensures data consistency, prevents message loss, and enables efficient stream processing in distributed systems.

---

**Document Version**: 1.0
**Date**: April 3, 2026
**Kafka Version**: 3.9.x
**Author**: Claude Code Analysis