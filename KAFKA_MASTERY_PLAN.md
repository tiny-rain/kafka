# Kafka mastery: architecture, source code, and operational reasoning

## Goal and approach

Move from knowing how to use topics and consumers to being able to explain Kafka
end to end, predict its behavior during failures, diagnose production symptoms,
and investigate or contribute a well-tested source change.

Use an architecture-first sequence:

1. Explain the concept and its tradeoffs in plain language.
2. Draw the relevant components, state, and request flow.
3. Trace one concrete scenario through this checkout.
4. Predict an experiment's outcome before running it.
5. Compare the prediction with records, logs, metrics, and tests.
6. Explain any discrepancy before advancing.

This is a learning implementation plan, not a request to modify Kafka itself.
Saving this document does not start any labs, builds, or Kafka source changes.
Progress is competency-based, not calendar-based; completing reading alone is
not evidence of expertise.

## Confirmed scope

- Starting point: practical topic/consumer experience, little internal knowledge.
- Priorities: architecture and internals; operations, troubleshooting, and
  performance; source understanding and eventual contributions.
- Method: architecture first, then source walkthroughs and local experiments.
- Target: modern KRaft Kafka, grounded in this repository.
- Application semantics are included where required to reason about correctness.
- Streams, Connect, share groups, tiered storage internals, cross-cluster
  replication, and ZooKeeper migration are extension topics, not prerequisites.
- Basic authentication, authorization, and listener isolation belong in the
  production readiness milestone; a deep security specialization is out of scope.

## Findings from this checkout

- `gradle.properties` identifies `4.1.2-SNAPSHOT`; `git describe` returned
  `4.1.1-40-g37c066a366`. Do not assume examples for another release apply.
- The codebase mixes Java and Scala. `README.md` recommends JDK 17 for development
  and documents module-scoped Gradle tests.
- `KafkaRaftServer` constructs broker and/or controller components according to
  `process.roles`. The sample `config/server.properties` uses combined roles.
- Topic data replication and KRaft metadata replication are different mechanisms.
  Do not explain topic partition durability as simply "Raft majority writes."
- Both `ClassicKafkaConsumer` and `AsyncKafkaConsumer` exist. Selection is through
  `ConsumerDelegateCreator` and `group.protocol`; this checkout's operations
  documentation says the newer consumer protocol is not the client default.
- KRaft documentation covers dynamic controller membership and feature levels.
  Older comments and examples can lag implementation: the `KafkaRaftClient`
  class comment still says dynamic quorum changes are unsupported.
- Core design documentation contains historical hardware examples. Use those to
  learn mechanisms, not as current performance measurements.
- Existing untracked files are outside the learning plan and must remain untouched.

## Repository map

| Area | Main locations | Question it answers |
|---|---|---|
| Concepts and operations | `docs/design/`, `docs/implementation/`, `docs/operations/` | Why does Kafka behave this way? |
| Client APIs and internals | `clients/src/main/java/org/apache/kafka/clients/` | How do send, fetch, poll, retries, and commits work? |
| Protocol schemas | `clients/src/main/resources/common/message/` | What crosses the network? |
| Broker lifecycle and request handling | `core/src/main/scala/kafka/server/`, `core/src/main/scala/kafka/network/` | How does a request reach broker logic? |
| Partition state and replication | `core/src/main/scala/kafka/cluster/Partition.scala`, `core/src/main/scala/kafka/server/ReplicaManager.scala` | Who can write, acknowledge, and become leader? |
| Storage | `storage/src/main/java/org/apache/kafka/storage/internals/log/` | How are records indexed, retained, and recovered? |
| Metadata control plane | `metadata/src/main/java/org/apache/kafka/controller/`, `metadata/src/main/java/org/apache/kafka/image/` | How do cluster changes become durable and reach brokers? |
| Metadata consensus | `raft/src/main/java/org/apache/kafka/raft/` | How is the metadata leader elected and its log replicated? |
| Consumer coordination | `group-coordinator/`, `coordinator-common/` | How do membership, assignment, and offsets survive failures? |
| Transactions | `core/src/main/scala/kafka/coordinator/transaction/`, `transaction-coordinator/` | How are producer epochs and transaction outcomes coordinated? |
| Inspection and experiments | `bin/`, `tools/`, module `src/test/` trees | How can an explanation be checked? |

Paths below are relative to the repository root. Class names without full paths
refer to the areas in this map.

## Todos and milestone acceptance criteria

### 0. Establish a reproducible study environment

Read `README.md`, `CONTRIBUTING.md`, `docs/getting-started/quickstart.md`,
and `config/server.properties`.

- Record checkout revision, JDK, relevant configs, and enabled feature levels.
- Check prerequisite knowledge: threads, callbacks/futures, sockets, filesystem
  page cache, replication, and state machines. Fill gaps as they arise rather
  than starting a separate prerequisite course.
- Start with one combined broker/controller for basic request traces.
- Expand to three local combined nodes for replication and quorum exercises.
  Explain that combined roles deliberately couple failures; isolated controller
  and broker processes are a later exercise, not a production recommendation.
- Use separate, explicitly named lab configs, ports, and data directories under
  a dedicated session artifact subdirectory. Never reuse production storage or
  run failure experiments against a real cluster.
- Build from this checkout using its documented instructions. Choose a bounded,
  module-scoped test to establish that source changes can be exercised.
- Keep diagrams and experiment notes in the session workspace unless repository
  documentation changes are explicitly requested.

Acceptance: produce and consume locally, identify the actual running version and
process roles, and explain which observations a single-node cluster cannot prove.

### 1. Build the whole-system mental model

Read `docs/getting-started/introduction.md` and selected sections of
`docs/design/design.md`.

Draw two connected diagrams:

- Data plane: producer -> partition leader -> follower replicas; consumer fetches
  from a broker. Mark ordering boundaries and independent consumer positions.
- Control plane: controller quorum -> committed metadata -> broker metadata
  state. Show broker registration, topic creation, and partition leadership.

Add the group coordinator and transaction coordinator without treating either
as the cluster controller. Explain bootstrap discovery, partition leaders,
replication factor, ISR, offsets, and retention independently.

Source entry: `KafkaRaftServer`, `BrokerServer`, and `ControllerServer`.
Read construction and lifecycle boundaries, not their entire implementations.

Experiment: create a multi-partition topic and compare client discovery,
partition descriptions, and process roles.

Acceptance: explain where one record goes, who owns each kind of state, why
consuming does not delete it, and why controllers are not a routing hop for
every produce/fetch request.

### 2. Trace one produce request end to end

Read producer and efficiency sections in `docs/design/design.md`, plus
`docs/implementation/network-layer.md`.

Trace:
`KafkaProducer.send` -> partition selection/metadata -> `RecordAccumulator` ->
`Sender` -> network request -> `SocketServer` / `KafkaRequestHandler` ->
`KafkaApis.handleProduceRequest` -> `ReplicaManager` -> `Partition` -> `UnifiedLog`.

Inspect `ProduceRequest.json`. Mark application versus background threads,
batch boundaries, delayed completion, callbacks, and error paths.

Experiment: vary `linger.ms`, `batch.size`, compression, and key distribution one
at a time. Record throughput, batch size, latency distribution, and errors.
Separate application end-to-end latency from the producer's request metrics.

Acceptance: explain when `send()` returns versus when its future completes,
what an acknowledgment proves, and how batching trades latency for throughput.

### 3. Understand the partition log and storage lifecycle

Read `docs/implementation/log.md`, `docs/implementation/message-format.md`,
and persistence sections of `docs/design/design.md`.

Source: `UnifiedLog`, `LocalLog`, `LogSegment`, and `LogCleaner`.
Study record batches, segment rolling, sparse indexes, page cache, recovery,
retention, compaction, and tombstones.

Experiment: use a disposable topic with small segments and explicit retention
settings; inspect files with Kafka's log-dump tooling. Use a separate compacted
topic to observe repeated keys and eventual cleanup. Account for cleanup
eligibility and asynchronous scheduling rather than expecting immediate deletion.

Acceptance: distinguish logical offsets from file positions, deletion retention
from compaction, and filesystem append from durable media flush. Explain why
consumption position does not control normal retention.

### 4. Reason about data replication and failure semantics

Read replication and delivery-guarantee sections in `docs/design/design.md`,
then the relevant configuration definitions for this checkout.

Source: `ReplicaManager`, `Partition`, and `UnifiedLog` high-watermark updates.
Follow follower fetches, ISR changes, leader epochs, truncation, and pending
produce responses. Identify log end offset and high watermark without confusing
either with a consumer's committed offset.

Experiment: use replication factor 3 and `min.insync.replicas=2`; compare
`acks=1` and `acks=all` as replicas stop and return. Observe ISR, leadership,
acknowledged records, exceptions, retries, and client metadata refresh.
Disable automatic topic creation and record effective settings to avoid
accidentally testing a different topic configuration.

Acceptance: predict availability and possible loss under a stated failure model.
Explain that `acks=all` is tied to ISR, not a fixed count of two and not an fsync
guarantee. Distinguish a timeout's ambiguous outcome from proof of failed append.
Keep feature-gated eligible-leader-replica behavior explicit rather than assuming
all leader-selection rules are identical across configurations.

### 5. Trace fetching, consumer groups, and offset commits

Read `docs/operations/consumer-rebalance-protocol.md`.

Source: `KafkaConsumer` -> `ConsumerDelegateCreator` -> the selected consumer
implementation; `FetchCollector`; `ConsumerNetworkThread` for the async path;
`KafkaApis.handleFetchRequest`; `GroupCoordinatorService`; `GroupMetadataManager`.
Inspect `FetchRequest.json` and `ConsumerGroupHeartbeatRequest.json`.

First separate data fetching from group coordination. Then compare
`group.protocol=classic` and `group.protocol=consumer` explicitly, following
their own threading and assignment behavior rather than mixing both traces.

Experiment: scale group membership, stop a consumer, delay processing beyond
the relevant poll limit, and restart around offset commits. Observe assignment,
rebalancing, lag, committed offsets, and duplicate processing.

Acceptance: distinguish fetched, returned, processed, and committed positions;
explain why lag is not a complete health signal; predict duplicates or skipped
application work for different processing/commit orders. Explain which heartbeat
and session settings belong to the client or server for each protocol.

### 6. Understand KRaft and the metadata lifecycle

Read `docs/operations/kraft.md`. Use `raft/README.md` only as a guide to its
standalone Raft test server, not as the broker-cluster quickstart.

Source: `KafkaRaftClient` and its quorum state transitions; `QuorumController`;
`ReplicationControlManager`; `MetadataLoader`; `MetadataImage`.

Trace topic creation: admin request -> active controller -> metadata records ->
quorum commitment -> metadata application -> broker-visible partition state.
Study elections, epochs, snapshots, broker fencing, and metadata replay.

Experiment: inspect quorum status, stop the active controller, and then remove
quorum majority in the disposable lab. Classify metadata operations and
already-established data-plane operations separately; observe rather than
assuming every operation either stops immediately or continues indefinitely.
Record broker-fencing behavior and restoration.

Acceptance: distinguish controller election from partition leader election;
explain why metadata consensus uses a majority while normal topic replication
uses ISR-based rules. Explain static versus dynamic controller membership and
the feature/configuration prerequisites for this checkout.

### 7. Establish precise delivery and transaction guarantees

Read `docs/operations/transaction-protocol.md` and relevant producer API docs.

Source: producer `Sender` and transaction management, `TransactionCoordinator`,
and `UnifiedLog` transaction visibility / last stable offset.

Study idempotent producer IDs, epochs and sequences, producer fencing,
transaction markers, `__transaction_state`, `__consumer_offsets`,
`read_committed`, and atomic Kafka output/offset commits.

Experiment: compare retries with idempotence enabled and disabled; commit and
abort transactions; leave a transaction open; restart a transactional producer.
Use controlled failure injection or focused tests when a network ambiguity
cannot be reproduced reliably through process stops.

Acceptance: distinguish producer retry deduplication from business-event
deduplication. Explain why Kafka transactions do not automatically make an
external database or HTTP side effect exactly once. Distinguish log end offset,
high watermark, last stable offset, and consumer committed offset.

### 8. Practice evidence-driven operations and performance diagnosis

Read `docs/operations/monitoring.md`, `basic-kafka-operations.md`,
`hardware-and-os.md`, and the relevant configuration/security docs.

Build a symptom -> hypotheses -> discriminating evidence -> safe action table for:

- Producer latency and timeouts.
- Consumer lag, stalled processing, and rebalance churn.
- Under-replicated or unavailable partitions.
- Hot partitions, disk pressure, and slow follower catch-up.
- Controller instability and metadata-operation failures.

Source/tool anchors: `ConsumerGroupCommand`, `MetadataQuorumCommand`, producer
and consumer performance tools, broker request metrics, and operating-system
resource observations. Choose exact metric names from this checkout's docs.

Run controlled changes with a fixed workload and baseline; record record size,
compression, partitions, replication, acknowledgments, and consumer behavior.
Measure throughput, p50/p95/p99 latency, errors, CPU, disk, network, and lag;
do not generalize laptop limits into production capacity recommendations.

Include partition/retention sizing, reassignment and recovery headroom, listener
versus advertised-listener configuration, ACL/TLS/SASL concepts, quotas, rolling
changes, compatibility, feature levels, and recovery procedures.

Acceptance: diagnose at least three injected symptoms using evidence, reject a
plausible alternative hypothesis, and explain the tradeoff of the proposed fix.
Separate broker-failure tolerance from backup and disaster-recovery strategy.

### 9. Complete a capstone and make a contribution-quality investigation

Design a small workload with explicit throughput, latency, ordering, durability,
replay, and recovery requirements. Derive rather than guess its configuration.

Deliver:

- A whole-system diagram and one annotated record lifecycle.
- A correctness matrix for producer, broker, controller, and consumer failures.
- An experiment notebook with predictions, commands, configs, and observations.
- An operational runbook connecting symptoms to evidence and recovery.
- One bounded source investigation: reproduce a behavior, identify the exact
  invariant and code path, and add or propose a focused regression test.

A code change is optional and requires a real demonstrated defect; do not invent
a fix just to make a contribution. Read `CONTRIBUTING.md` before preparing an
upstream change.

Acceptance: defend the design, explain failures without relying on configuration
folklore, and reproduce a source-backed conclusion with a focused test.

## Test-reading and execution strategy

Prefer a small relevant test over the full repository suite. The following
classes exist in this checkout and provide initial reading/execution anchors:

| Milestone | Focused test command |
|---|---|
| Producer | `./gradlew clients:test --tests org.apache.kafka.clients.producer.internals.SenderTest` |
| Storage | `./gradlew storage:test --tests org.apache.kafka.storage.internals.log.LogSegmentTest` |
| Data replication | `./gradlew core:test --tests kafka.server.ReplicaManagerTest` |
| Consumer | `./gradlew clients:test --tests org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumerTest` |
| Groups | `./gradlew group-coordinator:test --tests org.apache.kafka.coordinator.group.GroupMetadataManagerTest` |
| Metadata controller | `./gradlew metadata:test --tests org.apache.kafka.controller.QuorumControllerTest` |
| KRaft | `./gradlew raft:test --tests org.apache.kafka.raft.KafkaRaftClientTest` |

These commands are planned execution anchors, not claimed passing results.
Select individual methods after inspecting each test. Use tests to study
deterministic edge cases; use local clusters to connect those cases to observable
behavior. Build failures and experiment mismatches must be recorded and resolved,
not treated as successful demonstrations.

## Learning artifacts and progression

For each milestone, keep a short concept explanation, diagram, source trace,
prediction/observation table, and unanswered questions in the session workspace.
Revisit the whole-system diagram as the model becomes more precise.

Milestones form a deliberate learning sequence: environment -> overview ->
produce -> storage -> replication -> consumers -> KRaft -> transactions ->
operations -> capstone. Bring operational observations into earlier labs, but
defer confident tuning recommendations until the relevant mechanisms are clear.

Advance only when the acceptance criteria can be explained without notes and
supported by a source trace or observed experiment. Treat expertise as an ongoing
practice of prediction, diagnosis, and evidence, not a certificate conferred by
finishing this outline.
