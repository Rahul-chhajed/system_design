Absolutely — here is a structured `reference.md` you can directly keep as your Kafka system-design reference.

# Kafka Reference

## 1. What is Kafka?

**Apache Kafka** is a distributed event-streaming platform originally developed at **LinkedIn**.

Kafka is designed to handle:

* High-throughput data
* Real-time event streaming
* Large volumes of messages
* Distributed processing
* Fault tolerance
* Event-driven architectures
* Asynchronous communication between services

### Basic Architecture

```text
Producer
   |
   v
 Kafka Cluster
   |
   +---- Broker 1
   +---- Broker 2
   +---- Broker 3
   |
   v
Consumer
```

A simple mental model:

```text
Producer → Kafka → Consumer
```

---

# 2. What Problem Does Kafka Solve?

Consider a system such as Zomato, Uber, Ola, or Discord.

Thousands or millions of events can be generated continuously.

For example:

```text
Driver → Location Update
User   → Message
Payment → Transaction Event
Order  → Status Update
```

If every event directly interacts with the database and multiple backend services:

```text
                ┌→ Database
Producer → Server ├→ Analytics
                ├→ Notification
                └→ Other Services
```

the backend and database can become bottlenecks.

Kafka introduces an intermediate event-streaming layer:

```text
Producer
   |
   v
 Kafka
   |
   +---- Consumer 1
   +---- Consumer 2
   +---- Consumer 3
   +---- Consumer 4
```

This provides:

* Decoupling
* Asynchronous processing
* High throughput
* Horizontal scalability
* Fault tolerance
* Multiple independent consumers

---

# 3. Important Kafka Components

The most important Kafka concepts are:

```text
Producer
   ↓
Broker
   ↓
Topic
   ↓
Partition
   ↓
Offset
   ↓
Consumer
   ↓
Consumer Group
```

Kafka also uses:

```text
Replication
ISR
ACK
Retention
```

---

# 4. Producer

A **Producer** is an application that sends messages/events to Kafka.

Example:

```text
Driver App
    |
    | location = Bangalore
    ↓
 Kafka
```

Another example:

```text
Order Service
    |
    | OrderCreated
    ↓
 Kafka
```

A producer sends a record containing information such as:

```json
{
    "orderId": 123,
    "userId": 45,
    "amount": 500
}
```

---

# 5. Broker

A **Kafka Broker** is a Kafka server responsible for storing and serving messages.

A Kafka cluster normally contains multiple brokers.

Example:

```text
Kafka Cluster

┌──────────────┐
│   Broker 1   │
└──────────────┘

┌──────────────┐
│   Broker 2   │
└──────────────┘

┌──────────────┐
│   Broker 3   │
└──────────────┘
```

Each broker can store multiple partitions.

```text
Broker 1
 ├── Topic A - Partition 0
 ├── Topic A - Partition 1
 └── Topic B - Partition 0

Broker 2
 ├── Topic A - Partition 2
 └── Topic B - Partition 1
```

Kafka distributes data across brokers to achieve scalability.

---

# 6. Topic

A **Topic** is a logical category/name to which messages are written.

For example:

```text
orders
payments
user-events
driver-location
notifications
```

A producer sends messages to a topic:

```text
Producer
   |
   | OrderCreated
   v
orders topic
```

A topic can have multiple partitions.

```text
orders

Partition 0
Partition 1
Partition 2
Partition 3
```

---

# 7. Partition

A **Partition** is the fundamental unit of parallelism and storage in Kafka.

For example:

```text
orders topic

Partition 0
Partition 1
Partition 2
Partition 3
```

Each partition is an **ordered, append-only log**.

Example:

```text
Partition 0

Offset
  0 → Order A
  1 → Order B
  2 → Order C
  3 → Order D
  4 → Order E
```

New messages are appended to the end:

```text
0 → A
1 → B
2 → C
3 → D
4 → E
5 → F
```

Kafka does not normally modify existing records.

---

# 8. Why Partitions?

Partitions allow Kafka to process messages in parallel.

Suppose we have:

```text
1 Partition
```

Only one consumer in a consumer group can actively consume that partition at a time.

With:

```text
4 Partitions
```

we can have:

```text
Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 3
Partition 3 → Consumer 4
```

Therefore:

```text
More partitions
       ↓
More parallelism
       ↓
Higher throughput
```

---

# 9. Message Ordering

Kafka guarantees ordering **within a partition**.

Example:

```text
Partition 0

0 → A
1 → B
2 → C
3 → D
```

The consumer receives:

```text
A → B → C → D
```

However, Kafka does **not** provide global ordering across multiple partitions.

For example:

```text
Partition 0: A → C

Partition 1: B → D
```

There is no guaranteed global order:

```text
A → B → C → D
```

---

# 10. Kafka Key

A producer can send a message with a key:

```json
{
    "key": "user123",
    "value": "OrderCreated"
}
```

Kafka uses the key to determine the partition.

Conceptually:

```text
partition = hash(key) % numberOfPartitions
```

Therefore:

```text
user123
   ↓
hash
   ↓
Partition 2
```

This is useful when messages belonging to the same entity must remain ordered.

For example:

```text
user123 → OrderCreated
user123 → PaymentCompleted
user123 → OrderShipped
```

If they use the same key, they can be routed to the same partition.

---

# 11. Offset

Every message inside a partition has an **offset**.

Example:

```text
Partition 0

Offset    Message

0         A
1         B
2         C
3         D
4         E
```

The offset identifies the position of a message inside a partition.

Important:

> Offset is unique only within a partition.

For example:

```text
Partition 0 → Offset 10
Partition 1 → Offset 10
```

Both can exist.

---

# 12. Consumer

A **Consumer** reads messages from Kafka.

```text
Kafka
  |
  v
Consumer
```

Example:

```text
orders topic
     |
     v
Order Processing Service
```

The consumer maintains its position using offsets.

For example:

```text
Processed:

0 ✓
1 ✓
2 ✓
3 ✓
4 ← next
```

---

# 13. Consumer Group

A **Consumer Group** is a group of consumers working together to consume a topic.

Example:

```text
Topic: orders

Partition 0
Partition 1
Partition 2
Partition 3
```

Consumer group:

```text
Order Service Group

Consumer A → Partition 0
Consumer B → Partition 1
Consumer C → Partition 2
Consumer D → Partition 3
```

Each partition is assigned to only one consumer within the same consumer group at a time.

---

# 14. Multiple Consumer Groups

This is one of Kafka's most important features.

Suppose:

```text
orders topic
```

has three independent services:

```text
Order Processing
Analytics
Notification
```

We can create three consumer groups:

```text
                orders
                   |
       ┌───────────┼────────────┐
       ↓           ↓            ↓
 Order Group   Analytics     Notification
               Group           Group
```

Each group maintains its own offsets.

Therefore all three groups can consume the same events independently.

```text
OrderCreated
     |
     +---- Order Service
     |
     +---- Analytics
     |
     +---- Notification
```

---

# 15. Consumer Group vs Consumer

Important distinction:

```text
Consumer
```

is an individual application instance.

```text
Consumer Group
```

is a logical group of consumers working together.

Example:

```text
Order Service Group

Consumer 1
Consumer 2
Consumer 3
```

If there are 3 partitions:

```text
P0 → Consumer 1
P1 → Consumer 2
P2 → Consumer 3
```

---

# 16. What Happens If Consumers > Partitions?

Suppose:

```text
3 partitions
5 consumers
```

Then only 3 consumers can actively consume:

```text
P0 → C1
P1 → C2
P2 → C3

C4 → idle
C5 → idle
```

Therefore:

```text
Maximum active consumers in a group
≈ number of partitions
```

This is an important Kafka scaling concept.

---

# 17. Replication

Kafka replicates partitions across brokers for fault tolerance.

Suppose:

```text
Partition 0
```

has replication factor 3.

It may look like:

```text
Broker 1 → Partition 0 (Leader)
Broker 2 → Partition 0 (Follower)
Broker 3 → Partition 0 (Follower)
```

If Broker 1 fails:

```text
Broker 2
   ↓
becomes Leader
```

This prevents data loss and improves availability.

---

# 18. Leader and Followers

Each partition has:

```text
1 Leader
N Followers
```

Example:

```text
Partition 0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

Producers and consumers primarily interact with the partition leader.

Followers replicate the leader's data.

```text
Producer
   |
   v
Leader
 ├──→ Follower 1
 └──→ Follower 2
```

---

# 19. ISR — In-Sync Replicas

**ISR = In-Sync Replicas**

These are replicas that are sufficiently caught up with the partition leader according to Kafka's replication rules.

Example:

```text
Partition 0

Leader
Broker 1

ISR:
Broker 1
Broker 2
Broker 3
```

If Broker 3 becomes too far behind:

```text
ISR:

Broker 1
Broker 2
```

Broker 3 is temporarily removed from the ISR.

---

# 20. Why ISR Matters

ISR is important for reliability.

Suppose:

```text
Replication Factor = 3

Broker 1 → Leader
Broker 2 → ISR
Broker 3 → ISR
```

If Broker 1 fails:

```text
Broker 2
   ↓
can become the new leader
```

The exact availability behavior also depends on Kafka's configuration, including settings such as `min.insync.replicas`.

---

# 21. ACK — Producer Acknowledgement

`acks` controls how much acknowledgement the producer requires from Kafka before considering a write successful.

Common values:

```text
acks=0
acks=1
acks=all
```

---

## acks=0

Producer does not wait for an acknowledgement from the broker.

```text
Producer
   |
   | message
   ↓
Broker

Producer continues
```

### Advantage

Very low latency.

### Disadvantage

The producer may not know whether the broker successfully received the message.

---

# 22. acks=1

The producer waits for the partition leader to acknowledge the write.

```text
Producer
   |
   v
Leader
   |
 ACK
   |
   v
Producer
```

The leader does not need to wait for all followers before responding.

This provides more durability than `acks=0`, but less than `acks=all`.

---

# 23. acks=all

The producer waits until the message is acknowledged according to the configured in-sync replica requirements.

Conceptually:

```text
Producer
   |
   v
Leader
 ├──→ Follower
 └──→ Follower
   |
 ACK
   |
   v
Producer
```

This provides stronger durability.

A common production configuration is:

```text
acks=all
```

combined with appropriate replication and `min.insync.replicas`.

---

# 24. Retention

Kafka does **not** normally delete a message immediately after a consumer reads it.

This is a major difference from many traditional queue systems.

Example:

```text
Kafka Topic

0 → A
1 → B
2 → C
3 → D
```

Consumer reads:

```text
A
B
C
D
```

The records can still remain in Kafka.

They are removed according to the topic's retention configuration.

---

# 25. Retention by Time

Example:

```text
retention.ms = 7 days
```

Kafka can retain records for approximately the configured retention period, subject to the topic's retention mechanics.

After the retention period, old log segments become eligible for deletion.

---

# 26. Retention by Size

Kafka can also limit the amount of log data retained.

For example:

```text
retention.bytes = 100 GB
```

When the configured size limit is reached, older log segments become eligible for deletion.

---

# 27. Kafka Is an Append-Only Log

A useful way to understand Kafka:

```text
Partition

0 → Event A
1 → Event B
2 → Event C
3 → Event D
4 → Event E
```

New events are appended:

```text
5 → Event F
6 → Event G
7 → Event H
```

Kafka does not behave like a normal database table where records are constantly updated in place.

This append-oriented design is one of the reasons Kafka can achieve high sequential write throughput.

---

# 28. Kafka Data Flow

Complete flow:

```text
                 Kafka Cluster
              ┌─────────────────┐
              │                 │
Producer ────→ │     Topic       │
              │       │         │
              │       ↓         │
              │   Partitions    │
              │       │         │
              └───────┼─────────┘
                      ↓
                 Consumer Group
                      |
             ┌────────┼────────┐
             ↓        ↓        ↓
           C1         C2       C3
```

---

# 29. Complete Example — Food Delivery

Suppose a food delivery application generates:

```text
OrderCreated
PaymentCompleted
RestaurantAccepted
DriverAssigned
DriverLocationUpdated
OrderDelivered
```

These can become Kafka events.

```text
Mobile App
    |
    v
Backend Service
    |
    v
Kafka
    |
    v
orders topic
```

The topic may contain:

```text
Partition 0
Partition 1
Partition 2
```

Different services can consume the events:

```text
                 orders topic
                      |
        ┌─────────────┼──────────────┐
        ↓             ↓              ↓
 Order Service   Analytics       Notification
    Group           Group            Group
```

---

# 30. Kafka vs Traditional Database

### Direct Database Approach

```text
100,000 Events/sec
        |
        v
     Database
        |
        v
   Bottleneck
```

### Kafka-Based Approach

```text
100,000 Events/sec
        |
        v
      Kafka
        |
   ┌────┼─────┐
   ↓    ↓     ↓
 DB   Analytics Notification
```

Kafka separates event ingestion from downstream processing.

However:

> Kafka does not replace a database.

Kafka and databases solve different problems.

A database is generally used for querying and managing application state.

Kafka is primarily used for event streaming and durable event transport.

---

# 31. Kafka vs RabbitMQ

Both Kafka and RabbitMQ are messaging technologies, but their common use cases and architecture differ.

| Feature           | Kafka                                   | RabbitMQ                                         |
| ----------------- | --------------------------------------- | ------------------------------------------------ |
| Primary model     | Distributed event log / event streaming | Message broker                                   |
| Storage           | Persistent log                          | Queue-based messaging                            |
| Ordering          | Per partition                           | Queue-level ordering under applicable conditions |
| Replay            | Strong built-in replay model            | Not its primary design                           |
| Throughput        | Very high for streaming workloads       | Very good for messaging workloads                |
| Consumer model    | Consumer groups                         | Consumers on queues                              |
| Message retention | Configurable retention                  | Messages generally removed after acknowledgement |
| Scaling           | Partitions + brokers                    | Queues + consumers / clustering                  |
| Best suited for   | Event streaming, analytics, logs        | Task queues, routing, traditional messaging      |

---

# 32. Kafka vs RabbitMQ Example

### RabbitMQ

Suppose:

```text
Order Service
     |
     v
RabbitMQ Queue
     |
     v
Worker
```

Worker processes:

```text
Order A
```

After successful acknowledgement, the message is normally removed from the queue.

---

### Kafka

```text
Order Service
     |
     v
Kafka Topic
     |
     v
Consumer
```

The consumer reads:

```text
Offset 100
```

The record can remain available according to the topic's retention configuration.

Another consumer can independently read the same event.

---

# 33. When to Use Kafka

Kafka is a strong fit for:

### Event Streaming

```text
UserActivity
PaymentEvents
LocationUpdates
OrderEvents
```

### Event-Driven Architecture

```text
Service A
   ↓
Kafka
   ↓
Service B
Service C
Service D
```

### Log Aggregation

```text
Application Logs
       ↓
     Kafka
       ↓
Analytics / Storage
```

### Real-Time Analytics

```text
Events
  ↓
Kafka
  ↓
Stream Processing
  ↓
Analytics
```

### High-Throughput Data Pipelines

```text
Millions of Events
       ↓
     Kafka
       ↓
Data Processing
       ↓
Data Warehouse
```

---

# 34. When RabbitMQ May Be More Appropriate

RabbitMQ is commonly useful for:

```text
Task Queue
Background Jobs
Work Queues
Complex Message Routing
Request/Worker Systems
```

Example:

```text
API Server
    |
    v
RabbitMQ
    |
    v
Worker
    |
    v
Generate PDF
```

The worker can process the task asynchronously without making the API request wait for the entire operation.

---

# 35. Kafka Architecture — Big Picture

```text
                    PRODUCERS
                        |
          ┌─────────────┼─────────────┐
          ↓             ↓             ↓
      Service A     Service B     Service C
          |             |             |
          └─────────────┼─────────────┘
                        ↓
                ┌───────────────┐
                │     Kafka     │
                │    Cluster    │
                └───────────────┘
                        |
          ┌─────────────┼─────────────┐
          ↓             ↓             ↓
       Broker 1      Broker 2      Broker 3
          |             |             |
          └─────────────┼─────────────┘
                        ↓
                  Consumer Groups
               ┌────────┼─────────┐
               ↓        ↓         ↓
             Group A  Group B   Group C
```

---

# 36. Node.js Kafka Implementation

A popular Node.js Kafka client is **KafkaJS**.

Install:

```bash
npm install kafkajs
```

---

# 37. Create Kafka Client

```javascript
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "order-service",
    brokers: ["localhost:9092"]
});
```

Here:

```text
clientId
```

identifies the application.

```text
brokers
```

contains Kafka broker addresses.

---

# 38. Producer

Create a producer:

```javascript
const producer = kafka.producer();

async function startProducer() {
    await producer.connect();

    await producer.send({
        topic: "orders",

        messages: [
            {
                key: "order-123",

                value: JSON.stringify({
                    orderId: 123,
                    userId: 45,
                    amount: 500
                })
            }
        ]
    });

    await producer.disconnect();
}

startProducer();
```

Flow:

```text
Node.js Application
        |
        v
     Producer
        |
        v
orders topic
```

---

# 39. Consumer

```javascript
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "order-consumer",
    brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({
    groupId: "order-service-group"
});

async function startConsumer() {

    await consumer.connect();

    await consumer.subscribe({
        topic: "orders",
        fromBeginning: true
    });

    await consumer.run({

        eachMessage: async ({ topic, partition, message }) => {

            console.log(
                "Topic:",
                topic
            );

            console.log(
                "Partition:",
                partition
            );

            console.log(
                "Offset:",
                message.offset
            );

            console.log(
                "Message:",
                message.value.toString()
            );
        }
    });
}

startConsumer();
```

---

# 40. Producer + Consumer Architecture

```text
             Node.js Producer
                    |
                    v
              orders topic
                    |
          ┌─────────┴─────────┐
          ↓                   ↓
     Consumer Group A    Consumer Group B
          ↓                   ↓
     Order Service        Analytics
```

This demonstrates one of Kafka's major benefits:

**One event can be consumed independently by multiple services.**

---

# 41. Kafka Scaling

Suppose:

```text
Topic
 ├── Partition 0
 ├── Partition 1
 ├── Partition 2
 └── Partition 3
```

Consumer group:

```text
Consumer 1 → P0
Consumer 2 → P1
Consumer 3 → P2
Consumer 4 → P3
```

If traffic increases, we can add consumers:

```text
More traffic
     ↓
More partitions
     ↓
More consumers
     ↓
More parallel processing
```

However, partition count should be designed carefully because changing partition counts can affect key-based distribution and ordering expectations.

---

# 42. Kafka and Backpressure

Suppose producers generate:

```text
100,000 events/sec
```

but consumers can process only:

```text
50,000 events/sec
```

Instead of immediately forcing consumers to process everything synchronously:

```text
Producer
   ↓
Consumer
   ↓
Overloaded
```

Kafka can retain the backlog:

```text
Producer
   ↓
 Kafka
   ↓
 ┌───────────────┐
 │ Event Backlog │
 └───────────────┘
        ↓
     Consumer
```

Consumers can process the backlog as capacity becomes available.

This is one reason Kafka is useful for high-throughput systems.

---

# 43. Kafka Does Not Magically Increase Throughput

Important interview point:

Kafka itself does not magically make an application infinitely fast.

Throughput depends on:

```text
Partitions
+
Brokers
+
Disk
+
Network
+
Producer configuration
+
Consumer configuration
+
Message size
+
Replication
+
Consumer processing speed
```

Kafka provides the architecture for **distributed and parallel processing**.

---

# 44. Why Kafka Can Have High Throughput

Several design choices contribute to Kafka's throughput:

### 1. Sequential Append

Messages are appended to logs.

```text
A → B → C → D → E
```

### 2. Partitioning

Data is distributed:

```text
Partition 0
Partition 1
Partition 2
Partition 3
```

### 3. Parallel Processing

Multiple consumers can process different partitions simultaneously.

### 4. Batching

Producers can send multiple records together instead of making one network request per message.

```text
Message 1
Message 2
Message 3
Message 4
```

becomes a batch.

### 5. Compression

Kafka supports compression such as:

```text
gzip
snappy
lz4
zstd
```

### 6. Sequential Disk I/O

Kafka's log-oriented storage model makes sequential writes an important part of its performance characteristics.

### 7. Zero-Copy Optimizations

Kafka can use OS-level mechanisms such as `sendfile` to efficiently transfer data from page cache to network sockets in supported paths.

---

# 45. Kafka's Core Mental Model

Remember Kafka using this chain:

```text
Producer
   ↓
Broker
   ↓
Topic
   ↓
Partition
   ↓
Offset
   ↓
Consumer
   ↓
Consumer Group
```

Then add:

```text
Replication
ISR
ACK
Retention
```

---

# 46. One Complete Example

Imagine Uber-like live location tracking.

```text
Driver App
    |
    | Location Event
    v
Location Service
    |
    v
Kafka
    |
    v
driver-location topic
    |
    ├── Partition 0
    ├── Partition 1
    ├── Partition 2
    └── Partition 3
          |
          v
    Consumer Groups
    ├── Live Tracking
    ├── ETA Calculation
    ├── Analytics
    └── Data Storage
```

The same location event can therefore be consumed by multiple independent systems.

```text
Driver
  ↓
Kafka
  ├──→ Live Tracking
  ├──→ ETA
  ├──→ Analytics
  └──→ Storage
```

This is the core idea behind using Kafka in event-driven and high-throughput architectures.

---

# 47. Interview Quick Revision

### Kafka

> Distributed event-streaming platform.

### Producer

> Sends records to Kafka.

### Broker

> Kafka server that stores and serves records.

### Topic

> Logical category of events.

### Partition

> Ordered append-only log and unit of parallelism.

### Offset

> Position of a record within a partition.

### Consumer

> Reads records from Kafka.

### Consumer Group

> Multiple consumers working together to consume partitions.

### Replication

> Copies partitions across brokers for fault tolerance.

### ISR

> Replicas currently considered in sync with the leader.

### ACK

> Producer acknowledgement level.

### Retention

> Determines how long/how much data Kafka retains.

### Key

> Can determine partition assignment and preserve per-key ordering when records are routed to the same partition.

---

# 48. Most Important Diagram

```text
                         PRODUCER
                            |
                            v
                    ┌──────────────┐
                    │    KAFKA     │
                    │    TOPIC     │
                    └──────────────┘
                            |
             ┌──────────────┼──────────────┐
             ↓              ↓              ↓
        Partition 0    Partition 1    Partition 2
             |              |              |
             ↓              ↓              ↓
          Offset         Offset         Offset
             |              |              |
             └──────────────┼──────────────┘
                            ↓
                     CONSUMER GROUP
                    ┌───────┼───────┐
                    ↓       ↓       ↓
                   C1      C2      C3
                    |
                    ↓
                PROCESSING

        Replication happens across brokers
        ISR tracks in-sync replicas
        ACK controls producer acknowledgement
        Retention controls how long data remains
```

---

# 49. One-Line Summary

```text
Kafka = Distributed + Durable + High-Throughput + Partitioned
        Event Streaming Platform
```

The most important idea to remember:

```text
Producers generate events
        ↓
Kafka stores and distributes events
        ↓
Partitions provide parallelism
        ↓
Consumer groups provide scalable processing
        ↓
Replication provides fault tolerance
        ↓
Offsets provide consumer position
        ↓
Retention allows events to remain available for replay
```
