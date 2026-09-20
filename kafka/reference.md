# Kafka

**Created by LinkedIn**

## Problem Kafka Solves

Kafka is designed to handle **high-throughput, real-time data streams** between different services. It helps prevent databases and application servers from becoming overloaded when a system receives a large number of continuous events.

### 1. Zomato – Live Delivery Tracking

Suppose Zomato directly stores the driver's live GPS location in the database every few seconds.

For example:

```text
Driver Phone
     ↓
Server
     ↓
Database
```

If thousands or millions of delivery partners are continuously sending their locations, the database would receive a huge number of write requests.

This can cause:

* High database throughput
* Increased database load
* Increased latency
* Database bottlenecks
* Possible service degradation or failure

Kafka can act as a **buffer between the location producers and the services that process the data**:

```text
Driver Phone
     ↓
Location Service
     ↓
   Kafka
     ↓
Location Processing Services
     ↓
Database / Analytics / Tracking
```

The driver does not need to wait for every downstream service to process the location immediately. Kafka stores the events and allows consumers to process them asynchronously.

---

### 2. Discord – Real-Time Messaging

Suppose a Discord server has **5,000+ users online** and users are continuously sending messages.

A naive approach might be:

```text
User
 ↓
Server
 ↓
Database
 ↓
Read message
 ↓
Send to other users
```

With a large number of messages, directly writing every message to the database and then immediately reading it back for distribution can create:

* High database load
* Increased latency
* Large numbers of concurrent requests
* Throughput bottlenecks
* Difficulty scaling the system

Kafka can be used to separate **message production from message processing**:

```text
User
 ↓
Chat Server
 ↓
Kafka
 ↓
Message Consumers
 ├── Message Delivery
 ├── Persistence
 ├── Notifications
 └── Analytics
```

The message can be published as an event, while different consumers independently process that event.

For example, one consumer can handle message delivery while another stores messages for persistence and another handles analytics.

---

### 3. Uber/Ola – Live Ride Tracking & Analytics

Consider a ride where a driver is continuously sending location updates:

```text
Driver
 ↓
Location Updates
 ↓
Backend
```

During a trip, the system may need to process the driver's location for multiple purposes:

* Live location tracking
* ETA calculation
* Route analysis
* Driver/rider tracking
* Fraud detection
* Trip analytics
* Surge/demand analysis

If every location update is synchronously sent to multiple services and written directly to databases, the backend can experience extremely high throughput and processing requirements.

Kafka can act as the central event-streaming layer:

```text
Driver App
     ↓
Location Service
     ↓
   Kafka
     ↓
 ┌──────────────┬──────────────┬──────────────┐
 ↓              ↓              ↓              ↓
Tracking      ETA Service    Analytics    Persistence
Service
```

This allows multiple services to consume the same stream of location events independently without the driver service having to communicate synchronously with every downstream service.

---

## The Common Problem

All three examples have a similar pattern:

> **A large number of producers continuously generate events, while multiple backend services need to process those events.**

Without an event-streaming system:

```text
Many Producers
      ↓
   Backend
      ↓
   Database
      ↓
Multiple Services
```

This can create a bottleneck because the database or backend becomes responsible for handling too much work synchronously.

With Kafka:

```text
Many Producers
      ↓
   Kafka
      ↓
 ┌────┴────┬────────┬─────────┐
 ↓         ↓        ↓         ↓
Service A Service B Service C Analytics
```

Kafka provides a **durable, distributed, high-throughput event streaming layer** between producers and consumers.

### In Simple Words

**Kafka helps decouple producers and consumers and allows large amounts of events to be processed asynchronously and independently.**

Instead of:

```text
Producer → Database → Consumer
```

we can use:

```text
Producer → Kafka → Consumers
```

This makes it easier to build systems that need to handle **high throughput, real-time events, multiple consumers, and scalable processing**.



