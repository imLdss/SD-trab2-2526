# SD T2 - Fault-Tolerant Distributed Messaging System

An extension of a distributed message delivery platform developed for the **Distributed Systems** course at **NOVA School of Science and Technology (FCT NOVA)**.

This second assignment builds on the original domain-based messaging system and introduces three major distributed-systems concerns:

- **Security**
- **External service integration**
- **Fault tolerance**

The architecture and service interfaces remain compatible with the first assignment, while the system is extended with stronger authentication, external email integration, and replicated message servers.

---

## 📌 Project Overview

The original system provides domain-based user management and message delivery.

Each user belongs to a domain and interacts with the services provided by that domain. Messages can be delivered locally or forwarded to remote domains.

This second assignment extends that system with:

1. secure client/server and server/server communication;
2. integration with an external email provider through OAuth;
3. replicated message servers capable of tolerating failures.

---

## 🔐 Security

The system must prevent unauthorized access to both the **Users Service** and the **Messages Service**.

### Client → Server Authentication

Clients authenticate servers through the servers' **public-key certificates**.

Servers authenticate clients using the users' passwords already included in the original API design.

### Server → Server Authentication

Distributed operations such as forwarding messages between domains also require authentication.

Servers initiating inter-server communication must prove that they are trusted.

A shared secret can be passed when starting the services and used to protect operations that should only be executed by other trusted servers.

This prevents normal clients from invoking internal server-to-server operations.

---

## 🌐 External Email Service Integration

One possible extension is a Messages Server backed by an **external email service** instead of a local persistence layer.

The assignment suggests using:

```text
Zoho Mail
```

through a REST API with **OAuth authentication**.

The external-backed Messages Server must still expose the same Messages REST interface as the rest of the system.

### Responsibilities

The server must be able to:

- communicate with other Messages Servers;
- communicate with the Users Server in its domain;
- send and retrieve messages through the external email provider;
- preserve the predefined Messages Service API.

Instead of storing a mailbox in a local database, the implementation stores messages using the external provider.

Message metadata such as:

```text
id
sender
destination
creationTime
```

can be embedded in the email body so that the original distributed-system message representation can be reconstructed.

### OAuth

OAuth-based access can be implemented using a library such as:

```text
ScribeJava
```

---

## 🛡️ Fault Tolerance

A major objective of the project is to tolerate failures of machines running the **Messages Server**.

This requires replication of the message service state.

The specification proposes multiple possible approaches.

---

## 🟠 Option F1 — Kafka-Based Replication

The Messages Server can be replicated using a state-machine replication strategy based on an indirect communication system such as **Kafka**.

### Main Requirements

- Any domain may be replicated.
- Multiple domains may be replicated simultaneously.
- Failure of a replicated Messages Server must be tolerated.
- Replication is restricted to servers within the same domain.
- Kafka must **not** be used for inter-domain message delivery.

A single Kafka instance is assumed to exist in the testing environment.

### Monotonic Reads

The system must guarantee that once a client has observed a given state version, future reads never return an older version.

Conceptually:

```text
Client reads version 5
        ↓
Future reads must return version >= 5
```

This can be implemented using custom response headers.

The automated tester forwards headers beginning with:

```text
X-MESSAGES
```

allowing clients and replicas to maintain version information across requests.

---

## 🔵 Option F2a — Primary / Secondary Replication

Another approach is a **primary/secondary replication protocol**.

The primary server handles the authoritative state and propagates updates to secondary replicas.

### Requirements

The implementation must:

- replicate Messages Servers across different domains;
- tolerate the failure of one server;
- continue allowing reads if the primary fails;
- guarantee monotonic reads;
- expose a REST interface.

If the primary fails, write operations are not required to remain available, but reads must still be possible from sufficiently up-to-date replicas.

---

## 🟣 Option F2b — Primary / Secondary Without Primary Fault Masking

A simplified primary/secondary alternative also exists.

This model must tolerate failures of secondary replicas but does not need to fully mask a primary failure.

If the primary becomes unavailable:

- reads must remain possible;
- writes do not need to remain available.

The system still needs to preserve the monotonic-read guarantee for clients.

---

## 📈 Version Consistency

A central requirement of the replicated system is preventing clients from observing state regression.

Suppose a client reads:

```text
Version 2 from Server A
```

Then later requests sent to another replica must return:

```text
Version >= 2
```

This is stronger than simply allowing reads from any available replica.

The implementation must therefore track version information and ensure that requests are only served by replicas that are sufficiently up to date.

---

## 🏗️ Architecture

At a high level, the system becomes:

```text
                     Client
                        |
                        v
                 +-------------+
                 |   Gateway   |
                 +-------------+
                    /       \
                   v         v
          +-------------+  +----------------+
          | Users Server|  | Messages Server|
          +-------------+  +----------------+
                                  |
                           Replication Layer
                         /        |        \
                        v         v         v
                    Replica 1  Replica 2  Replica 3
                                  |
                                  v
                        External / Remote Services
```

Depending on the chosen extension, the replication layer may use:

```text
Kafka
```

or a custom:

```text
Primary / Secondary protocol
```

---

## 🔄 Temporary Communication Failures

As in the first assignment, the system must continue to account for temporary communication failures.

The implementation therefore needs robust handling of:

- failed remote calls;
- retry logic;
- delayed replicas;
- temporarily unreachable services;
- stale replica state.

---

## 🛠️ Technologies

- **Java 17**
- **JAX-RS**
- **REST APIs**
- **OAuth 2.0**
- **Public-key certificates**
- **ScribeJava**
- **Apache Kafka** *(replication option)*
- **Primary / Secondary Replication**
- **Docker**
- **Maven**
- **Concurrent Programming**
- **Distributed Systems**

---

## 🧠 Distributed Systems Concepts

This project explores several important topics in distributed computing:

- **Authentication**
- **Secure channels**
- **Public-key infrastructure**
- **Server-to-server authentication**
- **OAuth**
- **External service integration**
- **State machine replication**
- **Primary / Secondary replication**
- **Kafka-based replication**
- **Fault tolerance**
- **Replica consistency**
- **Monotonic reads**
- **Version tracking**
- **Failure handling**
- **Distributed APIs**
- **Containerized deployment**

---

## 🎯 Key Learning Outcomes

This assignment focuses on the practical challenges of evolving a distributed system beyond basic remote communication.

It demonstrates how to:

- secure distributed service interactions;
- distinguish client-facing operations from trusted internal operations;
- integrate third-party services while preserving an existing API;
- replicate application state;
- tolerate server failures;
- maintain consistent client-visible state across replicas;
- combine distributed protocols with real-world infrastructure such as Docker and Kafka.

---

## 🐳 Development Environment

The project targets:

```text
Linux
Java 17
Maven
Docker
```

The official automatic test environment uses Docker containers.

---

## 📁 Suggested Repository Structure

```text
.
├── src/
│   ├── main/
│   │   └── java/
│   └── test/
│       └── java/
├── pom.xml
├── Dockerfile
├── certificates/
├── config/
└── README.md
```

The exact structure depends on the implemented features and replication strategy.

---

## 👤 Author

**Luís Santos Pereira**  
Computer Science / Computer Engineering Student — NOVA School of Science and Technology

GitHub: https://github.com/imLdss
