# Enterprise High-Frequency Order Book Streamer (Step 6)

An enterprise-grade, ultra-low latency order book streaming microservice featuring **Fan-Out Distribution Architecture** and **Network Heartbeat (Keep-Alive)** verification layers.

This repository marks **Step 6 of my 1000-lesson masterclass backend engineering sprint**. Instead of multiplying database connection pools, this system scales linearly by binding thousands of concurrent WebSockets to a single centralized Redis Pub/Sub stream core. Built inside a localized mobile Linux ecosystem (UserLAnd) running Python 3.14 and Redis 8.0.

## 🚀 Advanced Architectural Upgrades

- **Fan-Out Clustering (Resource Protection)**: Implements a centralized connection manager. Only **one** global asynchronous worker subscribes to the Redis instance, multiplexing and distributing incoming frames natively via memory buffers to all connected clients.
- **Bi-Directional Network Heartbeats**: Integrates an asynchronous ping/pong routine to detect stale sockets, prevent memory leaks, and forcefully release dead communication pipes.
- **Graceful Fault Tolerance**: Client-side layers utilize automated state-checking hooks to execute programmatic reconnections during transit failures without disrupting overall system availability.

## 🛠️ Tech Stack

- **Framework**: FastAPI (Asynchronous Python 3.14)
- **Message Broker**: Redis 8.0 (In-Memory Pub/Sub Engine)
- **Protocol**: WebSockets with native Keep-Alive framing
- **Architecture**: Event-Driven Design (EDD / Fan-Out pattern)
