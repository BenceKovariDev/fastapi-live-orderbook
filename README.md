# ⚡ Low-Latency Binance-Grade Matching Engine & Live Streamer

A high-performance, real-time cryptocurrency orderbook matching engine implemented in Python using FastAPI, optimized with an in-memory data structure for atomic execution, and backed by Redis Pub/Sub for scalable, live fan-out WebSocket broadcasting.

## 🚀 Key Features

*   **In-Memory Matching Engine:** Performs real-time order matching (Bids/Asks) with atomic trade execution and price-time priority optimization.
*   **Redis Pub/Sub Layer:** Decouples the core execution engine from the communication layer, allowing ultra-low latency event streaming.
*   **Async Fan-Out Connection Manager:** Efficiently broadcasts live orderbook updates to thousands of concurrent WebSocket clients simultaneously.
*   **Asynchronous Architecture:** Built from the ground up using `asyncio` and `FastAPI` to guarantee high throughput under heavy traffic.
*   **Terminal UI:** Includes a built-in real-time frontend to monitor market depth and manually execute limit orders.

## 🏗️ Architecture Overview

[ Limit Order Post ] ──> [ FastAPI Endpoint ]
│
▼
[ In-Memory Matching Engine ]
│
(Atomic Match)
│
▼
[ Redis Pub/Sub Channel ]
│
(Fan-Out Broadcast)
│
▼
[ Async Connection Manager ] ──> [ WebSockets (Live UI) ]

## 🛠️ Tech Stack

*   **Backend Framework:** FastAPI (Python 3.14+)
*   **Asynchronous Runtime:** Asyncio / Uvicorn
*   **Message Broker & State Invalidation:** Redis (aioredis)
*   **Data Validation:** Pydantic v2
*   **Frontend:** Real-time Async WebSocket Terminal

## ⚡ Quick Start

### Prerequisites
Make sure you have a Redis server running in your environment:
```bash
redis-server --daemonize yes --ignore-warnings ARM64-COW-BUG
