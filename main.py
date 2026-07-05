import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import redis.asyncio as aioredis

app = FastAPI(
    title="Binance-Grade Matching Engine & Streamer v4.0",
    description="Step 7: In-Memory Order Matching Engine with Live Fan-Out WebSocket Broadcasting."
)

REDIS_URL = "redis://127.0.0.1:6379"
CHANNEL_NAME = "live_orderbook"

# --- MEGBÍZÁS MODELL ---
class OrderRequest(BaseModel):
    side: str  # "BUY" vagy "SELL"
    price: float
    amount: float

# --- IN-MEMORY MATCHING ENGINE ---
class MatchingEngine:
    def __init__(self):
        self.bids = [
            {"price": 92500.0, "amount": 1.5},
            {"price": 92499.0, "amount": 2.1},
            {"price": 92498.0, "amount": 0.8}
        ]
        self.asks = [
            {"price": 92502.0, "amount": 1.2},
            {"price": 92503.0, "amount": 0.5},
            {"price": 92504.0, "amount": 3.0}
        ]

    def get_orderbook_payload(self):
        return {
            "symbol": "BTCUSDT",
            "bids": sorted(self.bids, key=lambda x: x["price"], reverse=True),
            "asks": sorted(self.asks, key=lambda x: x["price"])
        }

    async def process_order(self, side: str, price: float, amount: float):
        trades = []
        remaining_amount = amount

        if side.upper() == "BUY":
            self.asks.sort(key=lambda x: x["price"])
            for ask in list(self.asks):
                if ask["price"] <= price and remaining_amount > 0:
                    matched_amount = min(ask["amount"], remaining_amount)
                    ask["amount"] -= matched_amount
                    remaining_amount -= matched_amount
                    trades.append({"price": ask["price"], "amount": matched_amount})
                    if ask["amount"] <= 0:
                        self.asks.remove(ask)
            if remaining_amount > 0:
                self.bids.append({"price": price, "amount": remaining_amount})

        elif side.upper() == "SELL":
            self.bids.sort(key=lambda x: x["price"], reverse=True)
            for bid in list(self.bids):
                if bid["price"] >= price and remaining_amount > 0:
                    matched_amount = min(bid["amount"], remaining_amount)
                    bid["amount"] -= matched_amount
                    remaining_amount -= matched_amount
                    trades.append({"price": bid["price"], "amount": matched_amount})
                    if bid["amount"] <= 0:
                        self.bids.remove(bid)
            if remaining_amount > 0:
                self.asks.append({"price": price, "amount": remaining_amount})

        redis_client = aioredis.from_url(REDIS_URL)
        await redis_client.publish(CHANNEL_NAME, json.dumps(self.get_orderbook_payload()))
        await redis_client.close()
        return trades

engine = MatchingEngine()

# --- FAN-OUT CONNECTION MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: set[WebSocket] = set()
        self.redis_task: asyncio.Task | None = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        await websocket.send_text(json.dumps(engine.get_orderbook_payload()))
        if len(self.active_connections) == 1:
            self.redis_task = asyncio.create_task(self._start_redis_listener())

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if len(self.active_connections) == 0 and self.redis_task:
            self.redis_task.cancel()
            self.redis_task = None

    async def broadcast(self, message: str):
        if not self.active_connections:
            return
        tasks = [websocket.send_text(message) for websocket in self.active_connections]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _start_redis_listener(self):
        redis_client = aioredis.from_url(REDIS_URL)
        pubsub = redis_client.pubsub()
        try:
            await pubsub.subscribe(CHANNEL_NAME)
            async for message in pubsub.listen():
                if message["type"] == "message":
                    await self.broadcast(message["data"].decode("utf-8"))
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(CHANNEL_NAME)
            await redis_client.close()

manager = ConnectionManager()

# --- HTTP ENDPOINT ORDERS ---
@app.post("/api/order")
async def create_order(order: OrderRequest):
    if order.side.upper() not in ["BUY", "SELL"]:
        raise HTTPException(status_code=400, detail="Invalid side.")
    executed_trades = await engine.process_order(order.side, order.price, order.amount)
    return {"status": "ORDER_PROCESSED", "trades_executed": executed_trades}

# --- WEBSOCKET ENDPOINT ---
@app.websocket("/ws/orderbook")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)

# --- TERMINAL FRONTEND ---
@app.get("/")
async def get():
    html_content = """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Binance-Grade Terminal</title>
            <style>
                body { font-family: monospace; background: #121214; color: #e1e1e6; padding: 20px; }
                .layout { display: flex; gap: 40px; }
                .book-side { min-width: 200px; }
                .asks { color: #ea3943; }
                .bids { color: #16c784; }
                h2 { border-bottom: 1px solid #29292e; padding-bottom: 10px; }
                .form-group { margin-bottom: 10px; }
                input, select, button { background: #202024; color: #fff; border: 1px solid #29292e; padding: 8px; font-family: monospace; }
                button { background: #16c784; border: none; cursor: pointer; font-weight: bold; }
            </style>
        </head>
        <body>
            <h1>⚡ Core Matching Engine Terminal (Step 7)</h1>
            <div id="status">Connecting...</div>
            <br>
            <div class="layout">
                <div class="book-side">
                    <h2 class="asks">🔴 ASKS (Eladók)</h2>
                    <pre id="asks_list"></pre>
                    <h2 class="bids">🟢 BIDS (Vevők)</h2>
                    <pre id="bids_list"></pre>
                </div>
                <div>
                    <h2>💼 Place Limit Order</h2>
                    <div class="form-group">
                        <label>Side: </label>
                        <select id="order_side"><option value="BUY">BUY</option><option value="SELL">SELL</option></select>
                    </div>
                    <div class="form-group">
                        <label>Price: </label>
                        <input type="number" id="order_price" value="92502.0" step="0.5">
                    </div>
                    <div class="form-group">
                        <label>Amount: </label>
                        <input type="number" id="order_amount" value="0.5" step="0.1">
                    </div>
                    <button onclick="submitOrder()">Execute Order</button>
                    <h3>📋 Engine Response:</h3>
                    <pre id="response_log">No active orders sent yet.</pre>
                </div>
            </div>
            <script>
                let ws = new WebSocket("ws://" + window.location.host + "/ws/orderbook");
                ws.onopen = () => document.getElementById("status").innerText = "🟢 Connected to Matching Core.";
                ws.onmessage = (event) => {
                    const data = JSON.parse(event.data);
                    let asksHTML = "";
                    [...data.asks].reverse().forEach(a => asksHTML += `${a.price.toFixed(2)} -- ${a.amount.toFixed(4)}\\n`);
                    document.getElementById("asks_list").innerText = asksHTML;
                    let bidsHTML = "";
                    data.bids.forEach(b => bidsHTML += `${b.price.toFixed(2)} -- ${b.amount.toFixed(4)}\\n`);
                    document.getElementById("bids_list").innerText = bidsHTML;
                };
                async function submitOrder() {
                    const side = document.getElementById("order_side").value;
                    const price = parseFloat(document.getElementById("order_price").value);
                    const amount = parseFloat(document.getElementById("order_amount").value);
                    const res = await fetch("/api/order", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ side, price, amount })
                    });
                    const result = await res.json();
                    document.getElementById("response_log").innerText = JSON.stringify(result, null, 2);
                }
            </script>
        </body>
    </html>
    """
    return HTMLResponse(html_content)
