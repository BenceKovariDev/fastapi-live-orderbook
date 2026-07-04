import asyncio
import json
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import redis.asyncio as aioredis

app = FastAPI(
    title="Enterprise HFT Order Book Streamer v3.0",
    description="Step 6: High-Frequency Crypto Exchange Simulation using FastAPI WebSockets with Fan-Out Architecture & Network Heartbeats."
)

REDIS_URL = "redis://127.0.0.1:6379"
CHANNEL_NAME = "live_orderbook"

# --- 1. ENTERPRISE CONNECTION MANAGER (FAN-OUT & HEARTBEAT) ---
class ConnectionManager:
    def __init__(self):
        # Az összes aktív WebSocket kapcsolatot egy halmazban tároljuk
        self.active_connections: set[WebSocket] = set()
        self.redis_task: asyncio.Task | None = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        print(f"🔌 Client connected. Total active connections: {len(self.active_connections)}")
        
        # Első kliensnél elindítjuk az EGYETLEN háttér Redis feliratkozást (Fan-Out)
        if len(self.active_connections) == 1:
            self.redis_task = asyncio.create_task(self._start_redis_listener())

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"❌ Client disconnected. Total active connections: {len(self.active_connections)}")
        
        # Ha nincs több kliens, leállítjuk a Redis figyelőt, hogy spóroljunk az erőforrással
        if len(self.active_connections) == 0 and self.redis_task:
            self.redis_task.cancel()
            self.redis_task = None

    async def broadcast(self, message: str):
        """Minden aktív kliensnek kiküldjük az adatot (Fan-Out elosztás)"""
        if not self.active_connections:
            return
        
        # Biztonságos, konkurens kiküldés az összes csatlakozott WebSocketre
        tasks = [websocket.send_text(message) for websocket in self.active_connections]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _start_redis_listener(self):
        """EGYETLEN központi szál, ami a Redist olvassa"""
        redis_client = aioredis.from_url(REDIS_URL)
        pubsub = redis_client.pubsub()
        print("🧠 Centralized Redis Fan-Out listener initialized.")
        try:
            await pubsub.subscribe(CHANNEL_NAME)
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = message["data"].decode("utf-8")
                    # Szétosztjuk a FastAPI memóriájából az összes kliensnek
                    await self.broadcast(data)
        except asyncio.CancelledError:
            print("🛑 Centralized Redis listener stopped (No active clients).")
        finally:
            await pubsub.unsubscribe(CHANNEL_NAME)
            await redis_client.close()

manager = ConnectionManager()

# --- 2. PIACI ADAT GENERÁTOR (PUBLISHER) ---
async def simulated_market_data_provider():
    redis_client = aioredis.from_url(REDIS_URL)
    base_price = 92500.0
    print("🚀 High-Frequency Market Data Provider active.")
    try:
        while True:
            base_price += round(random.uniform(-5.0, 5.0), 2)
            bids = [{"price": round(base_price - (i * 0.5), 2), "amount": round(random.uniform(0.01, 2.5), 4)} for i in range(1, 6)]
            asks = [{"price": round(base_price + (i * 0.5), 2), "amount": round(random.uniform(0.01, 2.5), 4)} for i in range(1, 6)]
            
            orderbook_payload = {
                "symbol": "BTCUSDT",
                "timestamp": round(asyncio.get_event_loop().time() * 1000),
                "bids": bids,
                "asks": asks
            }
            await redis_client.publish(CHANNEL_NAME, json.dumps(orderbook_payload))
            await asyncio.sleep(0.2)  # 200 ms streaming frekvencia
    except asyncio.CancelledError:
        pass
    finally:
        await redis_client.close()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(simulated_market_data_provider())

# --- 3. WEBSOCKET ENDPOINT NETWORK HEARTBEAT-TEL ---
@app.websocket("/ws/orderbook")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # NETWORK HEARTBEAT (Ping/Pong): 
            # Folyamatosan figyeljük, hogy a kliens életben van-e (nem szakadt-e meg a hálózat)
            data = await websocket.receive_text()
            # Ha a kliens küld valamit (pl. egy egyedi feliratkozást), itt lehet kezelni
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        print(f"⚠️ Connection anomaly detected: {e}")
        await manager.disconnect(websocket)

# --- 4. FRONTEND KLIENS AUTOMATIKUS PING/PONG-GAL ---
@app.get("/")
async def get():
    html_content = """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Live OrderBook Stream</title>
            <style>
                body { font-family: monospace; background: #121214; color: #e1e1e6; padding: 20px; }
                .container { display: flex; gap: 40px; }
                .asks { color: #ea3943; }
                .bids { color: #16c784; }
                h2 { border-bottom: 1px solid #29292e; padding-bottom: 10px; }
            </style>
        </head>
        <body>
            <h1>⚡ Live BTC/USDT Order Book (Fan-Out & Heartbeat)</h1>
            <div id="status">Connecting...</div>
            <div class="container">
                <div>
                    <h2 class="asks">🔴 ASKS (Eladók)</h2>
                    <pre id="asks_list"></pre>
                </div>
                <div>
                    <h2 class="bids">🟢 BIDS (Vevők)</h2>
                    <pre id="bids_list"></pre>
                </div>
            </div>
            <script>
                let ws;
                function connect() {
                    ws = new WebSocket("ws://" + window.location.host + "/ws/orderbook");
                    
                    ws.onopen = () => {
                        document.getElementById("status").innerText = "🟢 Connected to Cluster Fan-Out. Streaming data...";
                        // Elindítjuk a kliens oldali szívverést (Keep-Alive Ping)
                        setInterval(() => { if(ws.readyState === WebSocket.OPEN) ws.send("ping"); }, 10000);
                    };
                    
                    ws.onmessage = (event) => {
                        const data = JSON.parse(event.data);
                        let asksHTML = "";
                        data.asks.reverse().forEach(a => asksHTML += `${a.price} -- ${a.amount}\\n`);
                        document.getElementById("asks_list").innerText = asksHTML;
                        
                        let bidsHTML = "";
                        data.bids.forEach(b => bidsHTML += `${b.price} -- ${b.amount}\\n`);
                        document.getElementById("bids_list").innerText = bidsHTML;
                    };
                    
                    ws.onclose = () => {
                        document.getElementById("status").innerText = "🔴 Disconnected. Reconnecting in 3s...";
                        // GRACEFUL RECONNECT: Automatikus újracsatlakozás ha megszakad a hálózat
                        setTimeout(connect, 3000);
                    };
                }
                connect();
            </script>
        </body>
    </html>
    """
    return HTMLResponse(html_content)
