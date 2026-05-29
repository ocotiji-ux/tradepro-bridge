from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import json

# =====================================================
# FASTAPI APP
# =====================================================

app = FastAPI()

# =====================================================
# CORS
# =====================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================
# GLOBALS
# =====================================================

clients = set()

latest_trade = None
last_execution = None

# =====================================================
# MODELS
# =====================================================

class TradeSignal(BaseModel):
    symbol: str
    action: str
    lot: float = 0.01

# =====================================================
# WEBSOCKET
# =====================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    await websocket.accept()

    clients.add(websocket)

    print("Frontend connected")

    try:

        while True:

            # Wait for client messages to keep connection alive
            await websocket.receive_text()

    except WebSocketDisconnect:

        print("Frontend disconnected")

    except Exception as e:

        print("WS ERROR:", e)

    finally:

        clients.discard(websocket)

        print("Client removed")

# =====================================================
# MT5 TELEMETRY
# =====================================================

@app.post("/price")
async def receive_price(request: Request):

    dead_clients = []

    try:

        body = await request.body()

        text = body.decode(errors="ignore").strip()

        end = text.find("}") + 1

        clean = text[:end]

        data = json.loads(clean)

        print("Received:", data)

        symbol = data.get("symbol", "")

        if symbol == "GOLD":
            data["symbol"] = "XAUUSD"

        payload = {
            "type": "telemetry",
            "symbol": data.get("symbol"),
            "bid": data.get("bid"),
            "ask": data.get("ask"),
            "balance": data.get("balance"),
            "equity": data.get("equity"),
            "margin_level": data.get("margin_level"),
            "free_margin": data.get("free_margin"),
            "pnl": data.get("pnl")
        }

        for ws in clients:

            try:

                await ws.send_json(payload)

            except Exception as e:

                print("WS failed:", e)

                dead_clients.append(ws)

        for ws in dead_clients:

            clients.discard(ws)

    except Exception as e:

        print("PRICE ERROR:", e)

    return {"status": "ok"}

# =====================================================
# RECEIVE TRADE FROM APP
# =====================================================

@app.post("/trade")
async def receive_trade(signal: TradeSignal):

    global latest_trade

    try:

        latest_trade = signal.dict()

        print("TRADE RECEIVED:", latest_trade)

        return {
            "status": "received",
            "trade": latest_trade
        }

    except Exception as e:

        print("TRADE ERROR:", e)

        return {
            "status": "error",
            "message": str(e)
        }

# =====================================================
# EXECUTION ACKNOWLEDGEMENT FROM MT5
# =====================================================

@app.post("/execution-complete")
async def execution_complete(request: Request):

    global last_execution

    try:

        data = await request.json()

        last_execution = data

        print("EXECUTION ACK:", data)

        print("LATEST EXECUTION UPDATED")

        return {
            "status": "received"
        }

    except Exception as e:

        print("ACK ERROR:", e)

        return {
            "status": "error",
            "message": str(e)
        }

# =====================================================
# EXECUTION STATUS
# =====================================================

@app.get("/execution-status")
async def execution_status():

    global last_execution

    return last_execution or {}

# =====================================================
# MT5 POLLS FOR NEXT TRADE
# =====================================================

@app.api_route("/next-trade", methods=["GET", "POST"])
async def next_trade():

    global latest_trade

    if latest_trade is None:

        print("NO PENDING TRADE")

        return {}

    trade = latest_trade

    latest_trade = None

    print("TRADE DELIVERED TO MT5:", trade)

    return trade