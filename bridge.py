from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import json
import asyncio

app = FastAPI()

clients = set()

latest_trade = None


# ===============================
# TRADE SIGNAL MODEL
# ===============================
class TradeSignal(BaseModel):
    symbol: str
    action: str


# ===============================
# WEBSOCKET ENDPOINT
# ===============================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    await websocket.accept()

    clients.add(websocket)

    print("Frontend connected")

    try:

        while True:

            # keep connection alive
            await websocket.receive_text()

    except WebSocketDisconnect:

        print("Frontend disconnected")

    except Exception as e:

        print("WS ERROR:", e)

    finally:

        clients.discard(websocket)

        print("Client removed")

# ===============================
# RECEIVE MT5 PRICE DATA
# ===============================
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

        # ==========================================
        # SEND PRICE DATA
        # ==========================================

        for ws in clients:

            try:

                await ws.send_json({
                    "type": "telemetry",
                    "symbol": data.get("symbol"),
                    "bid": data.get("bid"),
                    "ask": data.get("ask"),
                    "balance": data.get("balance"),
                    "equity": data.get("equity"),
                    "margin_level": data.get("margin_level"),
                    "free_margin": data.get("free_margin"),
                    "pnl": data.get("pnl")
                })

            except Exception as e:

                print("WS failed:", e)

                dead_clients.append(ws)

        for ws in dead_clients:

            clients.discard(ws)

    except Exception as e:

        print("ERROR:", e)

    return {"status": "ok"}

# ===============================
# RECEIVE REMOTE TRADE SIGNAL
# ===============================
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
            "status": "error"
        }


# ===============================
# MT5 POLLS FOR NEXT TRADE
# ===============================
@app.api_route("/next-trade", methods=["GET", "POST"])
async def next_trade():

    global latest_trade

    if latest_trade is None:
        return {}

    trade = latest_trade

    latest_trade = None

    return trade