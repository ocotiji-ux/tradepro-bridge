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

            await asyncio.sleep(30)

            try:
                await websocket.send_json({
                    "type": "heartbeat"
                })

            except:
                break

    except WebSocketDisconnect:

        print("Frontend disconnected")

    finally:

        clients.discard(websocket)


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

        # Normalize broker symbols
        symbol = data.get("symbol", "")

        if symbol == "GOLD":
            data["symbol"] = "XAUUSD"

        # ACCOUNT METRICS
        if data.get("type") == "account":

            for ws in clients:
                try:
                    await ws.send_json({
                        "type": "account",
                        "balance": data["balance"],
                        "equity": data["equity"],
                        "margin_level": data["margin_level"],
                        "free_margin": data["free_margin"],
                        "pnl": data["pnl"]
                    })

                except Exception as e:
                    print("Account WS failed:", e)
                    dead_clients.append(ws)

        # PRICE DATA
        else:

            for ws in clients:
                try:
                    await ws.send_json([{
                        "symbol": data["symbol"],
                        "price": data["bid"]
                    }])

                except Exception as e:
                    print("Price WS failed:", e)
                    dead_clients.append(ws)

        # REMOVE DEAD CLIENTS
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