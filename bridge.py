from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json

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
positions = []

pending_position_action = None

# =====================================================
# MODELS
# =====================================================

class TradeSignal(BaseModel):
    symbol: str
    action: str
    lot: float = 0.01
class PositionAction(BaseModel):
    action: str
    ticket: int
    sl: float | None = None
    tp: float | None = None

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

        text = body.decode(errors="ignore")
        text = text.replace("\x00", "").strip()

        end = text.find("}") + 1
        clean = text[:end]

        data = json.loads(clean)

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

        for ws in list(clients):
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
# POSITION ACTION FROM APP
# =====================================================

@app.post("/position-action")
async def position_action(action: PositionAction):

    global pending_position_action

    try:

        pending_position_action = action.dict()

        print(
            "POSITION ACTION RECEIVED:",
            pending_position_action
        )

        return {
            "status": "queued",
            "action": pending_position_action
        }

    except Exception as e:

        print("POSITION ACTION ERROR:", e)

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
        body = await request.body()

        text = body.decode(errors="ignore")
        text = text.replace("\x00", "").strip()

        print("RAW EXECUTION ACK:", text)

        data = json.loads(text)

        last_execution = data

        print("EXECUTION ACK:", data)

        return {
            "status": "received",
            "execution": data
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
# POSITION SYNC FROM MT5
# =====================================================

@app.post("/positions")
async def update_positions(request: Request):

    global positions

    try:
        body = await request.body()

        text = body.decode(errors="ignore")
        text = text.replace("\x00", "").strip()

        print("RAW POSITIONS:", text)

        end = text.rfind("]") + 1

        if end > 0:
            text = text[:end]

        positions = json.loads(text)

        print(f"POSITIONS UPDATED: {len(positions)} positions")

        return {
            "status": "received",
            "count": len(positions)
        }

    except Exception as e:
        print("POSITION ERROR:", e)

        return {
            "status": "error",
            "message": str(e)
        }

# =====================================================
# GET OPEN POSITIONS
# =====================================================

@app.get("/positions")
async def get_positions():

    global positions

    return positions

# =====================================================
# MT5 POLLS FOR NEXT TRADE
# =====================================================

@app.api_route("/next-trade", methods=["GET", "POST"])
async def next_trade():

    global latest_trade

    if latest_trade is None:
        return {}

    trade = latest_trade

    latest_trade = None

    print("TRADE DELIVERED TO MT5:", trade)

    return trade

# =====================================================
# MT5 POLLS FOR POSITION ACTIONS
# =====================================================

@app.api_route(
    "/next-position-action",
    methods=["GET", "POST"]
)
async def next_position_action():

    global pending_position_action

    if pending_position_action is None:
        return {}

    action = pending_position_action

    pending_position_action = None

    print(
        "POSITION ACTION DELIVERED:",
        action
    )

    return action