from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
import json
import asyncio

app = FastAPI()

clients = set()

latest_trade = None

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

        # Normalize broker symbols safely
        symbol = data.get("symbol", "")

        if symbol == "GOLD":
            data["symbol"] = "XAUUSD"

        for ws in clients:
            try:
                await ws.send_json([{
                    "symbol": data["symbol"],
                    "price": data["bid"]
                }])
            except Exception as e:
                print("WS send failed:", e)
                dead_clients.append(ws)

        for ws in dead_clients:
            clients.discard(ws)

    except Exception as e:
        print("ERROR:", e)

    return {"status": "ok"}

@app.post("/trade")
async def receive_trade(request: Request):

    global latest_trade

    try:
        data = await request.json()

        latest_trade = data

        print("TRADE RECEIVED:", data)

        return {
            "status": "received",
            "trade": data
        }

    except Exception as e:
        print("TRADE ERROR:", e)

        return {
            "status": "error"
        }

@app.get("/next-trade")
async def next_trade():

    global latest_trade

    if latest_trade is None:
        return {}

    trade = latest_trade

    latest_trade = None

    return trade