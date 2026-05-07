from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
import json
import asyncio

app = FastAPI()

clients = set()

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

    try:
        body = await request.body()
        text = body.decode(errors="ignore").strip()

        end = text.find("}") + 1
        clean = text[:end]

        data = json.loads(clean)

        print("Received:", data)

        dead_clients = []

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