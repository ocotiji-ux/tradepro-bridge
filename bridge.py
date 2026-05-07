from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
import json

app = FastAPI()

clients = []

# Frontend connects here
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    print("Frontend connected")

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.remove(websocket)
        print("Frontend disconnected")


# MT5 sends prices here
@app.post("/price")
async def receive_price(request: Request):
    body = await request.body()
    text = body.decode(errors="ignore").strip()

    try:
        end = text.find("}") + 1
        clean = text[:end]

        data = json.loads(clean)

        print("Received:", data)

        alive_clients = []

        for ws in clients:
            try:
                await ws.send_json(data)
                alive_clients.append(ws)
            except:
                print("Dead websocket removed")

        clients[:] = alive_clients

    except Exception as e:
        print("ERROR:", e)
        print("RAW:", text)

    return {"status": "ok"}