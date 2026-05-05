from fastapi import FastAPI, Request
import asyncio
import json
import websocket
import threading

app = FastAPI()

WS_URL = "wss://tradepro-backend-production-feec.up.railway.app"

ws = None

def connect_ws():
    global ws
    ws = websocket.WebSocket()
    ws.connect(WS_URL)
    print("Connected to WebSocket")

def send_to_ws(data):
    try:
        if ws:
            payload = [
                {
                    "symbol": data["symbol"],
                    "price": data["bid"]
                }
            ]
            ws.send(json.dumps(payload))
            print("Sent to WS:", payload)
    except Exception as e:
        print("WS Error:", e)

@app.post("/price")
async def receive_price(request: Request):
    data = await request.json()
    send_to_ws(data)
    return {"status": "ok"}

def start_ws():
    while True:
        try:
            connect_ws()
            break
        except:
            print("Retrying WebSocket...")
            asyncio.sleep(2)

threading.Thread(target=start_ws).start()