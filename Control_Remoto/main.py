"""
Relay Server
------------
Este es el servidor que vive en Railway (24/7, con URL pública HTTPS).
No controla nada directamente: solo reenvía mensajes entre la laptop
(host_client.py, corriendo en tu máquina) y el iPhone (que abre la
página /control/<codigo> en Safari).

Flujo:
  1. host_client.py se conecta a /ws/host  -> el relay le genera un
     código de sala único (ej. "AXQ29P").
  2. host_client.py abre el navegador de la laptop en /host/<codigo>,
     página que muestra el QR apuntando a /control/<codigo>.
  3. El iPhone escanea el QR, abre /control/<codigo>, esa página se
     conecta a /ws/control/<codigo>.
  4. Todo lo que el iPhone manda por ese websocket, el relay lo
     reenvía tal cual al websocket de host_client.py, que lo ejecuta
     con pyautogui.
"""

import base64
import io
import json
import secrets
import string
from datetime import datetime, timedelta, timezone

import qrcode
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

ROOM_CODE_CHARS = string.ascii_uppercase + string.digits
ROOM_CODE_LEN = 6
ROOM_TTL_MINUTES = 60  # una sala sin nadie conectado se limpia sola


class Room:
    def __init__(self, host_ws: WebSocket):
        self.host_ws = host_ws
        self.control_ws: WebSocket | None = None
        self.created_at = datetime.now(timezone.utc)


rooms: dict[str, Room] = {}


def generate_room_code() -> str:
    while True:
        code = "".join(secrets.choice(ROOM_CODE_CHARS) for _ in range(ROOM_CODE_LEN))
        if code not in rooms:
            return code


def cleanup_expired_rooms():
    now = datetime.now(timezone.utc)
    expired = [
        code for code, room in rooms.items()
        if now - room.created_at > timedelta(minutes=ROOM_TTL_MINUTES)
    ]
    for code in expired:
        rooms.pop(code, None)


def make_qr_data_uri(url: str) -> str:
    qr = qrcode.QRCode(border=1, box_size=8)
    qr.add_data(url)
    qr.make()
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()
    return f"data:image/png;base64,{b64}"


# ---------------------------------------------------------------------------
# Páginas web
# ---------------------------------------------------------------------------

@app.get("/host/{room_code}", response_class=HTMLResponse)
async def host_page(request: Request, room_code: str):
    cleanup_expired_rooms()
    base = str(request.base_url)
    # Railway sirve HTTPS de por sí; si pruebas local se queda en http.
    scheme = "https" if "railway.app" in base else "http"
    host_no_scheme = base.split("://", 1)[-1].rstrip("/")
    control_url = f"{scheme}://{host_no_scheme}/control/{room_code}"

    qr_data_uri = make_qr_data_uri(control_url)
    return templates.TemplateResponse(request, "host.html", {
        "room_code": room_code,
        "control_url": control_url,
        "qr_data_uri": qr_data_uri,
    })


@app.get("/control/{room_code}", response_class=HTMLResponse)
async def control_page(request: Request, room_code: str):
    return templates.TemplateResponse(request, "control.html", {
        "room_code": room_code,
    })


@app.get("/api/status/{room_code}")
async def room_status(room_code: str):
    room = rooms.get(room_code)
    if room is None:
        return {"exists": False}
    return {"exists": True, "connected": room.control_ws is not None}


# ---------------------------------------------------------------------------
# WebSockets
# ---------------------------------------------------------------------------

@app.websocket("/ws/host")
async def ws_host(websocket: WebSocket):
    cleanup_expired_rooms()
    await websocket.accept()
    code = generate_room_code()
    rooms[code] = Room(websocket)
    await websocket.send_text(json.dumps({"type": "room_created", "room": code}))
    print(f"🖥️  Nueva laptop conectada, sala {code}")

    try:
        while True:
            # El host normalmente no manda nada, pero drenamos por si
            # acaso manda pings/keepalive desde el cliente.
            await websocket.receive_text()
    except WebSocketDisconnect:
        rooms.pop(code, None)
        print(f"🖥️  Laptop desconectada, sala {code} cerrada")


@app.websocket("/ws/control/{room_code}")
async def ws_control(websocket: WebSocket, room_code: str):
    room = rooms.get(room_code)

    if room is None:
        await websocket.accept()
        await websocket.send_text(json.dumps({
            "type": "error", "message": "Sala no encontrada o expirada. Vuelve a abrir el programa en tu laptop."
        }))
        await websocket.close()
        return

    if room.control_ws is not None:
        await websocket.accept()
        await websocket.send_text(json.dumps({
            "type": "error", "message": "Ya hay un teléfono controlando esta laptop ahorita."
        }))
        await websocket.close()
        return

    await websocket.accept()
    room.control_ws = websocket
    await websocket.send_text(json.dumps({"type": "connected"}))
    try:
        await room.host_ws.send_text(json.dumps({"type": "control_connected"}))
    except Exception:
        pass
    print(f"📱 Control conectado a sala {room_code}")

    try:
        while True:
            msg = await websocket.receive_text()
            # Reenviamos el mensaje tal cual al host, que es quien
            # realmente ejecuta la acción con pyautogui.
            try:
                await room.host_ws.send_text(msg)
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        if room_code in rooms and rooms[room_code].control_ws is websocket:
            rooms[room_code].control_ws = None
            try:
                await room.host_ws.send_text(json.dumps({"type": "control_disconnected"}))
            except Exception:
                pass
        print(f"📱 Control desconectado de sala {room_code}")
