from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from core.broadcaster import manager
from core.redis_layer import redis_client

ws_router = APIRouter()

@ws_router.websocket("/ws/option-chain")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Active listener loop mapping dynamically for Frontend Seq mismatches natively
            msg = await websocket.receive_text()
            if msg == "REQUEST_SYNC" and redis_client:
                raw_full = await redis_client.get("nifty:stream:last_full_state")
                if raw_full:
                    await websocket.send_text(raw_full)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
