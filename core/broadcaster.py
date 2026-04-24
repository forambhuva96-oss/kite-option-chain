from fastapi import WebSocket, WebSocketDisconnect
from typing import List
import logging
import asyncio
import json

from core.redis_layer import redis_client

logger = logging.getLogger("app")

class StatelessConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.redis_task = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        
        # 1. Fetch exact current baseline from Redis Memory Bank 
        # (This bypasses needing a local state map!)
        if redis_client:
            try:
                raw_full = await redis_client.get("nifty:stream:last_full_state")
                if raw_full:
                    await websocket.send_text(raw_full)
            except Exception as e:
                logger.error(f"Error hydrating client via Redis: {e}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def start_redis_listener(self):
        """
        The Isolated Consumer Daemon: Listens eternally to Redis and blind-fires out sockets natively.
        Runs exactly once per FastAPI container.
        """
        if not redis_client:
            logger.warning("No Redis client found natively. Broadcasting will fail offline.")
            return
            
        pubsub = redis_client.pubsub()
        await pubsub.subscribe("nifty:stream")
        logger.info("🟢 Redis Pub/Sub Worker Successfully Subscribed to 'nifty:stream'")
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    payload_str = message["data"]
                    
                    # 1. Extreme O(1) Speed: We do not json.loads(). We blind-copy the raw string natively!
                    for connection in list(self.active_connections):
                        try:
                            await connection.send_text(payload_str)
                        except Exception:
                            self.disconnect(connection)
        except asyncio.CancelledError:
            await pubsub.unsubscribe("nifty:stream")

manager = StatelessConnectionManager()
