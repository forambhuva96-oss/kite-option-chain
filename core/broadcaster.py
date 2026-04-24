from fastapi import WebSocket, WebSocketDisconnect
from typing import List, Optional
import logging
import asyncio
import json

logger = logging.getLogger("app")

class ConnectionManager:
    """
    Hybrid broadcaster:
    - If REDIS_URL is set and reachable: uses Redis Pub/Sub (scalable, multi-node)
    - If Redis is unavailable: falls back to direct in-process WebSocket broadcasting
    This ensures the server starts cleanly on environments without Redis (e.g. Render free tier).
    """
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.last_state: dict = {}  # In-process fallback state

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Hydrate new client with last known full state immediately
        if self.last_state:
            try:
                payload = {
                    "type": "FULL",
                    "seq_id": self.last_state.get("seq_id", 0),
                    "timestamp": self.last_state.get("last_updated", ""),
                    "spot_price": self.last_state.get("spot_price", 0),
                    "atm_strike": self.last_state.get("atm_strike", 0),
                    "expiry": self.last_state.get("expiry", ""),
                    "chain": self.last_state.get("latest_data", [])
                }
                await websocket.send_json(payload)
            except Exception as e:
                logger.error(f"Error hydrating new client: {e}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    def compute_delta(self, new_state: dict) -> Optional[dict]:
        if not self.last_state:
            return None
        if new_state.get("last_updated") == self.last_state.get("last_updated"):
            return None  # No change

        delta = {
            "type": "DELTA",
            "seq_id": new_state.get("seq_id", 0),
            "timestamp": new_state.get("last_updated"),
            "spot_price": new_state.get("spot_price"),
            "atm_strike": new_state.get("atm_strike"),
            "expiry": new_state.get("expiry"),
            "chain_updates": {}
        }

        old_chain = {str(r["strike"]): r for r in self.last_state.get("latest_data", [])}
        updates = False
        for row in new_state.get("latest_data", []):
            strike = str(row["strike"])
            old = old_chain.get(strike)
            if not old:
                delta["chain_updates"][strike] = row
                updates = True
                continue
            strike_diff = {}
            for side in ["CE", "PE"]:
                if row.get(side) and old.get(side):
                    diff = {k: v for k, v in row[side].items() if old[side].get(k) != v}
                    if diff:
                        strike_diff[side] = diff
                elif row.get(side):
                    strike_diff[side] = row[side]
            if strike_diff:
                delta["chain_updates"][strike] = strike_diff
                updates = True

        return delta if updates else None

    async def broadcast(self, new_state: dict):
        """Direct in-process broadcast — used when Redis is not available."""
        delta = self.compute_delta(new_state)
        self.last_state = dict(new_state)
        if not delta:
            return
        for connection in list(self.active_connections):
            try:
                await connection.send_json(delta)
            except Exception:
                self.disconnect(connection)

    async def start_redis_listener(self):
        """Attempt Redis Pub/Sub subscription. Silently skips if Redis is unavailable."""
        try:
            from core.redis_layer import redis_client
            if not redis_client:
                logger.info("Redis not configured — using in-process broadcast mode.")
                return
            # Quick connectivity test
            await redis_client.ping()
        except Exception:
            logger.info("Redis unreachable — using in-process broadcast mode.")
            return

        from core.redis_layer import redis_client
        pubsub = redis_client.pubsub()
        await pubsub.subscribe("nifty:stream")
        logger.info("🟢 Redis Pub/Sub Worker Subscribed to 'nifty:stream'")
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    payload_str = message["data"]
                    for connection in list(self.active_connections):
                        try:
                            await connection.send_text(payload_str)
                        except Exception:
                            self.disconnect(connection)
        except asyncio.CancelledError:
            await pubsub.unsubscribe("nifty:stream")

manager = ConnectionManager()
