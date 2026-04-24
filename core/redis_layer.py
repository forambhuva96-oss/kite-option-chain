import os
import json
import redis.asyncio as redis
import logging

logger = logging.getLogger("app")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
except Exception as e:
    logger.error(f"Failed to initialize Redis connection natively: {e}")
    redis_client = None

async def publish_delta(channel: str, delta_payload: dict, full_state_payload: dict):
    if not redis_client:
        return
    try:
        # 1. Distribute sequential delta stream
        await redis_client.publish(channel, json.dumps(delta_payload))
        
        # 2. Lock absolute full-state context for late-joining connections universally
        await redis_client.set(f"{channel}:last_full_state", json.dumps(full_state_payload))
    except Exception as e:
        logger.error(f"Redis Broadcast Failure: {e}")

async def get_full_state(channel: str):
    if not redis_client:
        return None
    try:
        raw = await redis_client.get(f"{channel}:last_full_state")
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.error(f"Redis Baseline Fetch Failure: {e}")
        return None
