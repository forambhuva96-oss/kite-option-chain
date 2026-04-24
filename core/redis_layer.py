import os
import json
import logging

logger = logging.getLogger("app")

redis_client = None

try:
    import redis.asyncio as _redis
    REDIS_URL = os.getenv("REDIS_URL", "")
    if REDIS_URL:
        redis_client = _redis.from_url(REDIS_URL, decode_responses=True)
        logger.info(f"Redis client initialised at {REDIS_URL}")
    else:
        logger.info("REDIS_URL not set — Redis disabled, using in-process broadcast.")
except ImportError:
    logger.info("redis package not installed — Redis disabled, using in-process broadcast.")
except Exception as e:
    logger.error(f"Redis init error: {e} — using in-process broadcast.")


async def publish_delta(channel: str, delta_payload: dict, full_state_payload: dict):
    if not redis_client:
        return
    try:
        await redis_client.publish(channel, json.dumps(delta_payload))
        await redis_client.set(f"{channel}:last_full_state", json.dumps(full_state_payload))
    except Exception as e:
        logger.error(f"Redis publish error: {e}")


async def get_full_state(channel: str):
    if not redis_client:
        return None
    try:
        raw = await redis_client.get(f"{channel}:last_full_state")
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.error(f"Redis get error: {e}")
        return None
