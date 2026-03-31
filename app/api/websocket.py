"""WebSocket manager + Redis pub/sub relay — per spec section 5."""

import json

from fastapi import WebSocket, WebSocketDisconnect, Query
import redis.asyncio as aioredis

from ..config import settings
from ..auth.security import decode_token


class ConnectionManager:
    def __init__(self):
        self.active: dict[str, list[WebSocket]] = {}

    async def connect(self, case_id: str, ws: WebSocket):
        await ws.accept()
        self.active.setdefault(case_id, []).append(ws)

    def disconnect(self, case_id: str, ws: WebSocket):
        if case_id in self.active:
            try:
                self.active[case_id].remove(ws)
            except ValueError:
                pass

    async def broadcast(self, case_id: str, message: dict):
        for ws in self.active.get(case_id, []):
            try:
                await ws.send_json(message)
            except Exception:
                pass


manager = ConnectionManager()


async def ws_pipeline_status(websocket: WebSocket, case_id: str):
    """WebSocket endpoint: subscribe to pipeline status for a case.

    Requires ?token=<JWT> query param for authentication.
    """
    # Authenticate via query param
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=4001, reason="Missing authentication token")
        return

    try:
        decode_token(token)
    except Exception:
        await websocket.close(code=4001, reason="Invalid or expired token")
        return

    await manager.connect(case_id, websocket)

    r = aioredis.from_url(settings.REDIS_URL)
    pubsub = r.pubsub()
    await pubsub.subscribe(f"pipeline:{case_id}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = json.loads(message["data"])
                await websocket.send_json(data)

                # Close after completion or error
                if data.get("type") in ("complete", "error"):
                    break
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(case_id, websocket)
        await pubsub.unsubscribe(f"pipeline:{case_id}")
        await r.aclose()


async def ws_sdss_status(websocket: WebSocket, task_id: str):
    """WebSocket endpoint: subscribe to SDSS task status updates.

    Requires ?token=<JWT> query param for authentication.
    The webhook endpoint publishes to Redis channel sdss:{task_id}
    when the GPU pod calls back with results.
    """
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=4001, reason="Missing authentication token")
        return

    try:
        decode_token(token)
    except Exception:
        await websocket.close(code=4001, reason="Invalid or expired token")
        return

    await websocket.accept()

    r = aioredis.from_url(settings.REDIS_URL)
    pubsub = r.pubsub()
    await pubsub.subscribe(f"sdss:{task_id}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = json.loads(message["data"])
                await websocket.send_json(data)

                if data.get("type") in ("complete", "error"):
                    break
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(f"sdss:{task_id}")
        await r.aclose()
