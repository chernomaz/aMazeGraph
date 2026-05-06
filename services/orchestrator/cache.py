from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Path, Request
from pydantic import BaseModel, Field
from redis import asyncio as aioredis
from redis.exceptions import RedisError

CACHE_KEY_PATTERN = r"^[a-f0-9]{32,64}$"

router = APIRouter()


class CacheGetResponse(BaseModel):
    hit: bool
    body: dict[str, Any] | None = None


class CachePutRequest(BaseModel):
    body: dict[str, Any]
    ttl: int = Field(gt=0)


def _cache_key(key: str) -> str:
    return f"cache:{key}"


def _redis(request: Request) -> aioredis.Redis:
    return request.app.state.redis


@router.get("/cache/{key}")
async def get_cache(
    request: Request,
    key: str = Path(pattern=CACHE_KEY_PATTERN, max_length=64),
) -> CacheGetResponse:
    try:
        raw = await _redis(request).get(_cache_key(key))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    if raw is None:
        return CacheGetResponse(hit=False)
    return CacheGetResponse(hit=True, body=json.loads(raw))


@router.put("/cache/{key}", status_code=200)
async def put_cache(
    req: CachePutRequest,
    request: Request,
    key: str = Path(pattern=CACHE_KEY_PATTERN, max_length=64),
) -> dict[str, str]:
    try:
        await _redis(request).setex(_cache_key(key), req.ttl, json.dumps(req.body))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return {"status": "ok"}
