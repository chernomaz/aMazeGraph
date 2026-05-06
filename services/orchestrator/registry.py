from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from redis import asyncio as aioredis
from redis.exceptions import RedisError

GRAPH_ID_PATTERN = r"^[a-z0-9_-]+$"
NODE_NAME_PATTERN = r"^[A-Za-z0-9_\-]+$"
ENDPOINT_PATTERN = r"^https?://[^\s]+$"

_GRAPH_ID_RE = re.compile(GRAPH_ID_PATTERN)
_ENDPOINT_RE = re.compile(ENDPOINT_PATTERN)

router = APIRouter()


class RegisterNodeRequest(BaseModel):
    graph_id: str = Field(min_length=1, max_length=128)
    node_name: str = Field(min_length=1, max_length=64, pattern=NODE_NAME_PATTERN)
    endpoint: str = Field(min_length=1, max_length=2048)
    cache_ttl: int | None = None

    @field_validator("graph_id")
    @classmethod
    def _validate_graph_id(cls, v: str) -> str:
        if not _GRAPH_ID_RE.match(v):
            raise ValueError("graph_id must match ^[a-z0-9_-]+$")
        return v

    @field_validator("endpoint")
    @classmethod
    def _validate_endpoint(cls, v: str) -> str:
        if not _ENDPOINT_RE.match(v):
            raise ValueError("endpoint must be an absolute http(s) URL")
        return v


class RegisterNodeResponse(BaseModel):
    status: Literal["ok"] = "ok"
    graph_id: str
    node_name: str


class DeleteNodeResponse(BaseModel):
    status: Literal["ok"] = "ok"
    graph_id: str
    node_name: str


class ResolveNodeResponse(BaseModel):
    graph_id: str
    node_name: str
    endpoint: str
    cache_ttl: int | None = None


def _node_key(graph_id: str, node_name: str) -> str:
    return f"graph_node:{graph_id}:{node_name}"


def _redis(request: Request) -> aioredis.Redis:
    return request.app.state.redis


@router.post("/register/node", response_model=RegisterNodeResponse)
async def register_node(req: RegisterNodeRequest, request: Request) -> RegisterNodeResponse:
    data: dict = {
        "endpoint": req.endpoint,
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }
    if req.cache_ttl is not None:
        data["cache_ttl"] = req.cache_ttl
    try:
        await _redis(request).set(_node_key(req.graph_id, req.node_name), json.dumps(data))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return RegisterNodeResponse(graph_id=req.graph_id, node_name=req.node_name)


@router.delete("/register/node", response_model=DeleteNodeResponse)
async def unregister_node(req: RegisterNodeRequest, request: Request) -> DeleteNodeResponse:
    try:
        await _redis(request).delete(_node_key(req.graph_id, req.node_name))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return DeleteNodeResponse(graph_id=req.graph_id, node_name=req.node_name)


@router.get("/resolve/node/{graph_id}/{node_name}", response_model=ResolveNodeResponse)
async def resolve_node(
    request: Request,
    graph_id: str = Path(pattern=GRAPH_ID_PATTERN, max_length=128),
    node_name: str = Path(pattern=NODE_NAME_PATTERN, max_length=64),
) -> ResolveNodeResponse:
    try:
        raw = await _redis(request).get(_node_key(graph_id, node_name))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    if raw is None:
        return JSONResponse(
            status_code=404,
            content={
                "detail": "node-not-registered",
                "graph_id": graph_id,
                "node_name": node_name,
            },
        )
    data = json.loads(raw)
    return ResolveNodeResponse(
        graph_id=graph_id,
        node_name=node_name,
        endpoint=data["endpoint"],
        cache_ttl=data.get("cache_ttl"),
    )
