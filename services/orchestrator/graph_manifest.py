from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Path, Request
from pydantic import BaseModel, Field, field_validator
from redis import asyncio as aioredis
from redis.exceptions import RedisError

GRAPH_ID_PATTERN = r"^[a-z0-9_-]+$"
_GRAPH_ID_RE = re.compile(GRAPH_ID_PATTERN)

router = APIRouter()


class RegisterGraphRequest(BaseModel):
    graph_id: str = Field(min_length=1, max_length=128)
    nodes: list[str]
    edges: list[tuple[str, str]]

    @field_validator("graph_id")
    @classmethod
    def _validate_graph_id(cls, v: str) -> str:
        if not _GRAPH_ID_RE.match(v):
            raise ValueError("graph_id must match ^[a-z0-9_-]+$")
        return v


class RegisterGraphResponse(BaseModel):
    status: Literal["ok"] = "ok"
    graph_id: str


class GraphManifestResponse(BaseModel):
    graph_id: str
    nodes: list[str]
    edges: list[tuple[str, str]]
    registered_at: str


def _graph_key(graph_id: str) -> str:
    return f"graph:{graph_id}"


def _redis(request: Request) -> aioredis.Redis:
    return request.app.state.redis


@router.post("/register/graph", response_model=RegisterGraphResponse)
async def register_graph(req: RegisterGraphRequest, request: Request) -> RegisterGraphResponse:
    payload = json.dumps(
        {
            "nodes": req.nodes,
            "edges": [list(edge) for edge in req.edges],
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    try:
        await _redis(request).set(_graph_key(req.graph_id), payload)
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return RegisterGraphResponse(graph_id=req.graph_id)


@router.get("/graphs/{graph_id}", response_model=GraphManifestResponse)
async def get_graph(
    request: Request,
    graph_id: str = Path(pattern=GRAPH_ID_PATTERN, max_length=128),
) -> GraphManifestResponse:
    try:
        raw = await _redis(request).get(_graph_key(graph_id))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail={"detail": "graph-not-registered", "graph_id": graph_id},
        )
    data = json.loads(raw)
    return GraphManifestResponse(
        graph_id=graph_id,
        nodes=data["nodes"],
        edges=[tuple(edge) for edge in data["edges"]],
        registered_at=data["registered_at"],
    )
