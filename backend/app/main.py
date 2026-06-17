"""ASGI app serving the read-only briefing GraphQL API plus static audio files."""

from __future__ import annotations

from strawberry.asgi import GraphQL
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount, Route

from .data_loader import AUDIO_DIR
from .schema import schema
from starlette.staticfiles import StaticFiles

AUDIO_DIR.mkdir(parents=True, exist_ok=True)

graphql_app = GraphQL(schema)

app = Starlette(
    middleware=[
        Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]),
    ],
    routes=[
        Route("/graphql", graphql_app, methods=["GET", "POST"]),
        Mount("/audio", app=StaticFiles(directory=AUDIO_DIR)),
    ],
)
