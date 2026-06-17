"""GraphQL schema: read-only briefing data, no mutations, no auth."""

from __future__ import annotations

import strawberry

from . import data_loader


@strawberry.type
class Article:
    title: str
    headline: str
    bullets: list[str]
    so_what: str
    source: str
    source_url: str | None
    rank: int
    tags: list[str]


@strawberry.type
class Briefing:
    date: str
    topic: str
    audio_url: str | None
    articles: list[Article]


@strawberry.type
class Query:
    @strawberry.field
    def briefings(self, topic: str | None = None, date: str | None = None) -> list[Briefing]:
        records = data_loader.get_briefings(topic=topic, date=date)
        return [
            Briefing(
                date=record["date"],
                topic=record["topic"],
                audio_url=record["audio_url"],
                articles=[Article(**article) for article in record["articles"]],
            )
            for record in records
        ]

    @strawberry.field
    def dates(self) -> list[str]:
        return data_loader.get_available_dates()


schema = strawberry.Schema(query=Query)
