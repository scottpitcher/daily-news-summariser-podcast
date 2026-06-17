"""Read-only access to the existing pipeline's output files.

Loads briefing data straight from the JSON/Markdown/audio files that the
pipeline (src/run_pipeline.py) already produces. Nothing here writes to
those directories or invokes the pipeline.

Note: src/config.py declares OUTPUT_DIRS pointing at data/tagged_articles,
data/ranked_articles, output/reports, output/audio -- but src/run_pipeline.py
does not actually use those constants. Each stage module carries its own
DEFAULT_INPUT_DIR/DEFAULT_OUTPUT_DIR, and those are defined relative to
config.BASE_DIR, which resolves to the src/ directory itself (Path(__file__)
.resolve().parent of config.py) -- not the repo root. So
summarize_articles.DEFAULT_OUTPUT_DIR is actually src/data/processed/
article_summaries, not data/processed/article_summaries. build_briefing.py
and generate_tts.py separately define PROJECT_ROOT = BASE_DIR.parent, which
*is* the repo root, so their report/audio output dirs (outputs/reports,
outputs/audio) are at the repo root as expected. The paths below match what
run_pipeline.py actually writes on disk, not config.py's (unused) OUTPUT_DIRS.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import ISSUE_AREAS  # noqa: E402

ARTICLE_SUMMARIES_DIR = PROJECT_ROOT / "src" / "data" / "processed" / "article_summaries"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
AUDIO_DIR = PROJECT_ROOT / "outputs" / "audio"

SUMMARIES_FILENAME_RE = re.compile(r"^article_summaries_(\d{4}-\d{2}-\d{2})\.json$")
AUDIO_EXTENSIONS = ("mp3", "wav", "m4a")

NO_CONNECTION_PHRASE = "no direct committee or district connection identified"


def _available_dates() -> list[str]:
    """Return dates (YYYY-MM-DD) that have an article-summaries file, newest first."""
    dates = []
    for path in ARTICLE_SUMMARIES_DIR.glob("article_summaries_*.json"):
        match = SUMMARIES_FILENAME_RE.match(path.name)
        if match:
            dates.append(match.group(1))
    return sorted(dates, reverse=True)


def _load_summaries_payload(date: str) -> dict[str, Any] | None:
    path = ARTICLE_SUMMARIES_DIR / f"article_summaries_{date}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _has_connection(summary: dict[str, Any]) -> bool:
    so_what = str(summary.get("so_what") or "").strip().lower()
    return NO_CONNECTION_PHRASE not in so_what


def _audio_url(date: str) -> str | None:
    for ext in AUDIO_EXTENSIONS:
        if (AUDIO_DIR / f"daily_briefing_{date}.{ext}").exists():
            return f"/audio/daily_briefing_{date}.{ext}"
    return None


def _to_article(rank: int, summary: dict[str, Any]) -> dict[str, Any]:
    citation = summary.get("source_citation") or {}
    return {
        "title": str(citation.get("article_title") or ""),
        "headline": str(summary.get("headline") or ""),
        "bullets": list(summary.get("bullets") or []),
        "so_what": str(summary.get("so_what") or ""),
        "source": str(citation.get("source_name") or ""),
        "source_url": citation.get("article_url"),
        "rank": rank,
        "tags": [str(summary.get("issue_area") or "")],
    }


def get_briefings(topic: str | None = None, date: str | None = None) -> list[dict[str, Any]]:
    """Return briefings, one per (date, issue area) that had stories.

    Filters by exact date match and by topic (matched against the issue-area
    key or its display label, case-insensitively) when provided.
    """
    dates = [date] if date else _available_dates()
    normalized_topic = topic.strip().lower() if topic else None

    briefings: list[dict[str, Any]] = []
    for current_date in dates:
        payload = _load_summaries_payload(current_date)
        if payload is None:
            continue

        grouped: dict[str, list[dict[str, Any]]] = {}
        for summary in payload.get("summaries", []):
            issue_key = str(summary.get("issue_area") or "").strip()
            if not issue_key or not _has_connection(summary):
                continue
            grouped.setdefault(issue_key, []).append(summary)

        for issue_key, summaries in grouped.items():
            issue_config = ISSUE_AREAS.get(issue_key, {})
            label = str(issue_config.get("label") or issue_key.replace("_", " ").title())

            if normalized_topic and normalized_topic not in (issue_key.lower(), label.lower()):
                continue

            briefings.append(
                {
                    "date": current_date,
                    "topic": issue_key,
                    "audio_url": _audio_url(current_date),
                    "articles": [
                        _to_article(rank, summary) for rank, summary in enumerate(summaries, start=1)
                    ],
                }
            )

    return briefings
