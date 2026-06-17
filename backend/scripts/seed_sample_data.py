"""Write sample pipeline output so the dashboard has something to show.

The real pipeline (src/run_pipeline.py) writes article summaries to
data/processed/article_summaries/article_summaries_{date}.json, reports to
outputs/reports/, and audio to outputs/audio/. This script fills in demo data
in the same shapes and locations, so it never touches the pipeline itself.
"""

from __future__ import annotations

import json
import struct
import sys
import wave
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from config import ISSUE_AREAS  # noqa: E402

ARTICLE_SUMMARIES_DIR = PROJECT_ROOT / "data" / "processed" / "article_summaries"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
AUDIO_DIR = PROJECT_ROOT / "outputs" / "audio"

SAMPLE_DAYS = [date(2026, 6, 16), date(2026, 6, 17)]

SAMPLE_STORIES = {
    "politics_government": [
        {
            "headline": "Council advances District 4 rezoning bill",
            "bullets": [
                "The City Council voted to move the rezoning proposal out of committee.",
                "The plan would add roughly 600 units of mixed-income housing near the East River.",
            ],
            "so_what": "Affects zoning decisions CM Maloney's office will be asked to weigh in on directly.",
            "source_name": "NYC Council News",
            "article_title": "City Council advances rezoning bill in District 4",
            "article_url": "https://example.com/politics_government/1",
        },
        {
            "headline": "State budget talks stall over housing fund",
            "bullets": [
                "Albany negotiators disagree on the size of a statewide affordable-housing fund.",
            ],
            "so_what": "A smaller fund could reduce state matching dollars available to District 4 housing projects.",
            "source_name": "Albany Times",
            "article_title": "State budget talks stall over housing fund",
            "article_url": "https://example.com/politics_government/2",
        },
    ],
    "economy_business": [
        {
            "headline": "Small business permits rebound in Queens corridor",
            "bullets": [
                "New storefront permit filings rose 18% year-over-year along Northern Blvd.",
            ],
            "so_what": "Signals recovery in a commercial corridor the office has prioritized for small-business grants.",
            "source_name": "Crain's NY",
            "article_title": "Small business permits rebound in Queens corridor",
            "article_url": "https://example.com/economy_business/1",
        },
    ],
    "transportation_housing": [
        {
            "headline": "MTA proposes new bus lane on Northern Blvd",
            "bullets": [
                "The proposed lane would run during weekday rush hours only.",
                "Local merchants have raised concerns about loading-zone access.",
            ],
            "so_what": "Directly affects a corridor within the district; office may want to weigh in during the comment period.",
            "source_name": "Streetsblog",
            "article_title": "MTA proposes new bus lane on Northern Blvd",
            "article_url": "https://example.com/transportation_housing/1",
        },
        {
            "headline": "Affordable housing lottery opens for new development",
            "bullets": [
                "Applications open this week for 240 units at 80-130% AMI.",
            ],
            "so_what": "Constituents will likely contact the office with application questions.",
            "source_name": "The City",
            "article_title": "Affordable housing lottery opens for new development",
            "article_url": "https://example.com/transportation_housing/2",
        },
    ],
}


def build_summary_record(issue_key: str, story: dict) -> dict:
    return {
        "headline": story["headline"],
        "bullets": story["bullets"],
        "summary": story["headline"],
        "issue_area": issue_key,
        "source_level": "local",
        "so_what": story["so_what"],
        "source_citation": {
            "source_name": story["source_name"],
            "source_level": "local",
            "article_title": story["article_title"],
            "article_url": story["article_url"],
            "published_at": None,
        },
        "summary_status": "generated",
        "summary_method": "llm_api",
        "failure_reason": None,
        "summarized_at": "2026-06-17T08:00:00+00:00",
    }


def build_summaries_payload(day: date) -> dict:
    summaries = []
    for issue_key, stories in SAMPLE_STORIES.items():
        for story in stories:
            summaries.append(build_summary_record(issue_key, story))
    return {
        "generated_at": f"{day.isoformat()}T09:00:00+00:00",
        "source_file": "sample",
        "article_count": len(summaries),
        "summaries": summaries,
    }


def build_report_markdown(day: date) -> str:
    sections = [f"# Daily Briefing — {day.isoformat()}"]
    for issue_key, stories in SAMPLE_STORIES.items():
        label = ISSUE_AREAS.get(issue_key, {}).get("label", issue_key)
        lines = [f"## {label}"]
        for story in stories:
            lines.append(f"**{story['headline']}**")
            for bullet in story["bullets"]:
                lines.append(f"- {bullet}")
            lines.append(f"\n**So what?** {story['so_what']}")
            lines.append(f"\nSource: [{story['article_title']}]({story['article_url']}) — {story['source_name']}")
        sections.append("\n\n".join(lines))
    return "\n\n".join(sections) + "\n"


def build_report_text(day: date) -> str:
    lines = [f"Daily briefing for {day.isoformat()}."]
    for issue_key, stories in SAMPLE_STORIES.items():
        label = ISSUE_AREAS.get(issue_key, {}).get("label", issue_key)
        lines.append(label)
        for story in stories:
            lines.append(story["headline"])
    return "\n\n".join(lines) + "\n"


def write_silent_wav(path: Path, seconds: float = 2.0, sample_rate: int = 16000) -> None:
    frame_count = int(seconds * sample_rate)
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        silence_frame = struct.pack("<h", 0)
        wav_file.writeframes(silence_frame * frame_count)


def main() -> None:
    ARTICLE_SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)

    for day in SAMPLE_DAYS:
        summaries_path = ARTICLE_SUMMARIES_DIR / f"article_summaries_{day.isoformat()}.json"
        summaries_path.write_text(json.dumps(build_summaries_payload(day), indent=2), encoding="utf-8")

        (REPORTS_DIR / f"daily_briefing_{day.isoformat()}.md").write_text(
            build_report_markdown(day), encoding="utf-8"
        )
        (REPORTS_DIR / f"daily_briefing_{day.isoformat()}.txt").write_text(
            build_report_text(day), encoding="utf-8"
        )

        write_silent_wav(AUDIO_DIR / f"daily_briefing_{day.isoformat()}.wav")

        print(f"Seeded sample data for {day.isoformat()}")


if __name__ == "__main__":
    main()
