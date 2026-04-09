"""Build the final spoken-briefing transcript from article summaries.

This stage reads per-article summaries from ``data/processed/article_summaries/``,
organizes the briefing by issue area and source level, trims content to fit the
configured transcript budget, and writes both Markdown and plain-text versions
to ``outputs/reports/``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config import BRIEFING_OUTPUT, ISSUE_AREAS


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DEFAULT_INPUT_DIR = BASE_DIR / "data" / "processed" / "article_summaries"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_OUTPUT_PREFIX = "daily_briefing"
DEFAULT_MAX_LISTEN_MINUTES = 25.0

LOGGER = logging.getLogger("build_briefing")

SECTION_INTRO_TEMPLATE = (
    "In {issue_label}, here are the main updates from the national, state, and local levels."
)


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_payload(input_path: Path) -> dict[str, Any]:
    """Load the article-summary JSON payload from disk."""
    return json.loads(input_path.read_text(encoding="utf-8"))


def resolve_input_file(
    input_dir: Path,
    input_file: Path | None = None,
    run_date: str | None = None,
) -> Path:
    """Resolve which article-summary file this run should process."""
    if input_file is not None:
        return input_file

    if run_date is not None:
        dated_path = input_dir / f"article_summaries_{run_date}.json"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Article summary file not found for date: {run_date}")

    input_files = sorted(input_dir.glob("article_summaries_*.json"))
    if not input_files:
        raise FileNotFoundError(f"No article summary files found in {input_dir}")

    return input_files[-1]


def normalize_text(text: str | None) -> str:
    """Normalize whitespace for transcript generation."""
    if not text:
        return ""
    normalized = re.sub(r"\s+", " ", text)
    return normalized.strip()


def word_count(text: str) -> int:
    """Return an approximate whitespace-based word count."""
    return len([token for token in text.split() if token])


def transcript_word_budget(max_listen_minutes: float) -> int:
    """Calculate the transcript word budget for this briefing."""
    configured_max_words = int(BRIEFING_OUTPUT.get("transcript_max_words") or 1400)
    words_per_minute = int(BRIEFING_OUTPUT.get("tts_words_per_minute") or 155)
    listen_budget_words = int(max_listen_minutes * words_per_minute)

    # The smaller of the two limits wins so the transcript stays aligned with
    # both editorial preference and spoken-duration constraints.
    return min(configured_max_words, listen_budget_words)


def sort_by_source_level(summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort summaries in national, then state, then local order."""
    level_order = {
        "national": 0,
        "state": 1,
        "local": 2,
    }
    return sorted(
        summaries,
        key=lambda summary: (
            level_order.get(str(summary.get("source_level") or "").lower(), 99),
            str(summary.get("source_citation", {}).get("source_name") or ""),
        ),
    )


NO_CONNECTION_PHRASE = "no direct committee or district connection identified"


def _has_connection(summary: dict[str, Any]) -> bool:
    """Return False if the so_what field explicitly says there is no connection."""
    so_what = str(summary.get("so_what") or summary.get("why_it_matters_to_nyc") or "").strip().lower()
    return NO_CONNECTION_PHRASE not in so_what


def group_summaries_by_issue_area(summaries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group summaries by issue area and sort within each issue."""
    grouped: dict[str, list[dict[str, Any]]] = {}

    for summary in summaries:
        issue_area = str(summary.get("issue_area") or "").strip()
        if not issue_area:
            continue
        if not _has_connection(summary):
            LOGGER.info(
                "Dropping article with no committee/district connection: %s",
                summary.get("source_citation", {}).get("article_title", "unknown"),
            )
            continue
        grouped.setdefault(issue_area, []).append(summary)

    for issue_area, issue_summaries in grouped.items():
        grouped[issue_area] = sort_by_source_level(issue_summaries)

    return grouped


def issue_area_sort_key(issue_area: str) -> tuple[int, str]:
    """Sort issue areas by configured editorial priority and label."""
    issue_config = ISSUE_AREAS.get(issue_area, {})
    priority = str(issue_config.get("priority") or "").lower()
    priority_rank = {"high": 0, "medium": 1, "low": 2}.get(priority, 3)
    return priority_rank, issue_area


def format_source_attribution(summary: dict[str, Any]) -> str:
    """Create lightweight source attribution suitable for audio delivery."""
    citation = summary.get("source_citation", {})
    source_name = normalize_text(str(citation.get("source_name") or ""))
    source_level = normalize_text(str(summary.get("source_level") or ""))
    if source_name and source_level:
        return f"According to {source_name}, via {source_level} coverage"
    if source_name:
        return f"According to {source_name}"
    return "According to reporting"


def format_source_attribution_markdown(summary: dict[str, Any]) -> str:
    """Create source attribution with a linked headline for Markdown output."""
    citation = summary.get("source_citation", {})
    source_name = normalize_text(str(citation.get("source_name") or ""))
    article_title = normalize_text(str(citation.get("article_title") or ""))
    article_url = normalize_text(str(citation.get("article_url") or ""))

    if article_title and article_url:
        link = f"[{article_title}]({article_url})"
    elif article_url:
        link = f"[Source]({article_url})"
    else:
        link = ""

    if link and source_name:
        return f"{link} — {source_name}"
    if link:
        return link
    if source_name:
        return source_name
    return "Source unavailable"


def build_story_paragraph(summary: dict[str, Any]) -> str:
    """Build one scannable story block with headline, bullets, and so-what."""
    headline = normalize_text(str(summary.get("headline") or summary.get("summary") or ""))
    bullets = summary.get("bullets") or []
    so_what = normalize_text(str(summary.get("so_what") or summary.get("why_it_matters_to_nyc") or ""))
    attribution = format_source_attribution(summary)

    lines = [headline]
    for bullet in bullets:
        bullet_text = normalize_text(str(bullet))
        if bullet_text:
            lines.append(f"- {bullet_text}")
    if so_what:
        lines.append(f"So what? {so_what}")
    lines.append(f"{attribution}.")
    return "\n".join(lines)


def build_issue_section(issue_area: str, summaries: list[dict[str, Any]]) -> str:
    """Build the spoken section for one issue area."""
    issue_label = str(ISSUE_AREAS.get(issue_area, {}).get("label") or issue_area.replace("_", " ").title())
    section_parts = [
        issue_label,
        SECTION_INTRO_TEMPLATE.format(issue_label=issue_label.lower()),
    ]

    for summary in summaries:
        section_parts.append(build_story_paragraph(summary))

    return "\n\n".join(section_parts)


def trim_grouped_summaries_to_budget(
    grouped_summaries: dict[str, list[dict[str, Any]]],
    word_budget: int,
) -> dict[str, list[dict[str, Any]]]:
    """Trim grouped summaries so the final transcript fits the target budget."""
    selected: dict[str, list[dict[str, Any]]] = {}
    total_words = 0

    intro_words = word_count(str(BRIEFING_OUTPUT.get("intro_template") or ""))
    outro_words = word_count(str(BRIEFING_OUTPUT.get("outro_template") or ""))
    total_words += intro_words + outro_words

    for issue_area in sorted(grouped_summaries, key=issue_area_sort_key):
        issue_summaries = grouped_summaries[issue_area]
        if not issue_summaries:
            continue

        kept_issue_summaries: list[dict[str, Any]] = []
        section_header_words = word_count(
            str(ISSUE_AREAS.get(issue_area, {}).get("label") or issue_area)
        ) + 12

        for summary in issue_summaries:
            story_text = build_story_paragraph(summary)
            projected_words = total_words + section_header_words + word_count(story_text)
            if projected_words > word_budget and kept_issue_summaries:
                break
            if projected_words > word_budget:
                LOGGER.info(
                    "Skipping summary for %s to stay within transcript budget",
                    issue_area,
                )
                continue

            kept_issue_summaries.append(summary)
            total_words += word_count(story_text)

        if kept_issue_summaries:
            total_words += section_header_words
            selected[issue_area] = kept_issue_summaries

    return selected


def build_briefing_text(grouped_summaries: dict[str, list[dict[str, Any]]]) -> tuple[str, str]:
    """Build the final plain-text and Markdown briefing transcripts."""
    intro = normalize_text(str(BRIEFING_OUTPUT.get("intro_template") or ""))
    outro = normalize_text(str(BRIEFING_OUTPUT.get("outro_template") or ""))

    text_sections: list[str] = [intro]
    markdown_sections: list[str] = [intro]

    for issue_area in sorted(grouped_summaries, key=issue_area_sort_key):
        issue_label = str(ISSUE_AREAS.get(issue_area, {}).get("label") or issue_area.replace("_", " ").title())
        section_text = build_issue_section(issue_area, grouped_summaries[issue_area])
        text_sections.append(section_text)

        markdown_story_lines = [f"## {issue_label}"]
        for summary in grouped_summaries[issue_area]:
            headline = normalize_text(str(summary.get("headline") or summary.get("summary") or ""))
            bullets = summary.get("bullets") or []
            so_what = normalize_text(str(summary.get("so_what") or summary.get("why_it_matters_to_nyc") or ""))
            source_link = format_source_attribution_markdown(summary)

            story_block = [f"**{headline}**"]
            for bullet in bullets:
                bullet_text = normalize_text(str(bullet))
                if bullet_text:
                    story_block.append(f"- {bullet_text}")
            if so_what:
                story_block.append(f"\n**So what?** {so_what}")
            story_block.append(f"\nSource: {source_link}")
            markdown_story_lines.append("\n".join(story_block))
        markdown_sections.append("\n\n".join(markdown_story_lines))

    text_sections.append(outro)
    markdown_sections.append(outro)

    return "\n\n".join(text_sections).strip(), "\n\n".join(markdown_sections).strip()


def write_report_files(
    transcript_text: str,
    transcript_markdown: str,
    output_dir: Path,
    run_time: datetime,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> tuple[Path, Path]:
    """Write both plain-text and Markdown briefing reports."""
    date_suffix = run_time.date().isoformat()
    txt_path = output_dir / f"{output_prefix}_{date_suffix}.txt"
    md_path = output_dir / f"{output_prefix}_{date_suffix}.md"

    txt_path.write_text(transcript_text + "\n", encoding="utf-8")
    md_path.write_text(transcript_markdown + "\n", encoding="utf-8")
    return txt_path, md_path


def build_briefing(
    summaries: list[dict[str, Any]],
    max_listen_minutes: float,
) -> tuple[str, str, dict[str, Any]]:
    """Build the final briefing transcript and supporting stats."""
    grouped_summaries = group_summaries_by_issue_area(summaries)
    word_budget = transcript_word_budget(max_listen_minutes)
    trimmed_grouped_summaries = trim_grouped_summaries_to_budget(grouped_summaries, word_budget)
    transcript_text, transcript_markdown = build_briefing_text(trimmed_grouped_summaries)

    stats = {
        "input_summaries": len(summaries),
        "included_summaries": sum(len(issue_summaries) for issue_summaries in trimmed_grouped_summaries.values()),
        "issue_areas_included": len(trimmed_grouped_summaries),
        "transcript_word_count": word_count(transcript_text),
        "transcript_word_budget": word_budget,
        "target_audio_duration_minutes": max_listen_minutes,
    }
    return transcript_text, transcript_markdown, stats


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory containing article summary JSON files",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Specific article summary JSON file to process",
    )
    parser.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Date for the article summary file in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for final briefing report files",
    )
    parser.add_argument(
        "--max-listen-minutes",
        type=float,
        default=DEFAULT_MAX_LISTEN_MINUTES,
        help=f"Maximum target listening time in minutes (default: {DEFAULT_MAX_LISTEN_MINUTES})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> int:
    """Entry point for CLI execution."""
    args = parse_args()
    configure_logging(verbose=args.verbose)

    input_path = resolve_input_file(
        input_dir=args.input_dir,
        input_file=args.input_file,
        run_date=args.run_date,
    )
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Loading article summaries from %s", input_path)
    payload = load_payload(input_path)
    summaries = list(payload.get("summaries", []))
    LOGGER.info("Loaded %s article summaries", len(summaries))

    run_time = datetime.now(UTC)
    transcript_text, transcript_markdown, stats = build_briefing(
        summaries=summaries,
        max_listen_minutes=args.max_listen_minutes,
    )
    txt_path, md_path = write_report_files(
        transcript_text=transcript_text,
        transcript_markdown=transcript_markdown,
        output_dir=output_dir,
        run_time=run_time,
    )
    LOGGER.info(
        "Wrote briefing reports to %s and %s (%s words, %s included summaries)",
        txt_path,
        md_path,
        stats["transcript_word_count"],
        stats["included_summaries"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
