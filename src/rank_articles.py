"""Rank tagged articles and select the top stories per issue area.

This stage reads tagged article records from ``data/processed/tagged/``,
calculates a transparent weighted score for each relevant article, ranks
articles within each issue area, selects the top stories using configurable
issue caps from ``config.py``, and writes the ranked batch to
``data/processed/ranked/``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from config import BASE_DIR, ISSUE_AREA_ARTICLE_CAPS, ISSUE_AREAS, RANKING_WEIGHTS


DEFAULT_INPUT_DIR = BASE_DIR / "data" / "processed" / "tagged"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "processed" / "ranked"
DEFAULT_OUTPUT_PREFIX = "ranked_articles"

# These constants stay local to the ranking script because they shape the score
# normalization curve rather than acting like environment-level configuration.
RECENCY_FULL_SCORE_HOURS = 24.0
RECENCY_ZERO_SCORE_HOURS = 168.0
CONTENT_FULL_SCORE_WORDS = 1200
TITLE_MIN_SPECIFIC_WORDS = 5
TITLE_MAX_SPECIFIC_WORDS = 18

LOGGER = logging.getLogger("rank_articles")

VAGUE_TITLE_TERMS = {
    "live",
    "updates",
    "update",
    "watch",
    "what to know",
    "here s what we know",
    "photos",
    "video",
}


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_payload(input_path: Path) -> dict[str, Any]:
    """Load the tagged-article JSON payload from disk."""
    return json.loads(input_path.read_text(encoding="utf-8"))


def resolve_input_file(
    input_dir: Path,
    input_file: Path | None = None,
    run_date: str | None = None,
) -> Path:
    """Resolve which tagged-article file this run should process."""
    if input_file is not None:
        return input_file

    if run_date is not None:
        dated_path = input_dir / f"tagged_articles_{run_date}.json"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Tagged article file not found for date: {run_date}")

    input_files = sorted(input_dir.glob("tagged_articles_*.json"))
    if not input_files:
        raise FileNotFoundError(f"No tagged article files found in {input_dir}")

    return input_files[-1]


def normalize_text(text: str | None) -> str:
    """Normalize free text for lightweight ranking heuristics."""
    if not text:
        return ""
    lowered = text.lower()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-like datetime string into a timezone-aware datetime."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def article_word_count(article: dict[str, Any]) -> int:
    """Return an approximate article-body word count."""
    article_text = str(article.get("article_text") or "")
    return len([token for token in article_text.split() if token])


def source_priority_score(article: dict[str, Any]) -> float:
    """Return a normalized source-priority score."""
    try:
        score = float(article.get("source_priority") or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(score, 1.0))


def recency_score(article: dict[str, Any], now: datetime) -> float:
    """Score how recent the article is based on ``published_at``."""
    published_at = parse_datetime(str(article.get("published_at") or ""))
    if published_at is None:
        return 0.2

    age_hours = max((now - published_at).total_seconds() / 3600.0, 0.0)
    if age_hours <= RECENCY_FULL_SCORE_HOURS:
        return 1.0
    if age_hours >= RECENCY_ZERO_SCORE_HOURS:
        return 0.0

    remaining = RECENCY_ZERO_SCORE_HOURS - age_hours
    window = RECENCY_ZERO_SCORE_HOURS - RECENCY_FULL_SCORE_HOURS
    return max(0.0, min(remaining / window, 1.0))


def issue_priority_factor(issue_key: str) -> float:
    """Convert issue-area priority labels into numeric score factors."""
    issue_config = ISSUE_AREAS.get(issue_key, {})
    priority = str(issue_config.get("priority") or "").lower()
    if priority == "high":
        return 1.0
    if priority == "medium":
        return 0.75
    return 0.5


def issue_relevance_score(article: dict[str, Any], issue_key: str) -> float:
    """Score how central the issue area is for the article."""
    issue_tags = list(article.get("issue_area_tags") or [])
    if issue_key not in issue_tags:
        return 0.0

    # Earlier tags indicate stronger rule-based confidence from the tagging
    # stage, so the primary tag gets the highest issue-relevance score.
    tag_position = issue_tags.index(issue_key)
    position_factor = 1.0 / (tag_position + 1)
    return max(0.0, min(position_factor * issue_priority_factor(issue_key), 1.0))


def title_specificity_score(article: dict[str, Any]) -> float:
    """Score whether the headline looks specific and information-rich."""
    title = normalize_text(str(article.get("title") or ""))
    if not title:
        return 0.0

    words = [token for token in title.split() if token]
    word_count = len(words)
    if word_count < TITLE_MIN_SPECIFIC_WORDS:
        base_score = word_count / TITLE_MIN_SPECIFIC_WORDS
    elif word_count <= TITLE_MAX_SPECIFIC_WORDS:
        base_score = 1.0
    else:
        overflow = word_count - TITLE_MAX_SPECIFIC_WORDS
        base_score = max(0.4, 1.0 - (overflow / TITLE_MAX_SPECIFIC_WORDS))

    vague_penalty = 0.0
    for phrase in VAGUE_TITLE_TERMS:
        if normalize_text(phrase) in title:
            vague_penalty = max(vague_penalty, 0.2)

    return max(0.0, min(base_score - vague_penalty, 1.0))


def content_quality_score(article: dict[str, Any]) -> float:
    """Score article completeness using extracted-body length and metadata."""
    word_count = article_word_count(article)
    length_factor = min(word_count / CONTENT_FULL_SCORE_WORDS, 1.0)

    metadata_bonus = 0.0
    if article.get("author"):
        metadata_bonus += 0.05
    if article.get("published_at"):
        metadata_bonus += 0.05

    return max(0.0, min(length_factor + metadata_bonus, 1.0))


def local_relevance_score(article: dict[str, Any]) -> float:
    """Score the local/state relevance signal from the source level."""
    source_level = str(article.get("source_level") or "").lower()
    if source_level == "local":
        return 1.0
    if source_level == "state":
        return 0.8
    if source_level == "national":
        return 0.6
    return 0.5


def title_similarity(left: str | None, right: str | None) -> float:
    """Measure title overlap between two articles."""
    normalized_left = normalize_text(left)
    normalized_right = normalize_text(right)
    if not normalized_left or not normalized_right:
        return 0.0
    return SequenceMatcher(None, normalized_left, normalized_right).ratio()


def cross_source_confirmation_score(
    article: dict[str, Any],
    issue_articles: list[dict[str, Any]],
) -> float:
    """Score whether similar coverage appears from other sources."""
    title = article.get("title")
    source_name = article.get("source_name")
    confirming_sources: set[str] = set()

    for candidate in issue_articles:
        if candidate is article:
            continue
        if candidate.get("source_name") == source_name:
            continue
        if title_similarity(title, candidate.get("title")) >= 0.75:
            confirming_sources.add(str(candidate.get("source_name") or ""))

    # Multiple corroborating sources help, but the score caps quickly so it
    # cannot overwhelm the more important editorial factors.
    return min(len(confirming_sources) / 3.0, 1.0)


def overlap_penalty(
    article: dict[str, Any],
    issue_articles: list[dict[str, Any]],
) -> float:
    """Calculate a small penalty for highly overlapping same-issue coverage."""
    max_similarity = 0.0
    for candidate in issue_articles:
        if candidate is article:
            continue
        similarity = title_similarity(article.get("title"), candidate.get("title"))
        max_similarity = max(max_similarity, similarity)

    # Only penalize very high overlap. Lower similarity is better interpreted
    # as corroboration and handled by the cross-source-confirmation signal.
    if max_similarity < 0.92:
        return 0.0
    return min((max_similarity - 0.92) / 0.08, 1.0)


def score_article_for_issue(
    article: dict[str, Any],
    issue_key: str,
    issue_articles: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    """Calculate the full weighted score breakdown for one article in one issue area."""
    component_scores = {
        "source_priority": source_priority_score(article),
        "recency": recency_score(article, now),
        "issue_priority": issue_relevance_score(article, issue_key),
        "local_relevance": local_relevance_score(article),
        "title_signal": title_specificity_score(article),
        "content_quality": content_quality_score(article),
        "cross_source_confirmation": cross_source_confirmation_score(article, issue_articles),
    }
    penalty_scores = {
        "overlap_penalty": overlap_penalty(article, issue_articles),
    }

    weighted_total = 0.0
    for factor_name, component_score in component_scores.items():
        factor_weight = float(RANKING_WEIGHTS.get(factor_name, 0.0))
        weighted_total += factor_weight * component_score

    # The penalty is intentionally modest so strong articles are not pushed out
    # entirely just because several outlets covered the same event.
    weighted_total -= 0.05 * penalty_scores["overlap_penalty"]

    score_breakdown = {
        "weights": {factor_name: float(RANKING_WEIGHTS.get(factor_name, 0.0)) for factor_name in component_scores},
        "components": component_scores,
        "penalties": penalty_scores,
        "final_score": round(weighted_total, 6),
    }

    ranked_article = dict(article)
    ranked_article.update(
        {
            "rank_issue_area": issue_key,
            "score_breakdown": score_breakdown,
            "ranking_method": "weighted_rules_v1",
        }
    )
    return ranked_article


def rank_issue_area_articles(
    issue_key: str,
    articles: list[dict[str, Any]],
    now: datetime,
) -> list[dict[str, Any]]:
    """Rank relevant articles for one issue area."""
    issue_articles = [
        article
        for article in articles
        if article.get("is_relevant") and issue_key in list(article.get("issue_area_tags") or [])
    ]

    ranked_articles = [
        score_article_for_issue(article, issue_key, issue_articles, now)
        for article in issue_articles
    ]
    ranked_articles.sort(
        key=lambda article: (
            -float(article["score_breakdown"]["final_score"]),
            -float(article.get("source_priority") or 0.0),
            str(article.get("title") or ""),
        )
    )

    for index, article in enumerate(ranked_articles, start=1):
        article["issue_rank"] = index

    return ranked_articles


def select_top_articles_by_issue(
    ranked_by_issue: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Select the top-ranked articles per issue area using configured caps."""
    selected: dict[str, list[dict[str, Any]]] = {}

    for issue_key, ranked_articles in ranked_by_issue.items():
        issue_cap = int(ISSUE_AREA_ARTICLE_CAPS.get(issue_key, 0))
        selected_articles = ranked_articles[:issue_cap] if issue_cap > 0 else []

        # Mark selected status on the copied issue-specific ranked records so
        # debugging output shows both the order and editorial cutoff.
        for article in ranked_articles:
            article["selected_for_briefing"] = article["issue_rank"] <= issue_cap

        selected[issue_key] = selected_articles

    return selected


def rank_articles(articles: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]], dict[str, int]]:
    """Rank all relevant articles by issue area and apply per-issue caps."""
    now = datetime.now(UTC)
    ranked_by_issue: dict[str, list[dict[str, Any]]] = {}

    for issue_key in ISSUE_AREAS:
        ranked_by_issue[issue_key] = rank_issue_area_articles(issue_key, articles, now)

    selected_by_issue = select_top_articles_by_issue(ranked_by_issue)
    stats = {
        "input_articles": len(articles),
        "relevant_articles": sum(1 for article in articles if article.get("is_relevant")),
        "ranked_articles": sum(len(issue_articles) for issue_articles in ranked_by_issue.values()),
        "selected_articles": sum(len(issue_articles) for issue_articles in selected_by_issue.values()),
    }
    return ranked_by_issue, selected_by_issue, stats


def write_ranked_json(
    ranked_by_issue: dict[str, list[dict[str, Any]]],
    selected_by_issue: dict[str, list[dict[str, Any]]],
    stats: dict[str, int],
    output_dir: Path,
    run_time: datetime,
    source_file: Path,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> Path:
    """Write ranked article records to a dated JSON file."""
    output_path = output_dir / f"{output_prefix}_{run_time.date().isoformat()}.json"
    payload = {
        "generated_at": run_time.isoformat(),
        "source_file": str(source_file),
        "ranking_weights": dict(RANKING_WEIGHTS),
        "issue_area_caps": dict(ISSUE_AREA_ARTICLE_CAPS),
        "ranking_stats": stats,
        "ranked_by_issue_area": ranked_by_issue,
        "selected_by_issue_area": selected_by_issue,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory containing tagged article JSON files",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Specific tagged article JSON file to process",
    )
    parser.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Date for the tagged article file in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for ranked article JSON output",
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

    LOGGER.info("Loading tagged articles from %s", input_path)
    payload = load_payload(input_path)
    articles = list(payload.get("articles", []))
    LOGGER.info("Loaded %s tagged articles", len(articles))

    run_time = datetime.now(UTC)
    ranked_by_issue, selected_by_issue, stats = rank_articles(articles)
    output_path = write_ranked_json(
        ranked_by_issue=ranked_by_issue,
        selected_by_issue=selected_by_issue,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    LOGGER.info(
        "Wrote ranked articles to %s (%s ranked entries, %s selected entries)",
        output_path,
        stats["ranked_articles"],
        stats["selected_articles"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
