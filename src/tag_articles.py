"""Assign issue-area tags to deduped articles.

This stage reads cleaned article records from ``data/processed/deduped/``,
assigns one or more issue-area tags from ``config.py``, marks whether each
article is relevant for the briefing, and writes the tagged batch to
``data/processed/tagged/``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from config import BASE_DIR, ISSUE_AREAS


DEFAULT_INPUT_DIR = BASE_DIR / "data" / "processed" / "deduped"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "processed" / "tagged"
DEFAULT_OUTPUT_PREFIX = "tagged_articles"

LOGGER = logging.getLogger("tag_articles")

GEOPOLITICAL_EXCLUSION_TERMS = {
    "geopolitics",
    "diplomatic",
    "foreign ministry",
    "foreign minister",
    "border conflict",
    "missile strike",
    "ceasefire",
    "armed forces",
    "military operation",
    "naval",
    "territorial dispute",
    "sanctions on",
    "nato",
}

DOMESTIC_RELEVANCE_TERMS = {
    "state",
    "city",
    "county",
    "school",
    "district",
    "public",
    "housing",
    "jobs",
    "hospital",
    "transit",
    "governor",
    "mayor",
    "congress",
    "legislation",
    "policy",
}


class ArticleClassifier(Protocol):
    """Protocol for future classifier implementations."""

    def classify(self, article: dict[str, Any]) -> dict[str, Any]:
        """Return tag and relevance decisions for an article."""


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_payload(input_path: Path) -> dict[str, Any]:
    """Load the deduped-article JSON payload from disk."""
    return json.loads(input_path.read_text(encoding="utf-8"))


def resolve_input_file(
    input_dir: Path,
    input_file: Path | None = None,
    run_date: str | None = None,
) -> Path:
    """Resolve which deduped-article file this run should process."""
    if input_file is not None:
        return input_file

    if run_date is not None:
        dated_path = input_dir / f"deduped_articles_{run_date}.json"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Deduped article file not found for date: {run_date}")

    input_files = sorted(input_dir.glob("deduped_articles_*.json"))
    if not input_files:
        raise FileNotFoundError(f"No deduped article files found in {input_dir}")

    return input_files[-1]


def normalize_text(text: str | None) -> str:
    """Normalize free text for keyword and rule matching."""
    if not text:
        return ""
    lowered = text.lower()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def article_search_text(article: dict[str, Any]) -> str:
    """Build the combined text used for MVP tagging."""
    # Combining title and body gives rule-based tagging more signal without
    # needing separate weighting logic in the first implementation.
    parts = [
        str(article.get("title") or ""),
        str(article.get("article_text") or ""),
    ]
    return normalize_text(" ".join(parts))


def issue_area_score(article_text: str, keywords: list[str]) -> int:
    """Count keyword matches for one issue area."""
    score = 0
    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        if not normalized_keyword:
            continue
        if normalized_keyword in article_text:
            score += 1
    return score


def assign_issue_area_tags(article: dict[str, Any]) -> list[str]:
    """Assign one or more issue-area tags using keyword rules."""
    search_text = article_search_text(article)
    if not search_text:
        return []

    scored_tags: list[tuple[str, int]] = []
    for issue_key, issue_config in ISSUE_AREAS.items():
        keywords = list(issue_config.get("keywords", []))
        score = issue_area_score(search_text, keywords)
        if score > 0:
            scored_tags.append((issue_key, score))

    # Returning multiple tags allows downstream ranking to handle overlap
    # between adjacent topics such as housing and transportation.
    scored_tags.sort(key=lambda item: (-item[1], item[0]))
    return [issue_key for issue_key, _ in scored_tags]


def is_geopolitical_or_off_topic(article: dict[str, Any], assigned_tags: list[str]) -> tuple[bool, str | None]:
    """Determine whether an article should be excluded from the briefing."""
    search_text = article_search_text(article)
    if not search_text:
        return True, "empty_content"

    geopolitical_hits = sum(
        1 for term in GEOPOLITICAL_EXCLUSION_TERMS if normalize_text(term) in search_text
    )
    domestic_hits = sum(
        1 for term in DOMESTIC_RELEVANCE_TERMS if normalize_text(term) in search_text
    )

    if not assigned_tags:
        return True, "no_matching_issue_area"

    # This rule intentionally excludes items that look primarily like foreign
    # affairs coverage unless there is clear domestic-policy relevance.
    if geopolitical_hits >= 2 and domestic_hits == 0:
        return True, "mostly_geopolitical"

    return False, None


def validate_tagged_article(tagged_article: dict[str, Any]) -> bool:
    """Validate that the tagged record contains the required downstream fields."""
    required_fields = (
        "title",
        "url",
        "source_name",
        "source_level",
        "source_priority",
        "issue_area_tags",
        "is_relevant",
        "excluded_reason",
        "tagged_at",
    )
    return all(field in tagged_article for field in required_fields)


class KeywordRuleClassifier:
    """MVP classifier that uses local rules and keyword scoring only."""

    def classify(self, article: dict[str, Any]) -> dict[str, Any]:
        assigned_tags = assign_issue_area_tags(article)
        is_excluded, excluded_reason = is_geopolitical_or_off_topic(article, assigned_tags)
        return {
            "issue_area_tags": assigned_tags,
            "is_relevant": not is_excluded,
            "excluded_reason": excluded_reason,
            "classification_method": "keyword_rules",
        }


def build_tagged_article(
    article: dict[str, Any],
    classification: dict[str, Any],
    tagged_at: str,
) -> dict[str, Any]:
    """Merge the original article record with tagging results."""
    tagged_article = dict(article)
    tagged_article.update(
        {
            "issue_area_tags": classification["issue_area_tags"],
            "is_relevant": classification["is_relevant"],
            "excluded_reason": classification["excluded_reason"],
            "classification_method": classification["classification_method"],
            "tagged_at": tagged_at,
        }
    )
    return tagged_article


def tag_articles(
    articles: list[dict[str, Any]],
    classifier: ArticleClassifier | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Assign tags and relevance decisions to all articles."""
    classifier = classifier or KeywordRuleClassifier()
    tagged_articles: list[dict[str, Any]] = []
    stats = {
        "input_articles": len(articles),
        "relevant_articles": 0,
        "excluded_articles": 0,
        "invalid_articles": 0,
    }

    for article in articles:
        classification = classifier.classify(article)
        tagged_article = build_tagged_article(
            article=article,
            classification=classification,
            tagged_at=datetime.now(UTC).isoformat(),
        )

        if not validate_tagged_article(tagged_article):
            stats["invalid_articles"] += 1
            LOGGER.warning("Skipping invalid tagged article for URL %s", article.get("url"))
            continue

        if tagged_article["is_relevant"]:
            stats["relevant_articles"] += 1
        else:
            stats["excluded_articles"] += 1

        tagged_articles.append(tagged_article)

    return tagged_articles, stats


def write_tagged_json(
    articles: list[dict[str, Any]],
    stats: dict[str, int],
    output_dir: Path,
    run_time: datetime,
    source_file: Path,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> Path:
    """Write tagged article records to a dated JSON file."""
    output_path = output_dir / f"{output_prefix}_{run_time.date().isoformat()}.json"
    payload = {
        "generated_at": run_time.isoformat(),
        "source_file": str(source_file),
        "article_count": len(articles),
        "tagging_stats": stats,
        "articles": articles,
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
        help="Directory containing deduped article JSON files",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Specific deduped article JSON file to process",
    )
    parser.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Date for the deduped article file in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for tagged article JSON output",
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

    LOGGER.info("Loading deduped articles from %s", input_path)
    payload = load_payload(input_path)
    articles = list(payload.get("articles", []))
    LOGGER.info("Loaded %s deduped articles", len(articles))

    run_time = datetime.now(UTC)
    tagged_articles, stats = tag_articles(articles)
    output_path = write_tagged_json(
        articles=tagged_articles,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    LOGGER.info(
        "Wrote %s tagged articles to %s (%s relevant, %s excluded)",
        len(tagged_articles),
        output_path,
        stats["relevant_articles"],
        stats["excluded_articles"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
