"""Clean and deduplicate extracted article records.

This stage reads extracted articles from ``data/raw/articles/``, removes exact
and near-duplicate records, keeps the strongest version of each duplicate set,
and writes the cleaned batch to ``data/processed/deduped/``.
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

from config import BASE_DIR, DEDUPE


DEFAULT_INPUT_DIR = BASE_DIR / "data" / "raw" / "articles"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "processed" / "deduped"
DEFAULT_OUTPUT_PREFIX = "deduped_articles"

LOGGER = logging.getLogger("clean_and_dedupe")


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_article_payload(article_path: Path) -> dict[str, Any]:
    """Load the extracted-article JSON payload from disk."""
    return json.loads(article_path.read_text(encoding="utf-8"))


def resolve_input_file(
    input_dir: Path,
    input_file: Path | None = None,
    run_date: str | None = None,
) -> Path:
    """Resolve which extracted-article file this run should process."""
    if input_file is not None:
        return input_file

    if run_date is not None:
        dated_path = input_dir / f"articles_{run_date}.json"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Article file not found for date: {run_date}")

    article_files = sorted(input_dir.glob("articles_*.json"))
    if not article_files:
        raise FileNotFoundError(f"No article files found in {input_dir}")

    return article_files[-1]


def normalize_url(url: str | None) -> str:
    """Normalize URL values for exact duplicate detection."""
    if not url:
        return ""
    return url.strip()


def normalize_text(text: str | None) -> str:
    """Normalize free text before similarity comparisons."""
    if not text:
        return ""
    lowered = text.lower()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def metadata_completeness_score(article: dict[str, Any]) -> int:
    """Count how many useful metadata fields are populated."""
    fields = (
        "title",
        "url",
        "source_name",
        "source_level",
        "source_priority",
        "published_at",
        "author",
        "article_text",
        "extraction_method",
        "extracted_at",
    )
    return sum(1 for field in fields if article.get(field))


def article_text_length(article: dict[str, Any]) -> int:
    """Measure extracted-text richness for duplicate selection."""
    return len(str(article.get("article_text") or "").strip())


def source_priority(article: dict[str, Any]) -> float:
    """Return the article source-priority value as a float."""
    try:
        return float(article.get("source_priority") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def article_quality_key(article: dict[str, Any]) -> tuple[int, float, int]:
    """Build the comparison key used to pick the best duplicate candidate."""
    # Prefer records with richer article bodies first, then stronger sources,
    # then better metadata coverage for tie-breaking.
    return (
        article_text_length(article),
        source_priority(article),
        metadata_completeness_score(article),
    )


def select_best_article(articles: list[dict[str, Any]]) -> dict[str, Any]:
    """Select the strongest article from a duplicate group."""
    return max(articles, key=article_quality_key)


def dedupe_exact_by_url(
    articles: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Remove exact duplicates by URL while keeping the best article per URL."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    passthrough: list[dict[str, Any]] = []

    for article in articles:
        normalized_url = normalize_url(article.get("url"))
        if not normalized_url:
            passthrough.append(article)
            continue
        grouped.setdefault(normalized_url, []).append(article)

    deduped: list[dict[str, Any]] = list(passthrough)
    removed_count = 0

    for grouped_articles in grouped.values():
        if len(grouped_articles) == 1:
            deduped.append(grouped_articles[0])
            continue

        best_article = select_best_article(grouped_articles)
        deduped.append(best_article)
        removed_count += len(grouped_articles) - 1

    return deduped, removed_count


def title_similarity(left: str | None, right: str | None) -> float:
    """Compute a normalized similarity score between two titles."""
    normalized_left = normalize_text(left)
    normalized_right = normalize_text(right)
    if not normalized_left or not normalized_right:
        return 0.0
    return SequenceMatcher(None, normalized_left, normalized_right).ratio()


def text_similarity(left: str | None, right: str | None) -> float:
    """Compute a normalized similarity score between two article bodies."""
    normalized_left = normalize_text(left)
    normalized_right = normalize_text(right)
    if not normalized_left or not normalized_right:
        return 0.0
    return SequenceMatcher(None, normalized_left, normalized_right).ratio()


def token_overlap_ratio(left: str | None, right: str | None) -> float:
    """Measure token overlap between two article bodies."""
    left_tokens = set(normalize_text(left).split())
    right_tokens = set(normalize_text(right).split())
    if not left_tokens or not right_tokens:
        return 0.0

    shared_tokens = left_tokens & right_tokens
    baseline = min(len(left_tokens), len(right_tokens))
    if baseline == 0:
        return 0.0
    return len(shared_tokens) / baseline


def cluster_near_duplicates_by_title(
    articles: list[dict[str, Any]],
    threshold: float,
) -> list[list[dict[str, Any]]]:
    """Group together records whose titles are very similar."""
    clusters: list[list[dict[str, Any]]] = []
    used_indexes: set[int] = set()

    # This simple clustering pass is good enough for daily batches and keeps
    # the duplicate logic easy to inspect and tune.
    for index, article in enumerate(articles):
        if index in used_indexes:
            continue

        cluster = [article]
        used_indexes.add(index)
        title = article.get("title")

        for candidate_index in range(index + 1, len(articles)):
            if candidate_index in used_indexes:
                continue

            candidate = articles[candidate_index]
            if title_similarity(title, candidate.get("title")) >= threshold:
                cluster.append(candidate)
                used_indexes.add(candidate_index)

        clusters.append(cluster)

    return clusters


def dedupe_near_duplicates_by_title(
    articles: list[dict[str, Any]],
    threshold: float,
) -> tuple[list[dict[str, Any]], int]:
    """Remove near-duplicates based on title similarity."""
    clusters = cluster_near_duplicates_by_title(articles, threshold)
    deduped = [select_best_article(cluster) for cluster in clusters]
    removed_count = len(articles) - len(deduped)
    return deduped, removed_count


def dedupe_near_duplicates_by_text(
    articles: list[dict[str, Any]],
    threshold: float,
    min_token_overlap: float,
) -> tuple[list[dict[str, Any]], int]:
    """Optionally remove near-duplicates based on article-body similarity."""
    deduped: list[dict[str, Any]] = []
    removed_count = 0

    for article in articles:
        matched_index: int | None = None

        for index, kept_article in enumerate(deduped):
            similarity = text_similarity(
                article.get("article_text"),
                kept_article.get("article_text"),
            )
            overlap = token_overlap_ratio(
                article.get("article_text"),
                kept_article.get("article_text"),
            )
            if similarity >= threshold and overlap >= min_token_overlap:
                matched_index = index
                break

        if matched_index is None:
            deduped.append(article)
            continue

        best_article = select_best_article([deduped[matched_index], article])
        deduped[matched_index] = best_article
        removed_count += 1

    return deduped, removed_count


def sort_articles_for_stability(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return articles in a stable, high-signal order for downstream jobs."""
    return sorted(
        articles,
        key=lambda article: (
            -source_priority(article),
            -(article_text_length(article)),
            str(article.get("title") or ""),
        ),
    )


def clean_and_dedupe_articles(articles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Run the configured dedupe steps and return cleaned articles plus stats."""
    stats = {
        "input_articles": len(articles),
        "removed_exact_url_duplicates": 0,
        "removed_title_near_duplicates": 0,
        "removed_text_near_duplicates": 0,
        "output_articles": 0,
    }

    exact_url_match_enabled = bool(DEDUPE.get("exact_url_match", True))
    title_threshold = float(DEDUPE.get("title_similarity_threshold", 0.9))
    text_threshold = float(DEDUPE.get("near_duplicate_similarity_threshold", 0.88))
    min_token_overlap = float(DEDUPE.get("min_token_overlap_threshold", 0.75))

    current_articles = list(articles)

    if exact_url_match_enabled:
        current_articles, removed_count = dedupe_exact_by_url(current_articles)
        stats["removed_exact_url_duplicates"] = removed_count
        LOGGER.info("Removed %s exact URL duplicates", removed_count)

    current_articles, removed_count = dedupe_near_duplicates_by_title(
        current_articles,
        threshold=title_threshold,
    )
    stats["removed_title_near_duplicates"] = removed_count
    LOGGER.info(
        "Removed %s near-duplicates by title similarity (threshold=%s)",
        removed_count,
        title_threshold,
    )

    # Text-based dedupe is only meaningful when both records contain article
    # bodies, so the helper naturally skips empty texts via the similarity fn.
    current_articles, removed_count = dedupe_near_duplicates_by_text(
        current_articles,
        threshold=text_threshold,
        min_token_overlap=min_token_overlap,
    )
    stats["removed_text_near_duplicates"] = removed_count
    LOGGER.info(
        "Removed %s near-duplicates by article-text similarity (threshold=%s, overlap=%s)",
        removed_count,
        text_threshold,
        min_token_overlap,
    )

    current_articles = sort_articles_for_stability(current_articles)
    stats["output_articles"] = len(current_articles)
    return current_articles, stats


def write_deduped_json(
    articles: list[dict[str, Any]],
    stats: dict[str, int],
    output_dir: Path,
    run_time: datetime,
    source_file: Path,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> Path:
    """Write cleaned article records to a dated JSON file."""
    output_path = output_dir / f"{output_prefix}_{run_time.date().isoformat()}.json"
    payload = {
        "generated_at": run_time.isoformat(),
        "source_file": str(source_file),
        "article_count": len(articles),
        "dedupe_stats": stats,
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
        help="Directory containing extracted article JSON files",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Specific extracted article JSON file to process",
    )
    parser.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Date for the extracted article file in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for deduped article JSON output",
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

    LOGGER.info("Loading extracted articles from %s", input_path)
    payload = load_article_payload(input_path)
    articles = list(payload.get("articles", []))
    LOGGER.info("Loaded %s extracted articles", len(articles))

    run_time = datetime.now(UTC)
    cleaned_articles, stats = clean_and_dedupe_articles(articles)
    output_path = write_deduped_json(
        articles=cleaned_articles,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    LOGGER.info("Wrote %s cleaned articles to %s", len(cleaned_articles), output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
