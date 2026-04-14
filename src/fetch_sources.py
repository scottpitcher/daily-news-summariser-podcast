"""Fetch article candidates and metadata from configured news sources.

This script reads source definitions from ``config.py``, collects candidate
articles from RSS feeds and simple HTML pages, normalizes the records into a
shared schema, deduplicates by URL, and writes a dated JSON snapshot.
It intentionally stops at candidate collection and does not extract full
article body text.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import feedparser
import requests
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    BASE_DIR,
    LOCAL_SOURCES,
    # NATIONAL_SOURCES,
    PIPELINE_STAGES,
    SOURCE_PRIORITY_WEIGHTS,
    # STATE_SOURCES,
    ensure_output_dirs,
)


DEFAULT_TIMEOUT_SECONDS = int(
    PIPELINE_STAGES["fetch_candidate_links"]["request_timeout_seconds"]
)
MAX_CANDIDATES_PER_SOURCE = int(
    PIPELINE_STAGES["fetch_candidate_links"]["max_candidates_per_source"]
)
DEFAULT_OUTPUT_PREFIX = "article_candidates"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "raw" / "candidates"
USER_AGENT = "AutomatedDailyNewsBriefing/1.0 (+https://example.local)"

LOGGER = logging.getLogger("fetch_sources")


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_session() -> Session:
    """Build a requests session with retry support for transient failures."""
    # Retries are limited to safe idempotent methods so transient upstream
    # errors do not fail the whole ingestion run immediately.
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def flatten_sources() -> list[dict[str, object]]:
    """Return all configured sources as a single iterable collection."""
    sources: list[dict[str, object]] = []
    # sources.extend(NATIONAL_SOURCES)

    # State and local sources are stored by geography in config, but the fetch
    # pipeline only needs a flat list of source definitions.
    # for source_group in STATE_SOURCES.values():
    #     sources.extend(source_group)

    for source_group in LOCAL_SOURCES.values():
        sources.extend(source_group)

    return sources


def fetch_response(session: Session, url: str, timeout: int) -> Response | None:
    """Fetch a URL and return the response or ``None`` on failure."""
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        # Network failures and non-2xx responses are logged and converted to a
        # soft failure so one broken source does not abort the run.
        LOGGER.warning("Request failed for %s: %s", url, exc)
        return None


def parse_datetime(value: Any) -> str | None:
    """Convert common feed and HTML datetime values into ISO-8601 UTC strings."""
    if value is None:
        return None

    # Feedparser can return datetimes as strings, datetime objects, or
    # time.struct_time-like values depending on the source and parser result.
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
    elif hasattr(value, "tm_year"):
        try:
            dt = datetime(
                value.tm_year,
                value.tm_mon,
                value.tm_mday,
                value.tm_hour,
                value.tm_min,
                value.tm_sec,
                tzinfo=UTC,
            )
        except (TypeError, ValueError):
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    # Normalize everything to UTC so downstream storage and comparisons stay
    # consistent across feeds with different timezone conventions.
    return dt.astimezone(UTC).isoformat()


def normalize_url(url: str) -> str:
    """Normalize URLs before deduplication."""
    # Stripping fragments removes page anchors that do not change article
    # identity and would otherwise create duplicate records.
    parsed = urlparse(url.strip())
    clean = parsed._replace(fragment="", query=parsed.query.strip())
    return urlunparse(clean)


def get_source_priority(source: dict[str, object]) -> float:
    """Resolve the ranking priority assigned to a source."""
    source_name = str(source["name"])

    # Explicit per-source overrides win because they are easier to tune as the
    # editorial mix evolves. The source-level default remains the fallback.
    if source_name in SOURCE_PRIORITY_WEIGHTS:
        return float(SOURCE_PRIORITY_WEIGHTS[source_name])

    return float(source.get("priority_weight", 0.7))


def build_article_record(
    source: dict[str, object],
    title: str,
    article_url: str,
    published_at: str | None,
    fetched_at: str,
) -> dict[str, str | float | None]:
    """Normalize a candidate article record."""
    return {
        "source_name": source["name"],
        "source_level": source["level"],
        "source_priority": get_source_priority(source),
        "article_title": title.strip(),
        "article_url": normalize_url(article_url),
        "published_at": published_at,
        "source_type": source["type"],
        "fetched_at": fetched_at,
    }


def extract_rss_articles(
    source: dict[str, object],
    session: Session,
    timeout: int,
    fetched_at: str,
) -> list[dict[str, str | float | None]]:
    """Fetch and normalize article candidates from an RSS or Atom feed."""
    rss_url = source.get("rss_url")
    if not rss_url:
        return []

    # We fetch the feed content ourselves so retries, timeouts, headers, and
    # error handling are consistent with plain HTML source fetching.
    response = fetch_response(session, rss_url, timeout)
    if response is None:
        return []

    parsed_feed = feedparser.parse(response.content)
    if getattr(parsed_feed, "bozo", False):
        # feedparser marks malformed feeds as "bozo" but still often exposes
        # usable entries, so this is only a debug signal rather than a failure.
        LOGGER.debug(
            "Feed parser reported non-fatal issue for %s: %s",
            rss_url,
            getattr(parsed_feed, "bozo_exception", "unknown error"),
        )

    articles: list[dict[str, str | float | None]] = []
    for entry in parsed_feed.entries:
        title = (entry.get("title") or "").strip()
        article_url = (entry.get("link") or "").strip()
        if not title or not article_url:
            continue

        # Publication timestamps vary widely across feeds, so we try the most
        # common fields in order from structured to raw string variants.
        published_at = (
            parse_datetime(entry.get("published_parsed"))
            or parse_datetime(entry.get("updated_parsed"))
            or parse_datetime(entry.get("published"))
            or parse_datetime(entry.get("updated"))
        )
        articles.append(
            build_article_record(
                source=source,
                title=title,
                article_url=article_url,
                published_at=published_at,
                fetched_at=fetched_at,
            )
        )

    LOGGER.info("Fetched %s RSS articles from %s", len(articles), source["name"])
    return articles


def extract_datetime_from_node(node: Any) -> str | None:
    """Try a few common publication-date attributes on an HTML node."""
    if node is None:
        return None

    # These patterns cover the most common ways CMS templates expose article
    # publication times without requiring source-specific scraping rules.
    if node.has_attr("datetime"):
        return parse_datetime(node["datetime"])
    if node.has_attr("content"):
        return parse_datetime(node["content"])
    if node.string:
        return parse_datetime(node.string.strip())
    return None


def extract_html_articles(
    source: dict[str, object],
    session: Session,
    timeout: int,
    fetched_at: str,
) -> list[dict[str, str | float | None]]:
    """Extract article candidates from a simple source page using generic selectors."""
    homepage_url = source.get("homepage_url")
    if not homepage_url:
        return []

    response = fetch_response(session, homepage_url, timeout)
    if response is None:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    articles: list[dict[str, str | float | None]] = []
    seen_urls: set[str] = set()

    # Start with likely article containers; fall back to all links for simpler
    # government/newsroom pages that may not use semantic article markup.
    containers = soup.select("article, main article, .post, .story, .card, .headline, .article-list a, [data-type='article']")
    if not containers:
        containers = soup.select("a[href]")

    for container in containers:
        link = container if getattr(container, "name", None) == "a" else container.find("a", href=True)
        if link is None:
            continue

        raw_url = (link.get("href") or "").strip()
        title = link.get_text(" ", strip=True)
        if not raw_url or not title:
            continue

        # Relative links are common on publisher homepages, so normalize them
        # against the source homepage before deduplication.
        article_url = urljoin(homepage_url, raw_url)
        normalized_url = normalize_url(article_url)
        if normalized_url in seen_urls:
            continue

        # Skip obvious navigation and non-article links from generic page scraping.
        parsed = urlparse(normalized_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if any(token in normalized_url.lower() for token in ("/tag/", "/topic/", "/search", "/contact", "/about")):
            continue

        published_at = None
        if getattr(container, "name", None) != "a":
            # HTML pages are much less structured than feeds, so publication
            # dates are best-effort and optional in the normalized output.
            published_at = (
                extract_datetime_from_node(container.find("time"))
                or extract_datetime_from_node(container.find("meta", attrs={"property": "article:published_time"}))
                or extract_datetime_from_node(container.find("meta", attrs={"name": "article:published_time"}))
            )

        articles.append(
            build_article_record(
                source=source,
                title=title,
                article_url=normalized_url,
                published_at=published_at,
                fetched_at=fetched_at,
            )
        )
        seen_urls.add(normalized_url)

    LOGGER.info("Fetched %s HTML articles from %s", len(articles), source["name"])
    return articles


def fetch_source_articles(
    source: dict[str, object],
    session: Session,
    timeout: int,
    fetched_at: str,
) -> list[dict[str, str | float | None]]:
    """Fetch article candidates for a single source with RSS-first behavior."""
    source_type = source.get("type")

    if source_type == "rss":
        # RSS is preferred because it usually provides cleaner titles, links,
        # and timestamps than generic homepage scraping.
        articles = extract_rss_articles(source, session, timeout, fetched_at)
        if not articles:
            LOGGER.info("Falling back to HTML extraction for %s", source["name"])
            articles = extract_html_articles(source, session, timeout, fetched_at)
    else:
        articles = extract_html_articles(source, session, timeout, fetched_at)

    if len(articles) > MAX_CANDIDATES_PER_SOURCE:
        LOGGER.info(
            "Capping %s candidates from %s to %s",
            len(articles),
            source["name"],
            MAX_CANDIDATES_PER_SOURCE,
        )
        articles = articles[:MAX_CANDIDATES_PER_SOURCE]

    return articles


def deduplicate_articles(
    articles: Iterable[dict[str, str | float | None]],
) -> list[dict[str, str | float | None]]:
    """Deduplicate normalized article records by canonical URL."""
    deduplicated: list[dict[str, str | float | None]] = []
    seen_urls: set[str] = set()

    for article in articles:
        article_url = article.get("article_url")
        if not article_url or article_url in seen_urls:
            continue
        # First-seen wins, which keeps the collection order stable and avoids
        # source-specific tie-breaking logic for now.
        seen_urls.add(article_url)
        deduplicated.append(article)

    return deduplicated


_SPORTS_URL_TOKENS = (
    "/sports/", "/sport/",
    "/nfl/", "/nba/", "/mlb/", "/nhl/", "/ncaa/", "/mls/", "/wnba/",
    "/soccer/", "/football/", "/baseball/", "/basketball/", "/hockey/",
)

_SPORTS_TITLE_RE = re.compile(
    r"\b(?:nfl|nba|mlb|nhl|ncaa|fifa|espn|wnba|mls"
    r"|playoffs?|super bowl|world cup|world series"
    r"|touchdown|home run|grand slam"
    r"|premier league|champions league"
    r"|quarterback|wide receiver|tight end"
    r"|jets|giants|knicks|yankees|mets|nets|rangers|islanders|liberty"
    r"|cricket|rugby|tennis|golf|boxing|ufc|mma|wrestling|gymnastics|olympic"
    r"|pitcher|outfielder|shortstop|point guard|goalie|goalkeeper)\b",
    re.IGNORECASE,
)


def _is_sports_article(article: dict[str, str | float | None]) -> bool:
    """Return True if the article appears to be sports content."""
    url = str(article.get("article_url") or "").lower()
    if any(token in url for token in _SPORTS_URL_TOKENS):
        return True

    title = str(article.get("article_title") or "")
    return bool(_SPORTS_TITLE_RE.search(title))


def filter_sports_articles(
    articles: list[dict[str, str | float | None]],
) -> list[dict[str, str | float | None]]:
    """Remove articles that appear to be sports content."""
    kept = [a for a in articles if not _is_sports_article(a)]
    dropped = len(articles) - len(kept)
    if dropped:
        LOGGER.info(
            "Sports filter: dropped %s of %s articles", dropped, len(articles)
        )
    return kept


def filter_stale_articles(
    articles: list[dict[str, str | float | None]],
    max_age_hours: int = 24, 
) -> list[dict[str, str | float | None]]:
    """Drop articles with a published_at timestamp older than *max_age_hours*.

    Articles without a publication date are kept because they are typically
    recent homepage items scraped from HTML pages.
    """
    cutoff = datetime.now(UTC) - timedelta(hours=max_age_hours)
    kept: list[dict[str, str | float | None]] = []
    for article in articles:
        published_at = article.get("published_at")
        if published_at is None:
            kept.append(article)
            continue
        try:
            pub_dt = datetime.fromisoformat(str(published_at))
        except (TypeError, ValueError):
            kept.append(article)
            continue
        if pub_dt >= cutoff:
            kept.append(article)
        else:
            LOGGER.debug(
                "Dropping stale article: %s (%s)",
                article.get("article_title", ""),
                published_at,
            )
    LOGGER.info(
        "Recency filter: kept %s of %s articles (cutoff %sh)",
        len(kept),
        len(articles),
        max_age_hours,
    )
    return kept


def write_articles_json(
    articles: list[dict[str, str | float | None]],
    output_dir: Path,
    run_time: datetime,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> Path:
    """Write normalized articles to a dated JSON file."""
    output_path = output_dir / f"{output_prefix}_{run_time.date().isoformat()}.json"
    # The wrapper metadata makes it easier to audit runs without reading the
    # full article list or relying on filesystem timestamps alone.
    payload = {
        "generated_at": run_time.isoformat(),
        "article_count": len(articles),
        "articles": articles,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return output_path


def collect_articles(timeout: int) -> list[dict[str, str | float | None]]:
    """Fetch article candidates from all configured sources concurrently."""
    session = build_session()
    fetched_at = datetime.now(UTC).isoformat()
    articles: list[dict[str, str | float | None]] = []
    sources = flatten_sources()

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_source = {
            executor.submit(
                fetch_source_articles, source, session, timeout, fetched_at
            ): source
            for source in sources
        }
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                result = future.result()
                LOGGER.info(
                    "Fetched %s candidates from %s (%s)",
                    len(result),
                    source["name"],
                    source["type"],
                )
                articles.extend(result)
            except Exception:
                LOGGER.exception(
                    "Failed to fetch from %s", source["name"]
                )

    # Deduplication is intentionally done once at the end so overlaps across
    # different configured sources collapse into a single article candidate.
    articles = deduplicate_articles(articles)
    articles = filter_stale_articles(articles)
    articles = filter_sports_articles(articles)
    return articles


def resolve_output_dir(path_override: Path | None = None) -> Path:
    """Resolve the candidate-output directory for this stage."""
    # Candidate collection now has its own raw staging area separate from later
    # extraction and cleaning outputs.
    return path_override or DEFAULT_OUTPUT_DIR


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for dated candidate JSON output",
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
    ensure_output_dirs()

    run_time = datetime.now(UTC)
    output_dir = resolve_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # A single run timestamp is used for the output file and metadata so every
    # record batch can be tied back to one ingestion execution.
    LOGGER.info("Starting candidate source fetch run")
    articles = collect_articles(timeout=args.timeout)
    output_path = write_articles_json(articles, output_dir=output_dir, run_time=run_time)
    LOGGER.info("Wrote %s deduplicated candidate records to %s", len(articles), output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
