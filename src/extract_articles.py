"""Extract full article text from a dated candidate file.

This stage reads normalized candidate metadata from ``data/raw/candidates/``,
downloads each article URL, extracts article text, and writes enriched article
records to ``data/raw/articles/``. It is structured so source-specific rules
from ``config.py`` can be layered in later without rewriting the core flow.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import BASE_DIR, LOCAL_SOURCES  # NATIONAL_SOURCES, STATE_SOURCES

try:
    import trafilatura
except ImportError:  # pragma: no cover - optional dependency by design.
    trafilatura = None


DEFAULT_INPUT_DIR = BASE_DIR / "data" / "raw" / "candidates"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "raw" / "articles"
DEFAULT_OUTPUT_PREFIX = "articles"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_MIN_TEXT_WORDS = 150
USER_AGENT = "AutomatedDailyNewsBriefing/1.0 (+https://example.local)"

LOGGER = logging.getLogger("extract_articles")


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_session() -> Session:
    """Build a requests session with retry support for article downloads."""
    # Extraction runs across many publisher domains, so transient network and
    # upstream availability issues should not fail the full batch immediately.
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

    # for source_group in STATE_SOURCES.values():
    #     sources.extend(source_group)

    for source_group in LOCAL_SOURCES.values():
        sources.extend(source_group)

    return sources


def build_source_index() -> dict[str, dict[str, object]]:
    """Index sources by name for later source-specific extraction rules."""
    # The extraction stage only needs a light lookup layer today, but keeping
    # it centralized makes it easy to add selectors and custom parsing later.
    return {str(source["name"]): source for source in flatten_sources()}


def fetch_response(
    session: Session,
    url: str,
    timeout: int,
    request_headers: dict[str, str] | None = None,
) -> Response | None:
    """Fetch a URL and return the response or ``None`` on failure."""
    headers = dict(session.headers)
    if request_headers:
        headers.update(request_headers)

    try:
        response = session.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        # One bad article should be recorded and skipped rather than crashing
        # the whole extraction run.
        LOGGER.warning("Request failed for %s: %s", url, exc)
        return None


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace into single spaces."""
    return re.sub(r"\s+", " ", text).strip()


def clean_article_text(text: str) -> str:
    """Normalize extracted article text before output."""
    # This is intentionally conservative so later cleaning stages still have
    # access to most of the source material.
    return normalize_whitespace(text)


def word_count(text: str) -> int:
    """Return a simple whitespace-based word count."""
    return len([token for token in text.split() if token])


def load_candidate_payload(candidate_path: Path) -> dict[str, Any]:
    """Load the candidate JSON payload from disk."""
    return json.loads(candidate_path.read_text(encoding="utf-8"))


def resolve_candidate_file(
    input_dir: Path,
    candidate_file: Path | None = None,
    run_date: str | None = None,
) -> Path:
    """Resolve which candidate file this run should process."""
    if candidate_file is not None:
        return candidate_file

    if run_date is not None:
        dated_path = input_dir / f"article_candidates_{run_date}.json"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Candidate file not found for date: {run_date}")

    candidate_files = sorted(input_dir.glob("article_candidates_*.json"))
    if not candidate_files:
        raise FileNotFoundError(f"No candidate files found in {input_dir}")

    # Defaulting to the newest file is convenient for local iteration and cron
    # runs that only need the most recent candidate batch.
    return candidate_files[-1]


def get_source_config(
    source_index: dict[str, dict[str, object]],
    source_name: str,
) -> dict[str, object]:
    """Return the configured source definition or a safe fallback."""
    return source_index.get(
        source_name,
        {
            "name": source_name,
            "extraction": {
                "content_selector": None,
                "exclude_selectors": [],
                "date_selector": None,
                "follow_redirects": True,
                "render_js": False,
                "request_headers": {},
            },
        },
    )


def extract_with_trafilatura(html: str, url: str) -> dict[str, str | None] | None:
    """Attempt article extraction using trafilatura when installed."""
    if trafilatura is None:
        return None

    # Including metadata here gives us a stronger first-pass extractor when the
    # dependency is present, while the BeautifulSoup fallback remains portable.
    extracted = trafilatura.extract(
        html,
        url=url,
        include_comments=False,
        include_tables=False,
        include_images=False,
        include_formatting=False,
        with_metadata=True,
        favor_precision=True,
    )
    if not extracted:
        return None

    metadata = trafilatura.extract_metadata(html, default_url=url)
    return {
        "article_text": clean_article_text(extracted),
        "title": getattr(metadata, "title", None) if metadata else None,
        "author": getattr(metadata, "author", None) if metadata else None,
        "published_at": getattr(metadata, "date", None) if metadata else None,
        "extraction_method": "trafilatura",
    }


def remove_unwanted_nodes(soup: BeautifulSoup, exclude_selectors: Iterable[str]) -> None:
    """Remove unwanted page sections before text extraction."""
    for selector in exclude_selectors:
        for node in soup.select(selector):
            node.decompose()


def extract_author_from_soup(soup: BeautifulSoup) -> str | None:
    """Extract an author value using a few common article metadata patterns."""
    selectors = (
        ('meta[name="author"]', "content"),
        ('meta[property="article:author"]', "content"),
        ('[rel="author"]', None),
        (".byline", None),
        (".author", None),
    )

    for selector, attribute in selectors:
        node = soup.select_one(selector)
        if node is None:
            continue
        if attribute:
            value = node.get(attribute)
        else:
            value = node.get_text(" ", strip=True)
        if value:
            return normalize_whitespace(value)
    return None


def extract_published_at_from_soup(
    soup: BeautifulSoup,
    date_selector: str | None,
) -> str | None:
    """Extract the best-available publication timestamp from HTML."""
    candidate_selectors = []
    if date_selector:
        candidate_selectors.append(date_selector)
    candidate_selectors.extend(
        [
            'meta[property="article:published_time"]',
            'meta[name="article:published_time"]',
            "time[datetime]",
            "time",
        ]
    )

    for selector in candidate_selectors:
        node = soup.select_one(selector)
        if node is None:
            continue
        if node.has_attr("datetime"):
            return normalize_whitespace(str(node["datetime"]))
        if node.has_attr("content"):
            return normalize_whitespace(str(node["content"]))
        text = node.get_text(" ", strip=True)
        if text:
            return normalize_whitespace(text)
    return None


def extract_title_from_soup(soup: BeautifulSoup) -> str | None:
    """Extract a page title using article-first selectors."""
    selectors = (
        "article h1",
        "main h1",
        "h1",
        'meta[property="og:title"]',
        "title",
    )

    for selector in selectors:
        node = soup.select_one(selector)
        if node is None:
            continue
        if node.name == "meta":
            value = node.get("content")
        else:
            value = node.get_text(" ", strip=True)
        if value:
            return normalize_whitespace(value)
    return None


def extract_text_with_bs4(
    html: str,
    source_config: dict[str, object],
) -> dict[str, str | None] | None:
    """Extract article text and metadata with BeautifulSoup selectors."""
    soup = BeautifulSoup(html, "html.parser")
    extraction_config = source_config.get("extraction", {})
    content_selector = None
    exclude_selectors: list[str] = []
    date_selector = None

    if isinstance(extraction_config, dict):
        content_selector = extraction_config.get("content_selector")
        exclude_selectors = list(extraction_config.get("exclude_selectors", []))
        date_selector = extraction_config.get("date_selector")

    # Removing boilerplate upfront improves the fallback text quality even when
    # we only have generic selectors available.
    remove_unwanted_nodes(
        soup,
        [
            "script",
            "style",
            "noscript",
            "header",
            "footer",
            "nav",
            "aside",
            ".advertisement",
            ".ads",
            ".related",
            *exclude_selectors,
        ],
    )

    content_node = None
    if isinstance(content_selector, str) and content_selector.strip():
        content_node = soup.select_one(content_selector)

    if content_node is None:
        for selector in ("article", "main article", "main", ".article-body", ".post-content", ".entry-content", "body"):
            content_node = soup.select_one(selector)
            if content_node is not None:
                break

    if content_node is None:
        return None

    text = clean_article_text(content_node.get_text(" ", strip=True))
    if not text:
        return None

    return {
        "article_text": text,
        "title": extract_title_from_soup(soup),
        "author": extract_author_from_soup(soup),
        "published_at": extract_published_at_from_soup(soup, date_selector),
        "extraction_method": "beautifulsoup",
    }


def extract_article_content(
    html: str,
    url: str,
    source_config: dict[str, object],
) -> dict[str, str | None] | None:
    """Extract article content with trafilatura first and BeautifulSoup second."""
    extracted = extract_with_trafilatura(html, url)
    if extracted and extracted.get("article_text"):
        return extracted

    return extract_text_with_bs4(html, source_config)


def build_article_record(
    candidate: dict[str, Any],
    extraction: dict[str, str | None],
    extracted_at: str,
) -> dict[str, Any]:
    """Merge candidate metadata with extracted article fields."""
    # Candidate metadata remains the source of truth for pipeline identity so
    # later stages can join records even if extraction changes the title slightly.
    return {
        "title": extraction.get("title") or candidate.get("article_title"),
        "url": candidate.get("article_url"),
        "source_name": candidate.get("source_name"),
        "source_level": candidate.get("source_level"),
        "source_priority": candidate.get("source_priority"),
        "published_at": candidate.get("published_at") or extraction.get("published_at"),
        "author": extraction.get("author"),
        "article_text": extraction.get("article_text"),
        "extraction_method": extraction.get("extraction_method"),
        "extracted_at": extracted_at,
    }


def extract_candidate_article(
    candidate: dict[str, Any],
    session: Session,
    source_index: dict[str, dict[str, object]],
    timeout: int,
    min_text_words: int,
) -> dict[str, Any] | None:
    """Extract a single candidate article and return the normalized article record."""
    article_url = str(candidate.get("article_url") or "").strip()
    source_name = str(candidate.get("source_name") or "").strip()
    if not article_url or not source_name:
        LOGGER.warning("Skipping candidate with missing URL or source name: %s", candidate)
        return None

    source_config = get_source_config(source_index, source_name)
    extraction_config = source_config.get("extraction", {})
    request_headers: dict[str, str] | None = None
    if isinstance(extraction_config, dict):
        candidate_headers = extraction_config.get("request_headers")
        if isinstance(candidate_headers, dict):
            request_headers = {
                str(key): str(value) for key, value in candidate_headers.items()
            }

    response = fetch_response(
        session=session,
        url=article_url,
        timeout=timeout,
        request_headers=request_headers,
    )
    if response is None:
        return None

    extraction = extract_article_content(
        html=response.text,
        url=article_url,
        source_config=source_config,
    )
    if extraction is None:
        LOGGER.warning("No extractable article content found for %s", article_url)
        return None

    article_text = str(extraction.get("article_text") or "")
    if word_count(article_text) < min_text_words:
        LOGGER.info(
            "Dropping short article for %s (%s words)",
            article_url,
            word_count(article_text),
        )
        return None

    return build_article_record(
        candidate=candidate,
        extraction=extraction,
        extracted_at=datetime.now(UTC).isoformat(),
    )


def extract_articles(
    candidates: Iterable[dict[str, Any]],
    timeout: int,
    min_text_words: int,
) -> list[dict[str, Any]]:
    """Extract article text for all candidates in the input payload."""
    session = build_session()
    source_index = build_source_index()
    extracted_articles: list[dict[str, Any]] = []
    candidates_list = list(candidates)

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {}
        for candidate in candidates_list:
            article_url = candidate.get("article_url")
            LOGGER.info("Submitting extraction: %s", article_url)
            future = executor.submit(
                extract_candidate_article,
                candidate=candidate,
                session=session,
                source_index=source_index,
                timeout=timeout,
                min_text_words=min_text_words,
            )
            future_to_url[future] = article_url

        for future in as_completed(future_to_url):
            article_url = future_to_url[future]
            try:
                article = future.result()
            except Exception as exc:  # pragma: no cover - defensive batch protection.
                LOGGER.exception("Unexpected extraction failure for %s: %s", article_url, exc)
                continue

            if article is not None:
                extracted_articles.append(article)

    return extracted_articles


def write_articles_json(
    articles: list[dict[str, Any]],
    output_dir: Path,
    run_time: datetime,
    source_file: Path,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> Path:
    """Write extracted article records to a dated JSON file."""
    output_path = output_dir / f"{output_prefix}_{run_time.date().isoformat()}.json"
    payload = {
        "generated_at": run_time.isoformat(),
        "source_file": str(source_file),
        "article_count": len(articles),
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
        help="Directory containing dated candidate JSON files",
    )
    parser.add_argument(
        "--candidate-file",
        type=Path,
        default=None,
        help="Specific candidate JSON file to process",
    )
    parser.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Date for the candidate file in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for dated extracted-article JSON output",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--min-text-words",
        type=int,
        default=DEFAULT_MIN_TEXT_WORDS,
        help=f"Minimum cleaned article length in words (default: {DEFAULT_MIN_TEXT_WORDS})",
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

    input_dir = args.input_dir
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    candidate_path = resolve_candidate_file(
        input_dir=input_dir,
        candidate_file=args.candidate_file,
        run_date=args.run_date,
    )
    LOGGER.info("Loading candidate file: %s", candidate_path)
    candidate_payload = load_candidate_payload(candidate_path)
    candidates = candidate_payload.get("articles", [])

    run_time = datetime.now(UTC)
    LOGGER.info("Starting article extraction for %s candidates", len(candidates))
    articles = extract_articles(
        candidates=candidates,
        timeout=args.timeout,
        min_text_words=args.min_text_words,
    )
    output_path = write_articles_json(
        articles=articles,
        output_dir=output_dir,
        run_time=run_time,
        source_file=candidate_path,
    )
    LOGGER.info("Wrote %s extracted articles to %s", len(articles), output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
