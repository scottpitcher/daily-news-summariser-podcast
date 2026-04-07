"""Generate structured article summaries for selected ranked stories.

This stage reads ranked articles from ``data/processed/ranked/``, summarizes
each selected article into a concise structured record, and writes the results
to ``data/processed/article_summaries/``. The implementation is designed for
an OpenAI-compatible LLM API but keeps the client boundary modular so other
providers can be added later.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from config import BASE_DIR, MODELS


DEFAULT_INPUT_DIR = BASE_DIR / "data" / "processed" / "ranked"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "processed" / "article_summaries"
DEFAULT_OUTPUT_PREFIX = "article_summaries"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PROVIDER = str(MODELS["summarization"].get("provider") or "huggingface")
DEFAULT_MODEL = str(MODELS["summarization"].get("model") or "Qwen/Qwen2.5-7B-Instruct")
DEFAULT_MAX_TOKENS = int(MODELS["summarization"].get("max_tokens") or 1200)
DEFAULT_TEMPERATURE = float(MODELS["summarization"].get("temperature") or 0.2)
DEFAULT_TARGET_SUMMARY_WORDS = int(MODELS["summarization"].get("target_summary_words") or 120)
DEFAULT_API_BASE = str(MODELS["summarization"].get("base_url") or "https://router.huggingface.co/v1")
DEFAULT_API_KEY = MODELS["summarization"].get("api_key")

LOGGER = logging.getLogger("summarize_articles")

SYSTEM_PROMPT_TEMPLATE = str(
    MODELS["summarization"].get("system_prompt_template")
    or (
        "You write concise, factual news briefing summaries for the office of "
        "NYC Council Member Virginia Maloney (District 4, Manhattan). "
        "She sits on these committees: Sanitation and Solid Waste Management, "
        "Small Business, Finance, Cultural Affairs/Libraries/International Relations, "
        "Economic Development (Chair), Fire and Emergency Management, Higher Education, "
        "Housing and Buildings. She co-chairs the Irish Caucus and is in the Women's Caucus. "
        "Return valid JSON only with keys: summary, why_it_matters_to_nyc. "
        "The summary must be 2 to 4 sentences, neutral in tone, and useful "
        "for a spoken daily briefing. why_it_matters_to_nyc must be a specific, "
        "concrete sentence explaining how this story connects to NYC policy, "
        "legislation, city services, or the daily lives of New Yorkers — "
        "especially where it touches CM Maloney's committee portfolio. "
        "If the article is national or international, explain the local NYC "
        "angle or downstream impact. Never use generic filler. "
        "Avoid hype, speculation, and filler."
    )
)


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_payload(input_path: Path) -> dict[str, Any]:
    """Load the ranked-article JSON payload from disk."""
    return json.loads(input_path.read_text(encoding="utf-8"))


def resolve_input_file(
    input_dir: Path,
    input_file: Path | None = None,
    run_date: str | None = None,
) -> Path:
    """Resolve which ranked-article file this run should process."""
    if input_file is not None:
        return input_file

    if run_date is not None:
        dated_path = input_dir / f"ranked_articles_{run_date}.json"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Ranked article file not found for date: {run_date}")

    input_files = sorted(input_dir.glob("ranked_articles_*.json"))
    if not input_files:
        raise FileNotFoundError(f"No ranked article files found in {input_dir}")

    return input_files[-1]


def normalize_text(text: str | None) -> str:
    """Normalize whitespace for prompts and fallback summaries."""
    if not text:
        return ""
    normalized = re.sub(r"\s+", " ", text)
    return normalized.strip()


def split_sentences(text: str) -> list[str]:
    """Split text into approximate sentences for fallback summarization."""
    cleaned = normalize_text(text)
    if not cleaned:
        return []
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    return [sentence.strip() for sentence in sentences if sentence.strip()]


def collect_selected_articles(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the selected articles grouped by issue area into one list."""
    selected_by_issue = payload.get("selected_by_issue_area", {})
    selected_articles: list[dict[str, Any]] = []

    # A ranked article can appear under more than one issue area, so the issue
    # context is carried forward explicitly for the summarization prompt.
    for issue_area, issue_articles in selected_by_issue.items():
        for article in issue_articles:
            article_with_issue = dict(article)
            article_with_issue["issue_area"] = issue_area
            selected_articles.append(article_with_issue)

    return selected_articles


def build_user_prompt(article: dict[str, Any], target_summary_words: int) -> str:
    """Build the article-specific prompt for the summarization model."""
    article_text = strip_frontmatter(str(article.get("article_text") or ""))
    article_text = normalize_text(article_text)[:12000]

    # The prompt includes ranking and source metadata because those fields are
    # often useful for explaining why a story matters in the final briefing.
    prompt_payload = {
        "issue_area": article.get("issue_area"),
        "title": article.get("title"),
        "source_name": article.get("source_name"),
        "source_level": article.get("source_level"),
        "source_priority": article.get("source_priority"),
        "published_at": article.get("published_at"),
        "url": article.get("url"),
        "target_summary_words": target_summary_words,
        "article_text": article_text,
    }
    return (
        "Summarize the following news article for a spoken daily briefing.\n"
        "Return valid JSON with keys:\n"
        '- "summary": 2 to 4 sentences\n'
        '- "why_it_matters_to_nyc": 1 specific sentence on how this connects to NYC policy, legislation, city services, or daily life for New Yorkers (never generic)\n\n'
        f"{json.dumps(prompt_payload, ensure_ascii=True)}"
    )


def parse_llm_json(content: str) -> dict[str, Any] | None:
    """Parse the model response as JSON, tolerating fenced code blocks."""
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_+-]*\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def build_chat_payload(
    article: dict[str, Any],
    model: str,
    temperature: float,
    max_tokens: int,
    target_summary_words: int,
) -> dict[str, Any]:
    """Build the chat-completions request payload."""
    return {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT_TEMPLATE},
            {"role": "user", "content": build_user_prompt(article, target_summary_words)},
        ],
    }


def summarize_with_openai_compatible_api(
    article: dict[str, Any],
    api_key: str,
    base_url: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    target_summary_words: int,
) -> dict[str, str] | None:
    """Generate a structured summary using an OpenAI-compatible API."""
    endpoint = base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = build_chat_payload(
        article=article,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        target_summary_words=target_summary_words,
    )

    response = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
    response.raise_for_status()
    response_payload = response.json()
    content = (
        response_payload.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    parsed = parse_llm_json(str(content))
    if parsed is None:
        return None

    summary = normalize_text(str(parsed.get("summary") or ""))
    why_it_matters = normalize_text(str(parsed.get("why_it_matters_to_nyc") or parsed.get("why_it_matters") or ""))
    if not summary:
        return None

    return {
        "summary": summary,
        "why_it_matters_to_nyc": why_it_matters,
    }


def strip_frontmatter(text: str) -> str:
    """Remove YAML-style frontmatter (``--- ... ---``) from scraped article text."""
    return re.sub(r"^---\s.*?---\s*", "", text, count=1, flags=re.DOTALL).strip()


def fallback_summary(article: dict[str, Any]) -> dict[str, str]:
    """Create a simple extractive fallback summary when API generation fails."""
    raw_text = strip_frontmatter(str(article.get("article_text") or ""))
    sentences = split_sentences(raw_text)
    summary_sentences = sentences[:3]
    summary = " ".join(summary_sentences).strip()

    # The fallback keeps the output usable for downstream synthesis even when
    # the model call fails or is not configured in the environment.
    if not summary:
        summary = normalize_text(str(article.get("title") or ""))

    why_it_matters = (
        f"Relevant to NYC {article.get('issue_area', 'policy').replace('_', ' ')} "
        f"— check the full article for specific local implications."
    )
    return {
        "summary": summary,
        "why_it_matters_to_nyc": normalize_text(why_it_matters),
    }


def build_summary_record(
    article: dict[str, Any],
    summary_fields: dict[str, str],
    summary_status: str,
    summarized_at: str,
    failure_reason: str | None = None,
) -> dict[str, Any]:
    """Build the normalized article-summary output record."""
    citation = {
        "source_name": article.get("source_name"),
        "source_level": article.get("source_level"),
        "article_title": article.get("title"),
        "article_url": article.get("url"),
        "published_at": article.get("published_at"),
    }
    return {
        "summary": summary_fields.get("summary"),
        "issue_area": article.get("issue_area"),
        "source_level": article.get("source_level"),
        "why_it_matters_to_nyc": summary_fields.get("why_it_matters_to_nyc"),
        "source_citation": citation,
        "summary_status": summary_status,
        "summary_method": "llm_api" if summary_status == "generated" else "fallback",
        "failure_reason": failure_reason,
        "summarized_at": summarized_at,
    }


def summarize_article(
    article: dict[str, Any],
    provider: str,
    api_key: str | None,
    base_url: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    target_summary_words: int,
) -> dict[str, Any]:
    """Summarize one selected ranked article with graceful fallback behavior."""
    summary_fields: dict[str, str] | None = None
    summary_status = "fallback"
    failure_reason: str | None = None

    try:
        if provider in ("huggingface", "openai", "cerebras") and api_key:
            summary_fields = summarize_with_openai_compatible_api(
                article=article,
                api_key=api_key,
                base_url=base_url,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
                target_summary_words=target_summary_words,
            )
        elif provider in ("huggingface", "openai"):
            failure_reason = "missing_api_key"
        else:
            failure_reason = f"unsupported_provider:{provider}"
    except requests.RequestException as exc:
        LOGGER.warning("Summary request failed for %s: %s", article.get("url"), exc)
        failure_reason = f"request_error:{exc}"
    except Exception as exc:  # pragma: no cover - defensive runtime guard.
        LOGGER.exception("Unexpected summarization failure for %s: %s", article.get("url"), exc)
        failure_reason = f"unexpected_error:{exc}"

    if summary_fields:
        summary_status = "generated"
    else:
        if failure_reason is None:
            failure_reason = "empty_or_invalid_model_response"
        summary_fields = fallback_summary(article)

    return build_summary_record(
        article=article,
        summary_fields=summary_fields,
        summary_status=summary_status,
        summarized_at=datetime.now(UTC).isoformat(),
        failure_reason=failure_reason,
    )


def summarize_articles(
    articles: list[dict[str, Any]],
    provider: str,
    api_key: str | None,
    base_url: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    target_summary_words: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Summarize all selected articles and return summary records plus stats."""
    summary_records: list[dict[str, Any]] = []
    stats = {
        "input_articles": len(articles),
        "generated_summaries": 0,
        "fallback_summaries": 0,
    }

    for article in articles:
        LOGGER.info(
            "Summarizing article for issue %s: %s",
            article.get("issue_area"),
            article.get("title"),
        )
        summary_record = summarize_article(
            article=article,
            provider=provider,
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            target_summary_words=target_summary_words,
        )
        if summary_record["summary_status"] == "generated":
            stats["generated_summaries"] += 1
        else:
            stats["fallback_summaries"] += 1
        summary_records.append(summary_record)

    return summary_records, stats


def write_summaries_json(
    summaries: list[dict[str, Any]],
    stats: dict[str, int],
    output_dir: Path,
    run_time: datetime,
    source_file: Path,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> Path:
    """Write article-summary records to a dated JSON file."""
    output_path = output_dir / f"{output_prefix}_{run_time.date().isoformat()}.json"
    payload = {
        "generated_at": run_time.isoformat(),
        "source_file": str(source_file),
        "summary_count": len(summaries),
        "summary_stats": stats,
        "summaries": summaries,
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
        help="Directory containing ranked article JSON files",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Specific ranked article JSON file to process",
    )
    parser.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Date for the ranked article file in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for article-summary JSON output",
    )
    parser.add_argument(
        "--provider",
        default=DEFAULT_PROVIDER,
        help=f"Summarization provider (default: {DEFAULT_PROVIDER})",
    )
    parser.add_argument(
        "--api-base",
        default=DEFAULT_API_BASE,
        help="Base URL for the summarization API",
    )
    parser.add_argument(
        "--api-key",
        default=DEFAULT_API_KEY,
        help="API key for the summarization provider",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Summarization model (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=DEFAULT_TEMPERATURE,
        help=f"Model temperature (default: {DEFAULT_TEMPERATURE})",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=DEFAULT_MAX_TOKENS,
        help=f"Maximum output tokens for each summary (default: {DEFAULT_MAX_TOKENS})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--target-summary-words",
        type=int,
        default=DEFAULT_TARGET_SUMMARY_WORDS,
        help=f"Target summary word budget (default: {DEFAULT_TARGET_SUMMARY_WORDS})",
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

    LOGGER.info("Loading ranked articles from %s", input_path)
    payload = load_payload(input_path)
    selected_articles = collect_selected_articles(payload)
    LOGGER.info("Loaded %s selected ranked articles", len(selected_articles))

    run_time = datetime.now(UTC)
    summaries, stats = summarize_articles(
        articles=selected_articles,
        provider=args.provider,
        api_key=str(args.api_key) if args.api_key else None,
        base_url=args.api_base,
        model=args.model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout=args.timeout,
        target_summary_words=args.target_summary_words,
    )
    output_path = write_summaries_json(
        summaries=summaries,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    LOGGER.info(
        "Wrote %s article summaries to %s (%s generated, %s fallback)",
        len(summaries),
        output_path,
        stats["generated_summaries"],
        stats["fallback_summaries"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
