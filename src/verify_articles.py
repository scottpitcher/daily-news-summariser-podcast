"""Let the LLM decide, per story, whether it needs web verification before summarizing.

This stage reads ranked articles from ``data/processed/ranked/`` and, for each
selected story, asks the model to judge whether the story is well-enough
sourced to summarize as-is or whether it should call a ``web_search`` tool
first (e.g. single-source stories, load-bearing statistics/quotes/outcomes, or
stories that seem ambiguous or potentially outdated). This is a per-story
agentic decision, not a fixed step that always runs: most stories are expected
to skip straight through with no tool call. Results are written to
``data/processed/verified/`` in the same ``selected_by_issue_area`` shape that
``rank_articles`` produces, so ``summarize_articles`` can consume it unchanged
except for a "dropped" filter and an optional caveat appended to the prompt.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from config import BASE_DIR, MODELS


DEFAULT_INPUT_DIR = BASE_DIR / "data" / "processed" / "ranked"
DEFAULT_OUTPUT_DIR = BASE_DIR / "data" / "processed" / "verified"
DEFAULT_OUTPUT_PREFIX = "verified_articles"
DEFAULT_TIMEOUT_SECONDS = 30

DEFAULT_PROVIDER = str(MODELS["verification"].get("provider") or "huggingface")
DEFAULT_MODEL = str(MODELS["verification"].get("model") or "llama3.1-8b")
DEFAULT_MAX_TOKENS = int(MODELS["verification"].get("max_tokens") or 600)
DEFAULT_TEMPERATURE = float(MODELS["verification"].get("temperature") or 0.1)
DEFAULT_API_BASE = str(MODELS["verification"].get("base_url") or "https://api.cerebras.ai/v1")
DEFAULT_API_KEY = MODELS["verification"].get("api_key")

DEFAULT_SEARCH_PROVIDER = str(MODELS["verification"].get("search_provider") or "tavily")
DEFAULT_SEARCH_API_KEY = MODELS["verification"].get("search_api_key")
DEFAULT_SEARCH_BASE_URL = str(MODELS["verification"].get("search_base_url") or "https://api.tavily.com")
DEFAULT_MAX_SEARCH_RESULTS = int(MODELS["verification"].get("max_search_results") or 3)

LOGGER = logging.getLogger("verify_articles")

SYSTEM_PROMPT = (
    "You are a fact-checking gatekeeper for a daily news briefing prepared for "
    "an NYC Council Member's office. For each candidate story you will see its "
    "title, source, publish date, and article text. Decide for yourself whether "
    "the story needs verification before it can be trusted in the briefing.\n\n"
    "Call the web_search tool ONLY when warranted, for example:\n"
    "- the story comes from a single source with no corroboration\n"
    "- it makes a specific, load-bearing factual claim (a statistic, a quote, "
    "an outcome) that the briefing would be repeating as fact\n"
    "- it seems ambiguous, stale, or possibly outdated\n\n"
    "Most well-sourced, unambiguous, recent stories do NOT need verification — "
    "do not call web_search for those; go straight to a decision.\n\n"
    "If you decide NOT to verify, respond with nothing but this JSON object: "
    '{"verdict": "no_verification_needed", "reasoning": "<one short sentence>"}\n\n'
    "If you call web_search, you will be given the results in a follow-up "
    "message. After reviewing them, respond with nothing but this JSON object: "
    '{"verdict": "verified" | "caveat" | "drop", '
    '"caveat_text": "<only if verdict is caveat, one short sentence to append to '
    'the summary>", "reasoning": "<one short sentence on what the search showed>"}'
)

WEB_SEARCH_TOOL: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "web_search",
        "description": (
            "Search the public web for current information to verify a specific "
            "factual claim, statistic, quote, or outcome in a candidate news story."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A concise search query targeting the specific claim to verify.",
                }
            },
            "required": ["query"],
        },
    },
}


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


def collect_selected_articles(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the selected articles grouped by issue area into one list."""
    selected_by_issue = payload.get("selected_by_issue_area", {})
    selected_articles: list[dict[str, Any]] = []

    for issue_area, issue_articles in selected_by_issue.items():
        for article in issue_articles:
            article_with_issue = dict(article)
            article_with_issue["issue_area"] = issue_area
            selected_articles.append(article_with_issue)

    return selected_articles


def build_user_prompt(article: dict[str, Any]) -> str:
    """Build the article-specific verification prompt."""
    article_text = str(article.get("article_text") or "")[:8000]
    payload = {
        "title": article.get("title"),
        "source_name": article.get("source_name"),
        "source_level": article.get("source_level"),
        "published_at": article.get("published_at"),
        "url": article.get("url"),
        "article_text": article_text,
    }
    return f"Candidate story:\n{json.dumps(payload, ensure_ascii=True)}"


def parse_llm_json(content: str) -> dict[str, Any] | None:
    """Parse the model response as JSON, tolerating fenced code blocks."""
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1] if cleaned.count("```") >= 2 else cleaned
        cleaned = cleaned.removeprefix("json").strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def perform_web_search(
    query: str,
    api_key: str | None,
    base_url: str,
    max_results: int,
    timeout: int,
) -> str:
    """Run a real web search and return a plain-text digest of the results.

    Returns a clear "unavailable" message instead of raising when no search
    API key is configured, so the model can still reach a verdict using its
    own judgment rather than the pipeline crashing on a missing credential.
    """
    if not api_key:
        return (
            "Web search is not configured (missing API key). No external "
            "results are available; use your own judgment based on the "
            "article text and source alone."
        )

    endpoint = base_url.rstrip("/") + "/search"
    try:
        response = requests.post(
            endpoint,
            json={"api_key": api_key, "query": query, "max_results": max_results},
            timeout=timeout,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
    except requests.RequestException as exc:
        LOGGER.warning("Web search request failed for query %r: %s", query, exc)
        return f"Web search failed ({exc}). Use your own judgment based on the article text and source alone."

    if not results:
        return "Web search returned no results."

    lines = []
    for result in results[:max_results]:
        title = result.get("title", "")
        content = result.get("content", "")
        url = result.get("url", "")
        lines.append(f"- {title}: {content} ({url})")
    return "\n".join(lines)


def call_chat_api(
    messages: list[dict[str, Any]],
    api_key: str,
    base_url: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    tools: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Call the OpenAI-compatible chat-completions endpoint and return the message."""
    endpoint = base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload: dict[str, Any] = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    response = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
    response.raise_for_status()
    response_payload = response.json()
    return response_payload.get("choices", [{}])[0].get("message", {}) or {}


def verify_article(
    article: dict[str, Any],
    provider: str,
    api_key: str | None,
    base_url: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    search_api_key: str | None,
    search_base_url: str,
    max_search_results: int,
) -> dict[str, Any]:
    """Ask the model to decide, per article, whether verification is needed."""
    title = article.get("title")
    url = article.get("url")

    base_record = {
        "article_title": title,
        "article_url": url,
        "verification_triggered": False,
        "search_query": None,
        "search_summary": None,
        "verdict": "no_verification_needed",
        "caveat_text": None,
        "reasoning": None,
        "verified_at": datetime.now(UTC).isoformat(),
    }

    if provider not in ("huggingface", "openai", "cerebras") or not api_key:
        base_record["reasoning"] = "verification_skipped:missing_api_key_or_unsupported_provider"
        LOGGER.info("Verification skipped for %r: no API key/unsupported provider", title)
        return base_record

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_prompt(article)},
    ]

    try:
        first_message = call_chat_api(
            messages=messages,
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            tools=[WEB_SEARCH_TOOL],
        )
        tool_calls = first_message.get("tool_calls") or []

        if not tool_calls:
            decision = parse_llm_json(str(first_message.get("content") or ""))
            reasoning = (decision or {}).get("reasoning") or "no_tool_call_returned"
            LOGGER.info("Verification not triggered for %r: %s", title, reasoning)
            base_record["reasoning"] = reasoning
            return base_record

        tool_call = tool_calls[0]
        arguments = json.loads(tool_call.get("function", {}).get("arguments") or "{}")
        query = str(arguments.get("query") or title or "")
        LOGGER.info("Verification triggered for %r: calling web_search(query=%r)", title, query)

        search_summary = perform_web_search(
            query=query,
            api_key=search_api_key,
            base_url=search_base_url,
            max_results=max_search_results,
            timeout=timeout,
        )

        messages.append({"role": "assistant", "content": first_message.get("content"), "tool_calls": tool_calls})
        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call.get("id"),
                "content": search_summary,
            }
        )

        second_message = call_chat_api(
            messages=messages,
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
        decision = parse_llm_json(str(second_message.get("content") or "")) or {}
        verdict = str(decision.get("verdict") or "verified")
        reasoning = decision.get("reasoning") or "verification_completed"

        LOGGER.info("Verification verdict for %r: %s (%s)", title, verdict, reasoning)

        base_record.update(
            {
                "verification_triggered": True,
                "search_query": query,
                "search_summary": search_summary,
                "verdict": verdict,
                "caveat_text": decision.get("caveat_text"),
                "reasoning": reasoning,
            }
        )
        return base_record
    except requests.RequestException as exc:
        LOGGER.warning("Verification request failed for %r: %s", title, exc)
        base_record["reasoning"] = f"verification_request_failed:{exc}"
        return base_record
    except Exception as exc:  # pragma: no cover - defensive runtime guard.
        LOGGER.exception("Unexpected verification failure for %r: %s", title, exc)
        base_record["reasoning"] = f"unexpected_error:{exc}"
        return base_record


def verify_articles(
    articles: list[dict[str, Any]],
    provider: str,
    api_key: str | None,
    base_url: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    search_api_key: str | None,
    search_base_url: str,
    max_search_results: int,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """Verify each selected article and group survivors back by issue area."""
    selected_by_issue: dict[str, list[dict[str, Any]]] = {}
    stats = {
        "input_articles": len(articles),
        "verification_triggered": 0,
        "no_verification_needed": 0,
        "verified": 0,
        "caveat_added": 0,
        "dropped": 0,
    }

    for article in articles:
        record = verify_article(
            article=article,
            provider=provider,
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            search_api_key=search_api_key,
            search_base_url=search_base_url,
            max_search_results=max_search_results,
        )

        if record["verification_triggered"]:
            stats["verification_triggered"] += 1
        verdict = record["verdict"]
        if verdict == "drop":
            stats["dropped"] += 1
            continue
        if verdict == "caveat":
            stats["caveat_added"] += 1
        elif verdict == "verified":
            stats["verified"] += 1
        else:
            stats["no_verification_needed"] += 1

        issue_area = article.get("issue_area", "uncategorized")
        verified_article = dict(article)
        verified_article["verification"] = record
        if record.get("caveat_text"):
            verified_article["verification_caveat"] = record["caveat_text"]
        selected_by_issue.setdefault(issue_area, []).append(verified_article)

    return selected_by_issue, stats


def write_verified_json(
    selected_by_issue: dict[str, list[dict[str, Any]]],
    stats: dict[str, int],
    output_dir: Path,
    run_time: datetime,
    source_file: Path,
    output_prefix: str = DEFAULT_OUTPUT_PREFIX,
) -> Path:
    """Write verification results to a dated JSON file."""
    output_path = output_dir / f"{output_prefix}_{run_time.date().isoformat()}.json"
    payload = {
        "generated_at": run_time.isoformat(),
        "source_file": str(source_file),
        "verification_stats": stats,
        "selected_by_issue_area": selected_by_issue,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--input-file", type=Path, default=None)
    parser.add_argument("--date", dest="run_date", default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--provider", default=DEFAULT_PROVIDER)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--api-key", default=DEFAULT_API_KEY)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--search-api-key", default=DEFAULT_SEARCH_API_KEY)
    parser.add_argument("--search-base-url", default=DEFAULT_SEARCH_BASE_URL)
    parser.add_argument("--max-search-results", type=int, default=DEFAULT_MAX_SEARCH_RESULTS)
    parser.add_argument("--verbose", action="store_true")
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
    selected_by_issue, stats = verify_articles(
        articles=selected_articles,
        provider=args.provider,
        api_key=str(args.api_key) if args.api_key else None,
        base_url=args.api_base,
        model=args.model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout=args.timeout,
        search_api_key=str(args.search_api_key) if args.search_api_key else None,
        search_base_url=args.search_base_url,
        max_search_results=args.max_search_results,
    )
    output_path = write_verified_json(
        selected_by_issue=selected_by_issue,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    LOGGER.info(
        "Wrote verified articles to %s (%s/%s triggered verification, %s dropped)",
        output_path,
        stats["verification_triggered"],
        stats["input_articles"],
        stats["dropped"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
