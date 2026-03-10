"""Run the full Automated Daily News Briefing pipeline.

This orchestration script executes each pipeline stage in order, records stage
timings and output artifacts, and writes a run summary for inspection. The
pipeline is resilient around per-article failures inside stage implementations
and treats text-to-speech as optional so transcript delivery can still succeed.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import build_briefing
import clean_and_dedupe
import extract_articles
import fetch_sources
import rank_articles
import summarize_articles
import tag_articles


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
RUN_SUMMARY_DIR = PROJECT_ROOT / "outputs" / "reports" / "pipeline_runs"
STAGE_DATE_FORMAT = "%Y-%m-%d"

LOGGER = logging.getLogger("run_pipeline")


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def iso_now() -> str:
    """Return the current UTC timestamp as ISO-8601."""
    return datetime.now(UTC).isoformat()


def parse_run_datetime(run_date: str) -> datetime:
    """Convert the CLI run date into a UTC datetime used for stage file names."""
    parsed = datetime.strptime(run_date, STAGE_DATE_FORMAT)
    return parsed.replace(tzinfo=UTC)


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    """Write a JSON payload to disk with stable formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def resolve_optional_module(module_name: str) -> Any | None:
    """Try to import an optional pipeline module and return ``None`` if missing."""
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        return None


def run_fetch_sources(run_date: str, run_time: datetime) -> dict[str, Any]:
    """Run the candidate-fetch stage and return artifact metadata."""
    output_dir = fetch_sources.resolve_output_dir(fetch_sources.DEFAULT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    articles = fetch_sources.collect_articles(timeout=fetch_sources.DEFAULT_TIMEOUT_SECONDS)
    output_path = fetch_sources.write_articles_json(
        articles=articles,
        output_dir=output_dir,
        run_time=run_time,
    )
    return {
        "article_count": len(articles),
        "output_file": str(output_path),
        "run_date": run_date,
    }


def run_extract_articles(run_date: str, run_time: datetime) -> dict[str, Any]:
    """Run the article-extraction stage and return artifact metadata."""
    candidate_path = extract_articles.resolve_candidate_file(
        input_dir=extract_articles.DEFAULT_INPUT_DIR,
        run_date=run_date,
    )
    payload = extract_articles.load_candidate_payload(candidate_path)
    candidates = list(payload.get("articles", []))

    output_dir = extract_articles.DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    articles = extract_articles.extract_articles(
        candidates=candidates,
        timeout=extract_articles.DEFAULT_TIMEOUT_SECONDS,
        min_text_words=extract_articles.DEFAULT_MIN_TEXT_WORDS,
    )
    output_path = extract_articles.write_articles_json(
        articles=articles,
        output_dir=output_dir,
        run_time=run_time,
        source_file=candidate_path,
    )
    return {
        "input_file": str(candidate_path),
        "candidate_count": len(candidates),
        "article_count": len(articles),
        "output_file": str(output_path),
    }


def run_clean_and_dedupe(run_date: str, run_time: datetime) -> dict[str, Any]:
    """Run the clean-and-dedupe stage and return artifact metadata."""
    input_path = clean_and_dedupe.resolve_input_file(
        input_dir=clean_and_dedupe.DEFAULT_INPUT_DIR,
        run_date=run_date,
    )
    payload = clean_and_dedupe.load_article_payload(input_path)
    articles = list(payload.get("articles", []))

    output_dir = clean_and_dedupe.DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    cleaned_articles, stats = clean_and_dedupe.clean_and_dedupe_articles(articles)
    output_path = clean_and_dedupe.write_deduped_json(
        articles=cleaned_articles,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    return {
        "input_file": str(input_path),
        "output_file": str(output_path),
        "dedupe_stats": stats,
    }


def run_tag_articles(run_date: str, run_time: datetime) -> dict[str, Any]:
    """Run the tagging stage and return artifact metadata."""
    input_path = tag_articles.resolve_input_file(
        input_dir=tag_articles.DEFAULT_INPUT_DIR,
        run_date=run_date,
    )
    payload = tag_articles.load_payload(input_path)
    articles = list(payload.get("articles", []))

    output_dir = tag_articles.DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    tagged_articles, stats = tag_articles.tag_articles(articles)
    output_path = tag_articles.write_tagged_json(
        articles=tagged_articles,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    return {
        "input_file": str(input_path),
        "output_file": str(output_path),
        "tagging_stats": stats,
    }


def run_rank_articles(run_date: str, run_time: datetime) -> dict[str, Any]:
    """Run the ranking stage and return artifact metadata."""
    input_path = rank_articles.resolve_input_file(
        input_dir=rank_articles.DEFAULT_INPUT_DIR,
        run_date=run_date,
    )
    payload = rank_articles.load_payload(input_path)
    articles = list(payload.get("articles", []))

    output_dir = rank_articles.DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    ranked_by_issue, selected_by_issue, stats = rank_articles.rank_articles(articles)
    output_path = rank_articles.write_ranked_json(
        ranked_by_issue=ranked_by_issue,
        selected_by_issue=selected_by_issue,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    return {
        "input_file": str(input_path),
        "output_file": str(output_path),
        "ranking_stats": stats,
    }


def run_summarize_articles(run_date: str, run_time: datetime) -> dict[str, Any]:
    """Run the per-article summarization stage and return artifact metadata."""
    input_path = summarize_articles.resolve_input_file(
        input_dir=summarize_articles.DEFAULT_INPUT_DIR,
        run_date=run_date,
    )
    payload = summarize_articles.load_payload(input_path)
    selected_articles = summarize_articles.collect_selected_articles(payload)

    output_dir = summarize_articles.DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries, stats = summarize_articles.summarize_articles(
        articles=selected_articles,
        provider=summarize_articles.DEFAULT_PROVIDER,
        api_key=str(summarize_articles.DEFAULT_API_KEY) if summarize_articles.DEFAULT_API_KEY else None,
        base_url=summarize_articles.DEFAULT_API_BASE,
        model=summarize_articles.DEFAULT_MODEL,
        temperature=summarize_articles.DEFAULT_TEMPERATURE,
        max_tokens=summarize_articles.DEFAULT_MAX_TOKENS,
        timeout=summarize_articles.DEFAULT_TIMEOUT_SECONDS,
        target_summary_words=summarize_articles.DEFAULT_TARGET_SUMMARY_WORDS,
    )
    output_path = summarize_articles.write_summaries_json(
        summaries=summaries,
        stats=stats,
        output_dir=output_dir,
        run_time=run_time,
        source_file=input_path,
    )
    return {
        "input_file": str(input_path),
        "output_file": str(output_path),
        "summary_stats": stats,
    }


def run_build_briefing(run_date: str, run_time: datetime) -> dict[str, Any]:
    """Run the final transcript-building stage and return artifact metadata."""
    input_path = build_briefing.resolve_input_file(
        input_dir=build_briefing.DEFAULT_INPUT_DIR,
        run_date=run_date,
    )
    payload = build_briefing.load_payload(input_path)
    summaries = list(payload.get("summaries", []))

    output_dir = build_briefing.DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    transcript_text, transcript_markdown, stats = build_briefing.build_briefing(
        summaries=summaries,
        max_listen_minutes=build_briefing.DEFAULT_MAX_LISTEN_MINUTES,
    )
    txt_path, md_path = build_briefing.write_report_files(
        transcript_text=transcript_text,
        transcript_markdown=transcript_markdown,
        output_dir=output_dir,
        run_time=run_time,
    )
    return {
        "input_file": str(input_path),
        "txt_report": str(txt_path),
        "markdown_report": str(md_path),
        "briefing_stats": stats,
    }


def run_generate_tts(run_date: str) -> dict[str, Any]:
    """Run the optional text-to-speech stage when available."""
    module = resolve_optional_module("generate_tts")
    if module is None:
        LOGGER.warning("Optional stage generate_tts is not available; skipping")
        return {
            "status": "skipped",
            "reason": "module_not_found",
            "run_date": run_date,
        }

    if not hasattr(module, "main"):
        return {
            "status": "skipped",
            "reason": "missing_main_entrypoint",
            "run_date": run_date,
        }

    # The TTS stage is treated as best-effort. Any failure should not block
    # transcript delivery later in the pipeline.
    try:
        result = module.main()
        return {
            "status": "completed",
            "result": result,
            "run_date": run_date,
        }
    except Exception as exc:  # pragma: no cover - optional integration guard.
        LOGGER.exception("Optional TTS stage failed: %s", exc)
        return {
            "status": "failed",
            "reason": str(exc),
            "run_date": run_date,
        }


def run_deliver_report(run_date: str, briefing_artifacts: dict[str, Any], tts_artifacts: dict[str, Any]) -> dict[str, Any]:
    """Run the optional delivery stage, or fall back to local transcript delivery."""
    module = resolve_optional_module("deliver_report")
    if module is not None and hasattr(module, "main"):
        try:
            result = module.main()
            return {
                "status": "completed",
                "result": result,
                "run_date": run_date,
            }
        except Exception as exc:  # pragma: no cover - optional integration guard.
            LOGGER.exception("Optional delivery stage failed: %s", exc)
            return {
                "status": "failed",
                "reason": str(exc),
                "run_date": run_date,
                "delivered_artifacts": {
                    "txt_report": briefing_artifacts.get("txt_report"),
                    "markdown_report": briefing_artifacts.get("markdown_report"),
                    "tts_status": tts_artifacts.get("status"),
                },
            }

    # Local fallback delivery means the transcript artifacts were built and are
    # available on disk even if no remote delivery backend is implemented yet.
    return {
        "status": "completed",
        "delivery_mode": "local_artifacts_only",
        "run_date": run_date,
        "delivered_artifacts": {
            "txt_report": briefing_artifacts.get("txt_report"),
            "markdown_report": briefing_artifacts.get("markdown_report"),
            "tts_status": tts_artifacts.get("status"),
        },
    }


def run_stage(
    stage_name: str,
    stage_func: Callable[..., dict[str, Any]],
    *args: Any,
) -> dict[str, Any]:
    """Run one stage and capture timing, status, and artifacts."""
    started_at = iso_now()
    LOGGER.info("Starting stage: %s", stage_name)

    try:
        artifacts = stage_func(*args)
        stage_status = str(artifacts.get("status") or "completed")
        ended_at = iso_now()
        LOGGER.info("Finished stage: %s (%s)", stage_name, stage_status)
        return {
            "stage": stage_name,
            "status": stage_status,
            "started_at": started_at,
            "ended_at": ended_at,
            "artifacts": artifacts,
        }
    except Exception as exc:
        ended_at = iso_now()
        LOGGER.exception("Stage failed: %s", stage_name)
        return {
            "stage": stage_name,
            "status": "failed",
            "started_at": started_at,
            "ended_at": ended_at,
            "error": str(exc),
            "artifacts": {},
        }


def stage_failed(stage_result: dict[str, Any]) -> bool:
    """Return whether a stage result represents a hard failure."""
    return stage_result.get("status") == "failed"


def build_run_summary(stage_results: list[dict[str, Any]], started_at: str, ended_at: str) -> dict[str, Any]:
    """Build the top-level run summary payload."""
    pipeline_status = "completed"
    for result in stage_results:
        if result["status"] == "failed" and result["stage"] not in {"generate_tts"}:
            pipeline_status = "failed"
            break

    return {
        "pipeline_status": pipeline_status,
        "started_at": started_at,
        "ended_at": ended_at,
        "stages": stage_results,
    }


def write_run_summary(summary: dict[str, Any], run_id: str) -> Path:
    """Write the pipeline run summary to disk."""
    summary_path = RUN_SUMMARY_DIR / f"pipeline_run_{run_id}.json"
    write_json_file(summary_path, summary)
    return summary_path


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        dest="run_date",
        default=datetime.now(UTC).strftime(STAGE_DATE_FORMAT),
        help="Pipeline run date in YYYY-MM-DD format",
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

    run_started_at = iso_now()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_date = args.run_date
    stage_results: list[dict[str, Any]] = []

    stage_run_time = parse_run_datetime(run_date)

    fetch_result = run_stage("fetch_sources", run_fetch_sources, run_date, stage_run_time)
    stage_results.append(fetch_result)
    if stage_failed(fetch_result):
        run_ended_at = iso_now()
        summary = build_run_summary(stage_results, run_started_at, run_ended_at)
        summary_path = write_run_summary(summary, run_id)
        LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
        LOGGER.info("Run summary written to %s", summary_path)
        return 1

    extract_result = run_stage("extract_articles", run_extract_articles, run_date, stage_run_time)
    stage_results.append(extract_result)
    if stage_failed(extract_result):
        run_ended_at = iso_now()
        summary = build_run_summary(stage_results, run_started_at, run_ended_at)
        summary_path = write_run_summary(summary, run_id)
        LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
        LOGGER.info("Run summary written to %s", summary_path)
        return 1

    dedupe_result = run_stage("clean_and_dedupe", run_clean_and_dedupe, run_date, stage_run_time)
    stage_results.append(dedupe_result)
    if stage_failed(dedupe_result):
        run_ended_at = iso_now()
        summary = build_run_summary(stage_results, run_started_at, run_ended_at)
        summary_path = write_run_summary(summary, run_id)
        LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
        LOGGER.info("Run summary written to %s", summary_path)
        return 1

    tag_result = run_stage("tag_articles", run_tag_articles, run_date, stage_run_time)
    stage_results.append(tag_result)
    if stage_failed(tag_result):
        run_ended_at = iso_now()
        summary = build_run_summary(stage_results, run_started_at, run_ended_at)
        summary_path = write_run_summary(summary, run_id)
        LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
        LOGGER.info("Run summary written to %s", summary_path)
        return 1

    rank_result = run_stage("rank_articles", run_rank_articles, run_date, stage_run_time)
    stage_results.append(rank_result)
    if stage_failed(rank_result):
        run_ended_at = iso_now()
        summary = build_run_summary(stage_results, run_started_at, run_ended_at)
        summary_path = write_run_summary(summary, run_id)
        LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
        LOGGER.info("Run summary written to %s", summary_path)
        return 1

    summarize_result = run_stage("summarize_articles", run_summarize_articles, run_date, stage_run_time)
    stage_results.append(summarize_result)
    if stage_failed(summarize_result):
        run_ended_at = iso_now()
        summary = build_run_summary(stage_results, run_started_at, run_ended_at)
        summary_path = write_run_summary(summary, run_id)
        LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
        LOGGER.info("Run summary written to %s", summary_path)
        return 1

    briefing_result = run_stage("build_briefing", run_build_briefing, run_date, stage_run_time)
    stage_results.append(briefing_result)
    if stage_failed(briefing_result):
        run_ended_at = iso_now()
        summary = build_run_summary(stage_results, run_started_at, run_ended_at)
        summary_path = write_run_summary(summary, run_id)
        LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
        LOGGER.info("Run summary written to %s", summary_path)
        return 1

    tts_result = run_stage(
        "generate_tts",
        run_generate_tts,
        run_date,
    )
    stage_results.append(tts_result)

    delivery_result = run_stage(
        "deliver_report",
        run_deliver_report,
        run_date,
        briefing_result.get("artifacts", {}),
        tts_result.get("artifacts", {}),
    )
    stage_results.append(delivery_result)

    run_ended_at = iso_now()
    summary = build_run_summary(stage_results, run_started_at, run_ended_at)
    summary_path = write_run_summary(summary, run_id)
    LOGGER.info("Pipeline finished with status %s", summary["pipeline_status"])
    LOGGER.info("Run summary written to %s", summary_path)
    return 0 if summary["pipeline_status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
