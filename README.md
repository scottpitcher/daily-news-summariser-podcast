# Automated Daily News Briefing

Automated Daily News Briefing is a Python pipeline for collecting news, extracting article text, cleaning and ranking coverage, generating concise article summaries, and building a final spoken-style daily briefing transcript.

The project is designed as a sequence of small, focused stages. Each stage reads structured output from the previous one, writes its own dated artifacts, and can be run on its own or as part of the full pipeline.

**This project was completed by students at Cornell Tech in conjunction with the office of NYC CM Virginia Maloney**

## What the project does

At a high level, the pipeline:

- collects article candidates from configured news sources
- extracts full article text
- removes duplicates and low-value overlap
- tags articles by issue area
- ranks stories within each issue area
- summarizes the selected stories
- builds a final transcript for audio delivery
- leaves room for optional text-to-speech and delivery steps

The current implementation is built for a public-interest daily briefing, with issue areas such as politics and government, economy and business, public safety, health, education, climate and energy, and transportation and housing.

## Pipeline overview

The pipeline runs in this order:

1. `fetch_sources.py`
   Reads configured RSS feeds and simple source pages, collects candidate links, and saves metadata to `data/raw/candidates/`.

2. `extract_articles.py`
   Visits article URLs, extracts article text and metadata, and saves results to `data/raw/articles/`.

3. `clean_and_dedupe.py`
   Removes exact duplicates and near-duplicates, keeping the strongest version of overlapping stories, and saves output to `data/processed/deduped/`.

4. `tag_articles.py`
   Assigns issue-area tags, marks relevance, and excludes off-topic or mostly geopolitical coverage when appropriate. Output goes to `data/processed/tagged/`.

5. `rank_articles.py`
   Scores and ranks articles within each issue area using configurable weights from `config.py`, then selects top stories per issue. Output goes to `data/processed/ranked/`.

6. `summarize_articles.py`
   Generates short structured summaries for selected stories and saves them to `data/processed/article_summaries/`.

7. `build_briefing.py`
   Builds the final transcript in a spoken-friendly format and writes `.txt` and `.md` report files to `outputs/reports/`.

8. `generate_tts.py`
   Reads the plain-text briefing from `outputs/reports/`, splits it into chunks under the 4 000-character API limit, and synthesizes each chunk using the OpenAI TTS API (`gpt-4o-mini-tts` by default). The audio parts are concatenated and written as an MP3 to `outputs/audio/`. This stage is treated as optional — a failure here does not block email delivery. Requires `OPENAI_TTS_API_KEY`.

9. `deliver_report.py`
   Reads the Markdown briefing from `outputs/reports/`, converts it to a newsletter-style HTML email, and sends it via SMTP. If an audio file from stage 8 is present for the same date, it is attached to the email. Delivery is controlled by environment variables (`EMAIL_DELIVERY_ENABLED`, `SMTP_HOST`, `SMTP_USERNAME`, etc.) and the `DELIVERY` settings in `config.py`. When email delivery is disabled or unconfigured, the stage is skipped gracefully.

## Repository structure

```text
.
├── src/
│   ├── config.py
│   ├── prompts.py
│   ├── fetch_sources.py
│   ├── extract_articles.py
│   ├── clean_and_dedupe.py
│   ├── tag_articles.py
│   ├── rank_articles.py
│   ├── summarize_articles.py
│   ├── build_briefing.py
│   ├── generate_tts.py
│   ├── deliver_report.py
│   └── run_pipeline.py
├── data/
├── outputs/
└── README.md
```

## Configuration

The main project settings live in [`src/config.py`](/Users/scottpitcher/dev/daily-news-summariser-podcast/src/config.py).

That file defines:

- source registries for national, state, and local coverage
- issue areas and article caps
- ranking weights
- dedupe thresholds
- transcript and audio targets
- model settings for summarization and TTS
- delivery settings and environment-based secrets

Key environment variables:

| Variable | Stage | Description |
|---|---|---|
| `SUMMARIZATION_PROVIDER` | summarize | Provider name (`cerebras`, `openai`, `huggingface`). Default: `cerebras` |
| `SUMMARIZATION_API_KEY` | summarize | API key for the summarization provider |
| `SUMMARIZATION_BASE_URL` | summarize | Base URL for the summarization API. Default: `https://api.cerebras.ai/v1` |
| `SUMMARIZATION_MODEL` | summarize | Model name. Default: `llama3.1-8b` |
| `OPENAI_TTS_API_KEY` | generate_tts | OpenAI API key for text-to-speech synthesis |
| `TTS_MODEL` | generate_tts | TTS model. Default: `gpt-4o-mini-tts` |
| `TTS_VOICE` | generate_tts | Voice preset. Default: `alloy` |
| `EMAIL_DELIVERY_ENABLED` | deliver_report | Set to `true` to enable email sending |
| `SMTP_HOST` / `SMTP_PORT` | deliver_report | SMTP server and port |
| `SMTP_USERNAME` / `SMTP_PASSWORD` | deliver_report | SMTP credentials |
| `EMAIL_SENDER` | deliver_report | From address |
| `EMAIL_RECIPIENTS` | deliver_report | Comma-separated list of recipient addresses |

## How to run the pipeline

Run the full pipeline:

```bash
python3 src/run_pipeline.py
```

Run it for a specific date:

```bash
python3 src/run_pipeline.py --date 2026-03-10 --verbose
```

You can also run each stage directly if you want to inspect or debug one part of the pipeline:

```bash
python3 src/fetch_sources.py
python3 src/extract_articles.py
python3 src/clean_and_dedupe.py
python3 src/tag_articles.py
python3 src/rank_articles.py
python3 src/summarize_articles.py
python3 src/build_briefing.py
python3 src/generate_tts.py
python3 src/deliver_report.py
```

## Output flow

The data moves through the project in a simple staged flow:

```text
data/raw/candidates
-> data/raw/articles
-> data/processed/deduped
-> data/processed/tagged
-> data/processed/ranked
-> data/processed/article_summaries
-> outputs/reports
-> outputs/audio          (MP3 from generate_tts.py, optional)
```

Each stage writes dated JSON files so runs are easy to inspect and rerun.

## Dependencies

## Python version

The current codebase expects Python `3.11+`.

Why:

- several scripts use `datetime.UTC`, which is available in Python 3.11 and newer
- the code was syntax-checked in a newer Python environment during setup

If you want the safest path for running the project now, use Python `3.11` or newer for your virtual environment.

## Dependencies

The code uses standard Python plus a few common libraries, including:

- `requests`
- `feedparser`
- `beautifulsoup4`
- `trafilatura` (optional, for stronger article extraction)

Some stages also expect access to an LLM API if you want generated summaries instead of fallback summaries.

## Changelog

### Apr 6, 2026

**`fetch_sources.py`** Expediate fetch source gathering and limited to local data sources
- Concurrent source fetching (`ThreadPoolExecutor`, 5 workers)
- Sports content filter (URL path + title keyword matching)
- 48h recency filter to drop stale articles
- Per-source candidate cap (50) to prevent source flooding
- Improved HTML article container selectors
- Disabled national/state sources; pipeline now runs local-only

**`extract_articles.py`**
- Concurrent article extraction (`ThreadPoolExecutor`, 5 workers)

**`config.py`** Updated source weights and switched to Hugging Face
- Inverted source priority weights: local (1.0) > state (0.85) > national (0.75)
- Default summarization provider changed to `huggingface` with `Qwen/Qwen2.5-7B-Instruct`
- Renamed env vars from `OPENAI_API_KEY`/`OPENAI_BASE_URL` to `HF_API_TOKEN`/`HF_BASE_URL`
- Default base URL set to `https://router.huggingface.co/v1`

**`summarize_articles.py`** Updated provider logic for Hugging Face
- Accepts `"huggingface"` as a provider alongside `"openai"`
- Updated default model and base URL fallbacks

**`build_briefing.py`** Added linked headlines to Markdown output
- Markdown briefing now includes original article headline linked to source URL
- Source attribution formatted as `[Headline](url) — Source Name`

**`.github/workflows/daily_briefing.yml`** Automated daily pipeline
- Runs daily at 5:00 AM ET (9:00 UTC) with manual trigger support
- Email delivery of briefing to configured recipients
- Uses `HF_API_TOKEN` and `HF_BASE_URL` env vars

### May 2026

**`generate_tts.py`** Text-to-speech stage implemented
- Synthesizes the plain-text briefing to MP3 using the OpenAI TTS API (`gpt-4o-mini-tts`, voice `alloy`)
- Transcripts longer than 4 000 characters are split at sentence boundaries and concatenated into a single audio file
- Output written to `outputs/audio/daily_briefing_<date>.mp3`
- Configured via `OPENAI_TTS_API_KEY`, `TTS_MODEL`, `TTS_VOICE`, `TTS_AUDIO_FORMAT`, `TTS_SPEED`

**`deliver_report.py`** Audio attachment in email delivery
- If a matching audio file exists in `outputs/audio/`, it is attached to the outgoing email

**`config.py`** Switched summarization provider from HuggingFace to Cerebras
- Default provider changed to `cerebras` with model `llama3.1-8b`
- Default base URL changed to `https://api.cerebras.ai/v1`
- Env var for the API key changed to `SUMMARIZATION_API_KEY` (HF_API_TOKEN still accepted as fallback)

**`.github/workflows/daily_briefing.yml`** Updated to Cerebras
- Uses `SUMMARIZATION_PROVIDER=cerebras`, `CEREBRAS_API_KEY`, and `SUMMARIZATION_BASE_URL`

## Current status

What is implemented now:

- source fetching
- article extraction
- dedupe
- tagging
- ranking
- article summarization
- final briefing transcript assembly
- text-to-speech audio generation (OpenAI TTS, optional)
- pipeline orchestration
- HTML email delivery with optional MP3 attachment

Future-facing:

- deeper LLM-based classification and synthesis
- more source-specific extraction rules
