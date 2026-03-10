# Automated Daily News Briefing

Automated Daily News Briefing is a Python pipeline for collecting news, extracting article text, cleaning and ranking coverage, generating concise article summaries, and building a final spoken-style daily briefing transcript.

The project is designed as a sequence of small, focused stages. Each stage reads structured output from the previous one, writes its own dated artifacts, and can be run on its own or as part of the full pipeline.

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

8. `generate_tts`
   [Future] stage for turning the transcript into audio.

9. `deliver_report`
   [Future] stage for sending or publishing the final outputs.

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

Environment variables are used for API keys and delivery credentials. Put those in your shell environment or a local `.env` loader workflow if you use one.

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
```

Each stage writes dated JSON files so runs are easy to inspect and rerun.

## Dependencies

The code uses standard Python plus a few common libraries, including:

- `requests`
- `feedparser`
- `beautifulsoup4`
- `trafilatura` (optional, for stronger article extraction)

Some stages also expect access to an LLM API if you want generated summaries instead of fallback summaries.

## Current status

What is implemented now:

- source fetching
- article extraction
- dedupe
- tagging
- ranking
- article summarization
- final briefing transcript assembly
- pipeline orchestration

Future-facing:

- text-to-speech generation
- external report delivery
- deeper LLM-based classification and synthesis
- more source-specific extraction rules