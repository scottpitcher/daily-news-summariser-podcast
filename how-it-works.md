# How It Works

A data-processing walkthrough of the Automated Daily News Briefing pipeline, built for the office of NYC Council Member Virginia Maloney (District 4, Manhattan).

Each stage reads structured JSON from the previous stage, transforms it, and writes a new dated artifact. Nothing is stateful across runs beyond those files.

---

## 1. Source collection

**Tech stack: `feedparser`, `requests`, `BeautifulSoup4`, `ThreadPoolExecutor` (5 workers)**

All configured sources are fetched concurrently. The pipeline currently runs local-only — national and state sources are disabled in `config.py`, leaving 15 active NYC outlets:

| Source | Type |
|---|---|
| NY Daily News | HTML scrape |
| CBS New York | HTML scrape |
| ABC7 New York | HTML scrape |
| amNewYork | HTML scrape |
| Gothamist | RSS |
| City Limits | HTML scrape |
| Crain's New York Business | HTML scrape |
| Politico New York Playbook | HTML scrape |
| Newsday | HTML scrape |
| PIX11 | HTML scrape |
| FOX 5 New York | HTML scrape |
| Our Town | HTML scrape |
| Patch Upper East Side | HTML scrape |
| THE CITY | HTML scrape |
| NY1 | HTML scrape |

Sources with an RSS feed are fetched via `feedparser` first; HTML scraping is used as a fallback or primary method when no feed is configured. For HTML sources, the scraper looks for semantic containers (`article`, `main article`, `.post`, `.story`, `.card`, `.headline`) before falling back to all `<a href>` links.

Each source is capped at 50 candidates to prevent any single outlet from flooding downstream stages.

After collection, three filters are applied in sequence:

**Recency filter** — articles with a `published_at` timestamp older than 24 hours are dropped. Articles with no timestamp (common for HTML-scraped pages that don't expose dates) are kept.

**Sports filter** — articles matching a URL path token (`/nfl/`, `/nba/`, `/mlb/`, etc.) or a title keyword regex are removed. The regex covers team names, league acronyms, and position names including all major NYC teams (Jets, Giants, Knicks, Yankees, Mets, Nets, Rangers, Islanders, Liberty).

**URL deduplication** — exact URLs seen more than once (after stripping fragments) are collapsed to first-seen.

The result is a dated JSON file at `data/raw/candidates/article_candidates_YYYY-MM-DD.json`.

---

## 2. Article text extraction

**Tech stack: `trafilatura`, `BeautifulSoup4`, `requests`, `ThreadPoolExecutor` (5 workers)**

The candidate URLs are visited concurrently. For each URL, the stage fetches the page and attempts to extract the main article body. `trafilatura` is the primary extractor when available; `BeautifulSoup4` acts as a fallback.

Each extracted record carries:
- `title` — the article headline
- `article_text` — full body text
- `url` — canonical URL
- `source_name`, `source_level`, `source_priority`
- `published_at`, `author`, `extracted_at`
- `extraction_method` — which path succeeded

Articles with too few words are discarded. Records are written to `data/raw/articles/articles_YYYY-MM-DD.json`.

---

## 3. Deduplication

**Tech stack: Python standard library (`difflib.SequenceMatcher`)**

Three passes run in order, each feeding into the next:

**Pass 1 — Exact URL match.** Records with identical normalized URLs are grouped. The "best" article in each group survives, chosen by a three-key quality tuple: body text length → source priority → metadata completeness score. This handles the case where the same URL appears in two scraped sources.

**Pass 2 — Title near-duplicate (threshold: 0.90).** Titles are normalized (lowercased, non-alphanumeric stripped, whitespace collapsed) and compared pairwise using `SequenceMatcher`. Articles whose titles score ≥ 0.90 similarity are clustered together; again the best-quality record wins. This collapses wire service reprintings of the same story under different headlines that still look nearly identical.

**Pass 3 — Body text near-duplicate (similarity: 0.88, token overlap: 0.75).** Full article bodies are compared. Both a `SequenceMatcher` ratio and a token overlap ratio (shared tokens / min token count) must meet their thresholds simultaneously before two articles are merged. This is a stricter gate than title similarity and primarily catches identical syndicated text appearing under different headlines.

After all passes, records are sorted stably by source priority descending, then body length descending, then title alphabetically. Output goes to `data/processed/deduped/deduped_articles_YYYY-MM-DD.json`.

---

## 4. Relevancy determination and issue-area tagging

**Tech stack: keyword matching (no external model)**

Tagging uses a local `KeywordRuleClassifier`. It does not call any LLM. For each article, the classifier:

1. Builds a search corpus from the normalized title + article body.
2. Scores each of 7 configured issue areas by counting keyword hits.
3. Assigns tags for every area that scored above zero, ranked by hit count.

The 7 issue areas and their seed keywords:

| Issue area | Priority | Article cap | Keywords |
|---|---|---|---|
| Politics and Government | High | 4 | congress, governor, mayor, legislation, election, public policy |
| Economy and Business | High | 3 | inflation, jobs, interest rates, markets, small business, trade |
| Public Safety | High | 3 | crime, policing, emergency response, wildfire, storm, disaster |
| Health | Medium | 2 | public health, hospitals, disease outbreak, mental health, medicaid |
| Education | Medium | 2 | schools, district, curriculum, college, student loan |
| Climate and Energy | Medium | 2 | climate, renewable energy, power grid, emissions, drought |
| Transportation and Housing | Medium | 2 | transit, housing, zoning, rent, infrastructure |

An article can hold multiple tags; the primary tag (highest-scoring) informs the ranking stage.

**Exclusion logic** runs after tagging. An article is marked `is_relevant = false` and excluded if it:
- Produces no matching issue-area tag.
- Contains ≥ 2 geopolitical exclusion terms (`ceasefire`, `missile strike`, `border conflict`, `nato`, `territorial dispute`, `diplomatic`, etc.) and zero domestic relevance terms (`city`, `housing`, `mayor`, `legislation`, `policy`, etc.).

This keeps the pipeline focused on NYC-consequential coverage without blocking, say, a federal immigration bill that uses the word "border."

Output: `data/processed/tagged/tagged_articles_YYYY-MM-DD.json`, which includes `issue_area_tags`, `is_relevant`, and `excluded_reason` on every record.

---

## 5. Ranking and story selection

**Tech stack: deterministic scoring (no external model), `difflib.SequenceMatcher`**

Only articles marked `is_relevant = true` participate. Each article is scored separately for each issue area it tagged into. The score is a weighted sum of eight components:

| Component | Weight | How it is calculated |
|---|---|---|
| `source_priority` | 0.20 | A per-source float from `SOURCE_PRIORITY_WEIGHTS` (e.g. Gothamist = 0.85). Falls back to the source-level default (local = 1.0, state = 0.85, national = 0.75) if the source is not in the table. |
| `issue_priority` | 0.20 | Position of this issue tag in the article's ranked tag list × the issue priority factor (high = 1.0, medium = 0.75). Primary tag scores highest. |
| `recency` | 0.20 | Linear decay: full score (1.0) if published within 24 h; zero if older than 168 h (7 days); articles with no timestamp get 0.2. |
| `local_relevance` | 0.10 | Source-level lookup: local = 1.0, state = 0.8, national = 0.6. |
| `title_signal` | 0.05 | Word count in the headline (best range: 5–18 words), minus a 0.2 penalty for vague terms like "live", "updates", "what to know", "photos", "video". |
| `content_quality` | 0.10 | Body word count / 1200 (capped at 1.0), plus 0.05 bonus each for `author` and `published_at` being present. |
| `cross_source_confirmation` | 0.05 | Number of other sources covering a similar headline (title similarity ≥ 0.75) / 3, capped at 1.0. Measures corroboration. |
| `maloney_relevance` | 0.10 | See section 6 below. |

A small **overlap penalty** (−0.05 × penalty score) is subtracted when a very similar headline already exists in the same issue area (similarity ≥ 0.92). This is intentionally minor — modest overlap is treated as corroboration, not redundancy.

Articles are ranked descending by final score within each issue area. The top N are selected, where N is the issue-specific article cap from the table above (4 for Politics, 3 for Economy/Safety, 2 for all others). Rankings and selected flags are preserved in the output JSON for auditability.

Output: `data/processed/ranked/ranked_articles_YYYY-MM-DD.json`, which includes the full `score_breakdown` (weights, component scores, penalties, final score) for every ranked article.

---

## 6. How the system knows about CM Maloney

**Tech stack: keyword matching (`MALONEY_OFFICE_KEYWORDS` in `config.py`)**

The `maloney_relevance` ranking component is a direct keyword search against CM Maloney's legislative portfolio. It is not prompted — it is a deterministic lookup built from her public committee assignments, caucus memberships, and district geography.

The keyword groups in `config.py`:

| Area | Example keywords |
|---|---|
| Sanitation and Solid Waste | sanitation, waste, trash, recycling, composting, landfill, dsny |
| Small Business | small business, storefront, commercial rent, business improvement district, merchant, vendor, bid |
| Finance | budget, fiscal, bonds, revenue, tax, deficit, comptroller, municipal finance, omb |
| Economic Development (Chair) | economic development, edc, nycedc, jobs program, workforce development, rezoning, commercial development |
| Fire and Emergency Management | fdny, fire department, firefighter, emergency management, oem, fire safety, emergency response |
| Higher Education | cuny, community college, higher education, tuition, university, campus |
| Cultural Affairs and Libraries | library, libraries, museum, arts, cultural affairs, nypl, cultural institution |
| Housing and Buildings | housing, buildings, dob, hpd, rent, tenant, landlord, affordable housing, building code, zoning, eviction, construction |
| Women's Caucus | women, gender, maternal, childcare, pay equity |
| Irish Caucus | irish, ireland |
| District 4 geography | upper east side, midtown east, turtle bay, murray hill, sutton place, district 4, manhattan |

The scoring function searches the combined title + body text for keywords in each group. Each group is counted once (presence, not frequency). The score scales with the number of matched groups:

- 0 groups matched → 0.0
- 1 group → 0.4
- 2 groups → 0.7
- 3 or more groups → 1.0

Two bonuses are applied on top:
- **+0.15** if the `economic_development` group matched (she chairs this committee)
- **+0.15** if the `district_4` group matched (direct constituent geography)

The result is capped at 1.0. This score feeds into the overall 8-component ranking at a 0.10 weight — enough to meaningfully lift portfolio-relevant stories but not enough to override strong source, recency, and corroboration signals.

The same portfolio knowledge is also baked into the LLM summarization prompt (see section 7).

---

## 7. Article summarization

**Tech stack: Cerebras API (`llama3.1-8b`), OpenAI-compatible `/chat/completions` endpoint, `requests`**

Only the selected articles from the ranking stage are summarized — typically 16–20 per run, depending on how many issue areas have qualifying content.

Each article is sent as a structured JSON payload to the Cerebras API (model: `llama3.1-8b`, temperature: 0.2, max tokens: 1200). The system prompt specifies CM Maloney's full committee list and caucus memberships and instructs the model to return only valid JSON:

```
headline   — a rewritten title, 5–10 words, not the original
bullets    — 1–2 key facts, each under 25 words
so_what    — 1 sentence, see below
```

**The `so_what` field** is the most tightly constrained part of the prompt. The model is instructed to:

- Name a specific committee action CM Maloney could take — a hearing, an oversight request, a budget question, or draft legislation.
- Name a concrete District 4 constituent impact if no committee link applies.
- Write exactly `"No direct committee or district connection identified."` if neither applies.

The prompt explicitly prohibits filler like "relevant to her portfolio" or "impacts local businesses." Stretching a story to an unrelated committee is treated as worse than admitting no connection.

The article text is truncated to 12,000 characters before being sent. The model response is parsed as JSON, with tolerance for fenced code blocks. If the model response is empty, malformed, or missing the headline field, the pipeline falls back to an extractive summary: the first 10 words of the article title become the headline, and the first 1–2 sentences of article text become the bullets.

Output: `data/processed/article_summaries/article_summaries_YYYY-MM-DD.json`.

---

## 8. Briefing assembly

**Tech stack: pure Python string formatting**

The briefing builder reads all summary records and assembles them into two formats simultaneously: plain text (for TTS) and Markdown (for the email).

**Ordering.** Issue areas are sorted by editorial priority (high → medium). Within each issue area, stories are sorted by source level (national → state → local). The current pipeline only has local sources active, so this ordering is dormant but preserved for when state/national sources are re-enabled.

**Connection filter.** Any summary whose `so_what` field contains the phrase "no direct committee or district connection identified" is silently dropped from the briefing. This is a second gate after ranking — the model had a chance to find a connection, said it couldn't, and the story is excluded.

**Word budget.** The transcript is trimmed to fit within `min(1400 words, 25 minutes × 155 wpm)`, which evaluates to 1400 words. Stories are added in priority order until the next story would exceed the budget, at which point the section is closed and remaining stories are skipped.

**Output format.** Each story block contains:
- A rewritten headline
- 1–2 bullet points
- A "So what?" line
- Source attribution

In plain text, attribution reads `According to [Source Name], via [level] coverage`. In Markdown, the original article headline is hyperlinked to the source URL and formatted as `[Headline](url) — Source Name`.

Both files are written to `outputs/reports/daily_briefing_YYYY-MM-DD.txt` and `outputs/reports/daily_briefing_YYYY-MM-DD.md`.

---

## 9. Text-to-speech

**Tech stack: OpenAI TTS API (`gpt-4o-mini-tts`), `openai` Python SDK**

The plain-text briefing is read and split into chunks of up to 4,000 characters. Splitting happens at sentence boundaries (`.`, `!`, `?` followed by whitespace) to avoid mid-sentence cuts in the audio.

Each chunk is sent separately to the OpenAI TTS API:
- Model: `gpt-4o-mini-tts`
- Voice: `alloy`
- Format: `mp3`
- Speed: `1.0`

The raw audio bytes from each chunk are concatenated in order into a single MP3 file. This means the chunk boundaries are seamless in the audio as long as the text is well-punctuated (which the summarization stage ensures).

The output file is written to `outputs/audio/daily_briefing_YYYY-MM-DD.mp3`. If the API key is missing or the request fails, the TTS stage logs the error and exits cleanly — it is treated as optional and will not block email delivery.

---

## 10. Delivery

**Tech stack: Python standard library (`smtplib`, `email.mime`), Gmail SMTP**

The email stage reads the Markdown briefing and converts it to styled HTML. The converter handles each line type individually:

- `## Heading` → styled `<h2>` with a dark-blue bottom border
- `**Bold text**` → `<h3>` story headline
- `**So what?** text` → paragraph with the "So what?" label in accent red
- `Source: [link](url)` → small grey attribution line with a styled hyperlink
- `- bullet` → `<li>` inside a `<ul>` block
- All other lines → paragraph text

The email is wrapped in a full HTML newsletter layout with a dark header bar, white body, and a light footer crediting Cornell Tech and CM Maloney's office.

If a matching MP3 exists in `outputs/audio/` for today's run date, it is attached to the email as a binary `audio/mp3` MIME part. The email is sent via Gmail SMTP with STARTTLS on port 587.

Recipients are configured via the `EMAIL_RECIPIENTS` environment variable (comma-separated). The pipeline runs on a GitHub Actions cron job at 5:00 AM ET on weekdays.
