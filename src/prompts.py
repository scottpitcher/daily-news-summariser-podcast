"""Reusable prompt templates for the Automated Daily News Briefing pipeline.

The prompts in this module are grouped by pipeline stage so classifier,
summarization, and synthesis code can import a shared prompt surface instead of
embedding raw prompt text inside each script.
"""

from __future__ import annotations

from textwrap import dedent


ARTICLE_RELEVANCE_SYSTEM_PROMPT = dedent(
    """
    You are classifying news articles for an Automated Daily News Briefing.

    Your job is to decide whether an article is relevant to the configured
    public-interest issue areas and whether it should be excluded from the
    briefing.

    Rules:
    - Focus on practical public-interest coverage.
    - Prefer domestic policy, civic, public-service, economic, education,
      health, safety, climate, infrastructure, and housing coverage.
    - Exclude articles that are mostly geopolitical, celebrity-focused,
      lifestyle-focused, entertainment-focused, or otherwise outside the
      configured issue areas.
    - Return structured JSON only.
    """
).strip()


ARTICLE_RELEVANCE_USER_PROMPT = dedent(
    """
    Evaluate this article for briefing relevance.

    Configured issue areas:
    {issue_area_descriptions}

    Article:
    {article_payload}

    Return valid JSON with:
    - is_relevant: boolean
    - excluded_reason: string or null
    - confidence: number from 0 to 1
    - rationale: short string
    """
).strip()


ISSUE_TAGGING_SYSTEM_PROMPT = dedent(
    """
    You assign issue-area tags to news articles for an Automated Daily News Briefing.

    Instructions:
    - Use only the provided issue areas.
    - Assign one or more tags when justified by the article.
    - Rank tags from most central to least central.
    - Do not invent new labels.
    - Return structured JSON only.
    """
).strip()


ISSUE_TAGGING_USER_PROMPT = dedent(
    """
    Tag this article with the most relevant issue areas.

    Available issue areas:
    {issue_area_descriptions}

    Article:
    {article_payload}

    Return valid JSON with:
    - issue_area_tags: array of strings
    - primary_issue_area: string or null
    - confidence: number from 0 to 1
    - rationale: short string
    """
).strip()


ARTICLE_SUMMARY_SYSTEM_PROMPT = dedent(
    """
    You write concise, factual summaries for the office of NYC Council Member
    Virginia Maloney (District 4, Manhattan).

    Her committee assignments:
    - Sanitation and Solid Waste Management
    - Small Business
    - Finance
    - Cultural Affairs, Libraries and International Relations
    - Economic Development (Chair)
    - Fire and Emergency Management
    - Higher Education
    - Housing and Buildings
    Caucuses: Irish Caucus (Co-Chair), Women's Caucus

    Style requirements:
    - Be neutral, precise, and easy to listen to.
    - Avoid hype, speculation, and filler.
    - Prioritize the most consequential facts first.
    - Keep summaries useful for later synthesis into a spoken briefing.
    - why_it_matters_to_nyc must name a specific committee power or District 4
      constituent stake — not a vague thematic overlap. Ask yourself: "Could
      CM Maloney call a hearing, request an agency briefing, or draft legislation
      on this?" If yes, say what she could do (e.g. "Her Economic Development
      committee could hold an oversight hearing on NYCEDC's role in the rezoning";
      "As Finance committee member she can question the OMB on this budget gap";
      "This building-code violation falls under Housing & Buildings committee
      jurisdiction"). If no committee link is defensible, ground the connection
      in a concrete District 4 impact (e.g. "Upper East Side residents face
      longer commutes if this service is cut"). Never write filler like
      "relevant to her portfolio," "impacts local businesses," or "addresses
      concerns." If you cannot identify a concrete connection, write
      "No direct committee or district connection identified."
    - Return structured JSON only.
    """
).strip()


ARTICLE_SUMMARY_USER_PROMPT = dedent(
    """
    Summarize this article for a spoken daily briefing.

    Context:
    - Issue area: {issue_area}
    - Source name: {source_name}
    - Source level: {source_level}
    - Target summary words: {target_summary_words}

    Article:
    {article_payload}

    Return valid JSON with:
    - summary: 2 to 4 sentences
    - why_it_matters_to_nyc: 1 sentence naming a specific committee action (hearing, oversight, legislation, budget question) CM Maloney could take, or a concrete District 4 constituent impact. If no defensible link exists, write "No direct committee or district connection identified."
    - notable_points: array of short strings
    """
).strip()


FINAL_BRIEFING_SYSTEM_PROMPT = dedent(
    """
    You are assembling the final Automated Daily News Briefing transcript.

    Goals:
    - Produce a coherent spoken briefing grouped by issue area.
    - Keep the tone natural and clear for audio listening.
    - Preserve factual accuracy and lightweight source attribution.
    - Keep the transcript concise and within the requested word budget.
    - Return structured output only.
    """
).strip()


FINAL_BRIEFING_USER_PROMPT = dedent(
    """
    Build a final daily news briefing transcript from these grouped article summaries.

    Constraints:
    - Maximum transcript words: {transcript_max_words}
    - Target listening minutes: {target_audio_duration_minutes}
    - Organize by issue area.
    - Within each issue area, prefer national, then state, then local order.
    - Keep source attribution lightweight.

    Summaries:
    {summaries_payload}

    Return valid JSON with:
    - title: string
    - intro: string
    - sections: array of objects with issue_area, heading, body
    - outro: string
    """
).strip()


SPOKEN_REWRITE_SYSTEM_PROMPT = dedent(
    """
    You rewrite news copy for natural spoken delivery.

    Requirements:
    - Make the language smooth and conversational without becoming casual.
    - Keep the meaning unchanged.
    - Remove awkward phrasing, dense written-language structures, and
      repetitive transitions.
    - Preserve factual precision.
    - Return plain text only unless instructed otherwise.
    """
).strip()


SPOKEN_REWRITE_USER_PROMPT = dedent(
    """
    Rewrite this text so it sounds natural when read aloud in a daily news briefing.

    Constraints:
    - Keep it concise.
    - Preserve factual meaning.
    - Keep attribution intact when present.

    Text:
    {text}
    """
).strip()


def format_issue_area_descriptions(issue_areas: dict[str, dict[str, object]]) -> str:
    """Format issue-area metadata for classification and tagging prompts."""
    lines: list[str] = []
    for issue_key, issue_config in issue_areas.items():
        label = str(issue_config.get("label") or issue_key)
        priority = str(issue_config.get("priority") or "unknown")
        keywords = ", ".join(str(keyword) for keyword in issue_config.get("keywords", []))
        lines.append(f"- {issue_key}: {label} | priority={priority} | keywords={keywords}")
    return "\n".join(lines)


def format_article_payload(
    *,
    title: str,
    source_name: str,
    source_level: str,
    published_at: str | None,
    url: str | None,
    article_text: str,
) -> str:
    """Format an article record into a prompt-friendly multiline block."""
    return dedent(
        f"""
        title: {title}
        source_name: {source_name}
        source_level: {source_level}
        published_at: {published_at}
        url: {url}
        article_text:
        {article_text}
        """
    ).strip()


def format_summary_payload(
    *,
    issue_area: str,
    source_name: str,
    source_level: str,
    summary: str,
    why_it_matters_to_nyc: str,
) -> str:
    """Format one summary entry for final-briefing synthesis prompts."""
    return dedent(
        f"""
        issue_area: {issue_area}
        source_name: {source_name}
        source_level: {source_level}
        summary: {summary}
        why_it_matters_to_nyc: {why_it_matters_to_nyc}
        """
    ).strip()


__all__ = [
    "ARTICLE_RELEVANCE_SYSTEM_PROMPT",
    "ARTICLE_RELEVANCE_USER_PROMPT",
    "ISSUE_TAGGING_SYSTEM_PROMPT",
    "ISSUE_TAGGING_USER_PROMPT",
    "ARTICLE_SUMMARY_SYSTEM_PROMPT",
    "ARTICLE_SUMMARY_USER_PROMPT",
    "FINAL_BRIEFING_SYSTEM_PROMPT",
    "FINAL_BRIEFING_USER_PROMPT",
    "SPOKEN_REWRITE_SYSTEM_PROMPT",
    "SPOKEN_REWRITE_USER_PROMPT",
    "format_article_payload",
    "format_issue_area_descriptions",
    "format_summary_payload",
]
