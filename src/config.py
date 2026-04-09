"""Central configuration for the Automated Daily News Briefing project.

This module is designed to be imported by ingestion, summarization,
distribution, and maintenance scripts. Keep runtime settings here so the rest
of the codebase can treat configuration as read-only application state.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final


PROJECT_NAME: Final[str] = "Automated Daily News Briefing"
PROJECT_SLUG: Final[str] = "automated-daily-news-briefing"
BASE_DIR: Final[Path] = Path(__file__).resolve().parent


def _get_env(name: str, default: str | None = None) -> str | None:
    """Read an environment variable and treat blank values as unset."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _get_bool_env(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable using common deployment-friendly values."""
    value = _get_env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _get_float_env(name: str, default: float) -> float:
    """Parse a float environment variable with a safe default."""
    value = _get_env(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _get_int_env(name: str, default: int) -> int:
    """Parse an integer environment variable with a safe default."""
    value = _get_env(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _source(
    name: str,
    homepage_url: str,
    rss_url: str | None,
    level: str,
    source_type: str,
) -> dict[str, object]:
    """Create a normalized source record for scrapers and feed readers."""
    priority_weights = {
        "national": 0.75,
        "state": 0.85,
        "local": 1.0,
    }
    return {
        "name": name,
        "homepage_url": homepage_url,
        "rss_url": rss_url,
        "level": level,
        "type": source_type,
        "priority_weight": priority_weights.get(level, 0.7),
        "extraction": {
            "content_selector": None,
            "exclude_selectors": [],
            "date_selector": None,
            "follow_redirects": True,
            "render_js": False,
            "request_headers": {},
        },
    }

# Source registries are grouped by coverage level so downstream jobs can choose
# a national-only run or expand into state and local editions.
# NATIONAL_SOURCES: Final[list[dict[str, object]]] = [
#     _source(
#         name="Associated Press",
#         homepage_url="https://apnews.com/",
#         rss_url="https://apnews.com/hub/ap-top-news?output=rss",
#         level="national",
#         source_type="rss",
#     ),
#     _source(
#         name="Reuters World News",
#         homepage_url="https://www.reuters.com/world/",
#         rss_url="https://feeds.reuters.com/Reuters/worldNews",
#         level="national",
#         source_type="rss",
#     ),
#     _source(
#         name="NPR News",
#         homepage_url="https://www.npr.org/sections/news/",
#         rss_url="https://feeds.npr.org/1001/rss.xml",
#         level="national",
#         source_type="rss",
#     ),
#     _source(
#         name="PBS News",
#         homepage_url="https://www.pbs.org/newshour/",
#         rss_url="https://www.pbs.org/newshour/feeds/rss/headlines",
#         level="national",
#         source_type="rss",
#     ),
# ]


# STATE_SOURCES: Final[dict[str, list[dict[str, object]]]] = {
#     "new_york": [
#         _source(
#             name="New York State Government News",
#             homepage_url="https://www.governor.ny.gov/news",
#             rss_url=None,
#             level="state",
#             source_type="html",
#         ),
#         _source(
#             name="New York Public Radio",
#             homepage_url="https://www.wnyc.org/",
#             rss_url="https://www.wnyc.org/feeds/whatsnew/",
#             level="state",
#             source_type="rss",
#         ),
#     ],
#     "california": [
#         _source(
#             name="Office of the Governor of California",
#             homepage_url="https://www.gov.ca.gov/newsroom/",
#             rss_url=None,
#             level="state",
#             source_type="html",
#         ),
#         _source(
#             name="CalMatters",
#             homepage_url="https://calmatters.org/",
#             rss_url="https://calmatters.org/feed/",
#             level="state",
#             source_type="rss",
#         ),
#     ],
#     "texas": [
#         _source(
#             name="The Texas Tribune",
#             homepage_url="https://www.texastribune.org/",
#             rss_url="https://www.texastribune.org/feeds/articles/",
#             level="state",
#             source_type="rss",
#         ),
#         _source(
#             name="Office of the Texas Governor News",
#             homepage_url="https://gov.texas.gov/news",
#             rss_url=None,
#             level="state",
#             source_type="html",
#         ),
#     ],
# }


LOCAL_SOURCES: Final[dict[str, list[dict[str, object]]]] = {
    "new_york_city": [
        _source(
            name="The New York Times",
            homepage_url="https://www.nytimes.com/section/nyregion",
            rss_url="https://rss.nytimes.com/services/xml/rss/nyt/NYRegion.xml",
            level="local",
            source_type="rss",
        ),
        _source(
            name="New York Daily News",
            homepage_url="https://www.nydailynews.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="CBS New York",
            homepage_url="https://www.cbsnews.com/newyork/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="ABC7 New York",
            homepage_url="https://abc7ny.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="amNewYork",
            homepage_url="https://www.amny.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="Gothamist",
            homepage_url="https://gothamist.com/",
            rss_url="https://gothamist.com/feed",
            level="local",
            source_type="rss",
        ),
        _source(
            name="City Limits",
            homepage_url="https://citylimits.org/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="Crain's New York Business",
            homepage_url="https://www.crainsnewyork.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="Politico New York Playbook",
            homepage_url="https://www.politico.com/newsletters/new-york-playbook",
            rss_url=None,
            level="local",
            source_type="newsletter",
        ),
        _source(
            name="Newsday",
            homepage_url="https://www.newsday.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="PIX11",
            homepage_url="https://pix11.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="FOX 5 New York",
            homepage_url="https://www.fox5ny.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="Our Town",
            homepage_url="https://www.ourtownny.com/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="Patch Upper East Side",
            homepage_url="https://patch.com/new-york/upper-east-side-nyc",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="THE CITY",
            homepage_url="https://www.thecity.nyc/",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        _source(
            name="NY1",
            homepage_url="https://ny1.com/nyc/all-boroughs",
            rss_url=None,
            level="local",
            source_type="html",
        ),
        # _source(
        #     name="The Jewish Star",
        #     homepage_url="https://www.thejewishstar.com/",
        #     rss_url=None,
        #     level="local",
        #     source_type="html",
        # ),
    ],
}

# Issue areas drive filtering, clustering, and story-priority decisions after
# article collection. The keyword lists are intentionally broad seed terms.
ISSUE_AREAS: Final[dict[str, dict[str, object]]] = {
    "politics_government": {
        "label": "Politics and Government",
        "keywords": [
            "congress",
            "governor",
            "mayor",
            "legislation",
            "election",
            "public policy",
        ],
        "priority": "high",
        "article_cap": 4,
    },
    "economy_business": {
        "label": "Economy and Business",
        "keywords": [
            "inflation",
            "jobs",
            "interest rates",
            "markets",
            "small business",
            "trade",
        ],
        "priority": "high",
        "article_cap": 3,
    },
    "public_safety": {
        "label": "Public Safety",
        "keywords": [
            "crime",
            "policing",
            "emergency response",
            "wildfire",
            "storm",
            "disaster",
        ],
        "priority": "high",
        "article_cap": 3,
    },
    "health": {
        "label": "Health",
        "keywords": [
            "public health",
            "hospitals",
            "disease outbreak",
            "mental health",
            "medicaid",
        ],
        "priority": "medium",
        "article_cap": 2,
    },
    "education": {
        "label": "Education",
        "keywords": [
            "schools",
            "district",
            "curriculum",
            "college",
            "student loan",
        ],
        "priority": "medium",
        "article_cap": 2,
    },
    "climate_energy": {
        "label": "Climate and Energy",
        "keywords": [
            "climate",
            "renewable energy",
            "power grid",
            "emissions",
            "drought",
        ],
        "priority": "medium",
        "article_cap": 2,
    },
    "transportation_housing": {
        "label": "Transportation and Housing",
        "keywords": [
            "transit",
            "housing",
            "zoning",
            "rent",
            "infrastructure",
        ],
        "priority": "medium",
        "article_cap": 2,
    },
}

# Pipeline configuration keeps stage-level operational settings in one place so
# scripts can share defaults without hard-coding stage-specific behavior.
PIPELINE_STAGES: Final[dict[str, dict[str, object]]] = {
    "fetch_candidate_links": {
        "enabled": True,
        "output_dir": BASE_DIR / "data" / "raw_articles",
        "max_candidates_per_source": _get_int_env("FETCH_MAX_CANDIDATES_PER_SOURCE", 50),
        "request_timeout_seconds": _get_int_env("FETCH_TIMEOUT_SECONDS", 15),
    },
    "extract_full_article_text": {
        "enabled": True,
        "output_dir": BASE_DIR / "data" / "extracted_articles",
        "request_timeout_seconds": _get_int_env("EXTRACTION_TIMEOUT_SECONDS", 20),
        "respect_source_extraction_settings": True,
    },
    "clean_dedupe": {
        "enabled": True,
        "output_dir": BASE_DIR / "data" / "cleaned_articles",
    },
    "tag_by_issue_area": {
        "enabled": True,
        "output_dir": BASE_DIR / "data" / "tagged_articles",
    },
    "rank_select_stories": {
        "enabled": True,
        "output_dir": BASE_DIR / "data" / "ranked_articles",
    },
    "summarize_articles": {
        "enabled": True,
        "output_dir": BASE_DIR / "data" / "summaries",
    },
    "build_final_briefing": {
        "enabled": True,
        "output_dir": BASE_DIR / "output" / "reports",
    },
    "generate_tts": {
        "enabled": True,
        "output_dir": BASE_DIR / "output" / "audio",
    },
    "deliver_outputs": {
        "enabled": True,
        "output_dir": BASE_DIR / "output" / "delivery",
    },
}

# Ranking weights are designed to be composable across scripts. Each score
# component can be multiplied by the values here and summed into one rank score.
RANKING_WEIGHTS: Final[dict[str, float]] = {
    "source_priority": _get_float_env("RANK_WEIGHT_SOURCE_PRIORITY", 0.20),
    "issue_priority": _get_float_env("RANK_WEIGHT_ISSUE_PRIORITY", 0.20),
    "recency": _get_float_env("RANK_WEIGHT_RECENCY", 0.20),
    "local_relevance": _get_float_env("RANK_WEIGHT_LOCAL_RELEVANCE", 0.10),
    "title_signal": _get_float_env("RANK_WEIGHT_TITLE_SIGNAL", 0.05),
    "content_quality": _get_float_env("RANK_WEIGHT_CONTENT_QUALITY", 0.10),
    "cross_source_confirmation": _get_float_env("RANK_WEIGHT_CROSS_SOURCE_CONFIRMATION", 0.05),
    "maloney_relevance": _get_float_env("RANK_WEIGHT_MALONEY_RELEVANCE", 0.10),
}

# Source priority weights give the ranking stage a stable baseline per source
# while still allowing per-source overrides in the source definitions above.
SOURCE_PRIORITY_WEIGHTS: Final[dict[str, float]] = {
    "Associated Press": 1.0,
    "Reuters World News": 1.0,
    "NPR News": 0.95,
    "PBS News": 0.92,
    "New York State Government News": 0.84,
    "New York Public Radio": 0.86,
    "Office of the Governor of California": 0.84,
    "CalMatters": 0.9,
    "The Texas Tribune": 0.9,
    "Office of the Texas Governor News": 0.82,
    "NYC Mayor's Office": 0.8,
    "Gothamist": 0.85,
    "City of Los Angeles News": 0.8,
    "LAist": 0.85,
    "City of Austin News": 0.78,
    "Austin Monitor": 0.83,
}

# Keywords grouped by CM Virginia Maloney's committee assignments, caucuses,
# and district geography. The ranking stage uses these to score how closely an
# article connects to her legislative portfolio.
MALONEY_OFFICE_KEYWORDS: Final[dict[str, list[str]]] = {
    "sanitation_solid_waste": [
        "sanitation", "waste", "trash", "recycling", "garbage",
        "composting", "landfill", "dsny",
    ],
    "small_business": [
        "small business", "storefront", "commercial rent",
        "business improvement district", "merchant", "vendor", "bid",
    ],
    "finance": [
        "budget", "fiscal", "bonds", "revenue", "tax", "deficit",
        "comptroller", "municipal finance", "omb",
    ],
    "economic_development": [
        "economic development", "edc", "nycedc", "jobs program",
        "workforce development", "rezoning", "commercial development",
    ],
    "fire_emergency_management": [
        "fdny", "fire department", "firefighter", "emergency management",
        "oem", "fire safety", "emergency response",
    ],
    "higher_education": [
        "cuny", "community college", "higher education", "tuition",
        "university", "campus",
    ],
    "cultural_affairs_libraries": [
        "library", "libraries", "museum", "arts", "cultural affairs",
        "nypl", "cultural institution",
    ],
    "housing_buildings": [
        "housing", "buildings", "dob", "hpd", "rent", "tenant",
        "landlord", "affordable housing", "building code", "zoning",
        "eviction", "construction",
    ],
    "womens_caucus": [
        "women", "gender", "maternal", "childcare", "pay equity",
    ],
    "irish_caucus": [
        "irish", "ireland",
    ],
    "district_4": [
        "upper east side", "midtown east", "turtle bay", "murray hill",
        "sutton place", "district 4", "manhattan",
    ],
}

# A direct issue-area cap map makes selection logic simple even if a script does
# not want to read the full issue area definition structure.
ISSUE_AREA_ARTICLE_CAPS: Final[dict[str, int]] = {
    issue_key: int(issue_config["article_cap"])
    for issue_key, issue_config in ISSUE_AREAS.items()
}

# Transcript settings help summarization and TTS stay aligned around one target
# final product rather than drifting into mismatched text and audio lengths.
BRIEFING_OUTPUT: Final[dict[str, object]] = {
    "transcript_max_words": _get_int_env("TRANSCRIPT_MAX_WORDS", 1400),
    "target_audio_duration_minutes": _get_float_env("TARGET_AUDIO_DURATION_MINUTES", 9.0),
    "tts_words_per_minute": _get_int_env("TTS_WORDS_PER_MINUTE", 155),
    "max_story_count": _get_int_env("BRIEFING_MAX_STORY_COUNT", 10),
    "intro_template": _get_env(
        "BRIEFING_INTRO_TEMPLATE",
        "",
    ),
    "outro_template": _get_env(
        "BRIEFING_OUTRO_TEMPLATE",
        "That concludes today's briefing.",
    ),
}

# Dedupe settings separate exact URL/text matches from fuzzier semantic matches
# so clean/dedupe jobs can tune aggressiveness without changing code.
DEDUPE: Final[dict[str, object]] = {
    "exact_url_match": True,
    "exact_title_match": True,
    "normalize_query_params": True,
    "near_duplicate_similarity_threshold": _get_float_env(
        "NEAR_DUPLICATE_SIMILARITY_THRESHOLD",
        0.88,
    ),
    "min_token_overlap_threshold": _get_float_env(
        "MIN_TOKEN_OVERLAP_THRESHOLD",
        0.75,
    ),
    "title_similarity_threshold": _get_float_env(
        "TITLE_SIMILARITY_THRESHOLD",
        0.9,
    ),
}

# Model configuration is environment-driven so deployments can switch providers,
# model IDs, or token limits without changing application code.
MODELS: Final[dict[str, dict[str, object]]] = {
    "summarization": {
        "provider": _get_env("SUMMARIZATION_PROVIDER", "cerebras"),
        "model": _get_env("SUMMARIZATION_MODEL", "llama3.1-8b"),
        "temperature": float(_get_env("SUMMARIZATION_TEMPERATURE", "0.2")),
        "max_tokens": int(_get_env("SUMMARIZATION_MAX_TOKENS", "1200")),
        "api_key": _get_env("SUMMARIZATION_API_KEY") or _get_env("HF_API_TOKEN"),
        "base_url": _get_env("SUMMARIZATION_BASE_URL") or _get_env("HF_BASE_URL", "https://api.cerebras.ai/v1"),
        "target_summary_words": _get_int_env("TARGET_SUMMARY_WORDS", 120),
        "system_prompt_template": _get_env("SUMMARIZATION_SYSTEM_PROMPT"),
    },
    "text_to_speech": {
        "provider": _get_env("TTS_PROVIDER", "openai"),
        "model": _get_env("TTS_MODEL", "gpt-4o-mini-tts"),
        "voice": _get_env("TTS_VOICE", "alloy"),
        "audio_format": _get_env("TTS_AUDIO_FORMAT", "mp3"),
        "speed": float(_get_env("TTS_SPEED", "1.0")),
        "api_key": _get_env("HF_API_TOKEN"),
        "target_duration_minutes": BRIEFING_OUTPUT["target_audio_duration_minutes"],
    },
}

# Output paths stay relative to the repository root so local runs, cron jobs,
# and containerized jobs all write to predictable locations.
OUTPUT_DIRS: Final[dict[str, Path]] = {
    "raw_articles": BASE_DIR / "data" / "raw_articles",
    "extracted_articles": BASE_DIR / "data" / "extracted_articles",
    "cleaned_articles": BASE_DIR / "data" / "cleaned_articles",
    "tagged_articles": BASE_DIR / "data" / "tagged_articles",
    "ranked_articles": BASE_DIR / "data" / "ranked_articles",
    "summaries": BASE_DIR / "data" / "summaries",
    "processed_articles": BASE_DIR / "data" / "processed_articles",
    "reports": BASE_DIR / "output" / "reports",
    "audio": BASE_DIR / "output" / "audio",
    "delivery": BASE_DIR / "output" / "delivery",
    "logs": BASE_DIR / "output" / "logs",
}

# Delivery configuration supports multiple publication channels. Secrets remain
# in environment variables; this module only maps them into structured settings.
DELIVERY: Final[dict[str, dict[str, object]]] = {
    "email": {
        "enabled": _get_bool_env("EMAIL_DELIVERY_ENABLED", False),
        "smtp_host": _get_env("SMTP_HOST"),
        "smtp_port": int(_get_env("SMTP_PORT", "587")),
        "username": _get_env("SMTP_USERNAME"),
        "password": _get_env("SMTP_PASSWORD"),
        "sender": _get_env("EMAIL_SENDER"),
        "recipients": [
            email.strip()
            for email in _get_env("EMAIL_RECIPIENTS", "") .split(",")
            if email.strip()
        ],
        "use_tls": _get_bool_env("SMTP_USE_TLS", True),
    },
    "google_drive": {
        "enabled": _get_bool_env("GOOGLE_DRIVE_ENABLED", False),
        "folder_id": _get_env("GOOGLE_DRIVE_FOLDER_ID"),
        "service_account_file": _get_env("GOOGLE_SERVICE_ACCOUNT_FILE"),
        "service_account_json": _get_env("GOOGLE_SERVICE_ACCOUNT_JSON"),
    },
    "webhook": {
        "enabled": _get_bool_env("WEBHOOK_DELIVERY_ENABLED", False),
        "endpoint_url": _get_env("DELIVERY_WEBHOOK_URL"),
        "bearer_token": _get_env("DELIVERY_WEBHOOK_TOKEN"),
    },
}

# A small explicit secrets map helps validation code fail fast when required
# credentials are missing for a selected provider or delivery backend.
SECRETS: Final[dict[str, str | None]] = {
    "hf_api_token": _get_env("HF_API_TOKEN"),
    "smtp_password": _get_env("SMTP_PASSWORD"),
    "google_service_account_json": _get_env("GOOGLE_SERVICE_ACCOUNT_JSON"),
    "google_service_account_file": _get_env("GOOGLE_SERVICE_ACCOUNT_FILE"),
    "delivery_webhook_token": _get_env("DELIVERY_WEBHOOK_TOKEN"),
}


def ensure_output_dirs() -> None:
    """Create the configured filesystem layout before a run writes artifacts."""
    for directory in OUTPUT_DIRS.values():
        directory.mkdir(parents=True, exist_ok=True)


__all__ = [
    "BASE_DIR",
    "BRIEFING_OUTPUT",
    "DEDUPE",
    "DELIVERY",
    "ISSUE_AREAS",
    "ISSUE_AREA_ARTICLE_CAPS",
    "LOCAL_SOURCES",
    "MALONEY_OFFICE_KEYWORDS",
    "MODELS",
    # "NATIONAL_SOURCES",
    "OUTPUT_DIRS",
    "PIPELINE_STAGES",
    "PROJECT_NAME",
    "PROJECT_SLUG",
    "RANKING_WEIGHTS",
    "SECRETS",
    "SOURCE_PRIORITY_WEIGHTS",
    # "STATE_SOURCES",
    "ensure_output_dirs",
]
