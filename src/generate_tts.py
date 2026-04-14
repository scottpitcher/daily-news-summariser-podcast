"""Generate an MP3 audio file from the daily briefing transcript using OpenAI TTS.

This stage reads the latest plain-text briefing from ``outputs/reports/`` and
converts it to speech using the OpenAI TTS API. The resulting MP3 is written to
``outputs/audio/``. Long transcripts are split into chunks under the API's
4 000-character limit and the audio bytes are concatenated into one file.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config import MODELS, OUTPUT_DIRS

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_AUDIO_DIR = PROJECT_ROOT / "outputs" / "audio"
MAX_CHUNK_CHARS = 4000

LOGGER = logging.getLogger("generate_tts")


def configure_logging(verbose: bool = False) -> None:
    """Initialize process-wide logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def resolve_briefing_file(
    reports_dir: Path,
    run_date: str | None = None,
) -> Path:
    """Find the latest plain-text briefing file for today or a given date."""
    if run_date is not None:
        dated_path = reports_dir / f"daily_briefing_{run_date}.txt"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Briefing text file not found for date: {run_date}")

    txt_files = sorted(reports_dir.glob("daily_briefing_*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No briefing text files found in {reports_dir}")

    return txt_files[-1]


def chunk_text(text: str, max_chars: int = MAX_CHUNK_CHARS) -> list[str]:
    """Split text into chunks at sentence boundaries, each under max_chars."""
    # Split on sentence-ending punctuation followed by whitespace
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    chunks: list[str] = []
    current = ""

    for sentence in sentences:
        # A single sentence that exceeds the limit gets its own chunk
        if not current:
            current = sentence
        elif len(current) + 1 + len(sentence) <= max_chars:
            current += " " + sentence
        else:
            chunks.append(current)
            current = sentence

    if current:
        chunks.append(current)

    return chunks


def synthesize_chunk(
    client: Any,
    text: str,
    model: str,
    voice: str,
    audio_format: str,
    speed: float,
) -> bytes:
    """Call the OpenAI TTS API for one text chunk and return raw audio bytes."""
    response = client.audio.speech.create(
        model=model,
        voice=voice,
        input=text,
        response_format=audio_format,
        speed=speed,
    )
    return response.content


def generate_audio(
    text: str,
    api_key: str,
    model: str,
    voice: str,
    audio_format: str,
    speed: float,
) -> bytes:
    """Convert the full transcript to audio, chunking as needed."""
    try:
        import openai
    except ImportError as exc:
        raise ImportError(
            "The 'openai' package is required for TTS. Install it with: pip install openai"
        ) from exc

    client = openai.OpenAI(api_key=api_key)
    chunks = chunk_text(text)
    LOGGER.info("Synthesizing %d chunk(s) for %d characters of text", len(chunks), len(text))

    audio_parts: list[bytes] = []
    for i, chunk in enumerate(chunks, 1):
        LOGGER.debug("Synthesizing chunk %d/%d (%d chars)", i, len(chunks), len(chunk))
        audio_parts.append(
            synthesize_chunk(
                client=client,
                text=chunk,
                model=model,
                voice=voice,
                audio_format=audio_format,
                speed=speed,
            )
        )

    return b"".join(audio_parts)


def generate_tts(
    reports_dir: Path,
    audio_dir: Path,
    run_date: str | None = None,
) -> dict[str, Any]:
    """Read the briefing transcript, synthesize audio, and write the MP3."""
    tts_config = MODELS["text_to_speech"]

    api_key = str(tts_config.get("api_key") or "")
    if not api_key:
        raise ValueError(
            "OpenAI TTS API key is not set. Add OPENAI_TTS_API_KEY to your .env file."
        )

    model = str(tts_config.get("model") or "gpt-4o-mini-tts")
    voice = str(tts_config.get("voice") or "alloy")
    audio_format = str(tts_config.get("audio_format") or "mp3")
    speed = float(tts_config.get("speed") or 1.0)

    briefing_path = resolve_briefing_file(reports_dir, run_date)
    LOGGER.info("Reading briefing transcript from %s", briefing_path)
    text = briefing_path.read_text(encoding="utf-8").strip()

    if not text:
        raise ValueError(f"Briefing file is empty: {briefing_path}")

    audio_bytes = generate_audio(
        text=text,
        api_key=api_key,
        model=model,
        voice=voice,
        audio_format=audio_format,
        speed=speed,
    )

    # Derive the output filename from the briefing filename date
    date_suffix = briefing_path.stem.replace("daily_briefing_", "")
    audio_dir.mkdir(parents=True, exist_ok=True)
    audio_path = audio_dir / f"daily_briefing_{date_suffix}.{audio_format}"
    audio_path.write_bytes(audio_bytes)

    LOGGER.info(
        "Audio written to %s (%.1f KB)", audio_path, len(audio_bytes) / 1024
    )
    return {
        "briefing_file": str(briefing_path),
        "audio_file": str(audio_path),
        "audio_format": audio_format,
        "audio_size_bytes": len(audio_bytes),
        "model": model,
        "voice": voice,
    }


def main() -> dict[str, Any]:
    """Entry point for CLI and pipeline execution."""
    configure_logging()
    return generate_tts(
        reports_dir=DEFAULT_REPORTS_DIR,
        audio_dir=DEFAULT_AUDIO_DIR,
    )


if __name__ == "__main__":
    result = main()
    LOGGER.info("TTS result: %s", result)
