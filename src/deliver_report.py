"""Deliver the daily briefing transcript via email.

This stage reads the latest plain-text briefing from ``outputs/reports/`` and
sends it as an email body using SMTP credentials configured in ``config.py``.
The module uses only the Python standard library (``smtplib``, ``email.mime``).
"""

from __future__ import annotations

import argparse
import logging
import re
import smtplib
from datetime import UTC, datetime
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from config import DELIVERY

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"

LOGGER = logging.getLogger("deliver_report")


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
    """Find the latest Markdown briefing file for today or a given date."""
    if run_date is not None:
        dated_path = reports_dir / f"daily_briefing_{run_date}.md"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Briefing file not found for date: {run_date}")

    md_files = sorted(reports_dir.glob("daily_briefing_*.md"))
    if not md_files:
        raise FileNotFoundError(f"No briefing files found in {reports_dir}")

    return md_files[-1]


def markdown_to_html(md: str) -> str:
    """Convert Markdown briefing to a newsletter-style HTML email."""
    lines = md.strip().split("\n")
    body_parts: list[str] = []
    current_section_items: list[str] = []

    def flush_section_items() -> None:
        if current_section_items:
            body_parts.append('<table role="presentation" width="100%" cellpadding="0" cellspacing="0">')
            for item in current_section_items:
                body_parts.append(
                    '<tr><td style="padding: 12px 0; border-bottom: 1px solid #eee;">'
                    f"{item}</td></tr>"
                )
            body_parts.append("</table>")
            current_section_items.clear()

    for line in lines:
        stripped = line.strip()

        if not stripped:
            continue

        if stripped.startswith("## "):
            flush_section_items()
            heading = stripped[3:]
            body_parts.append(
                f'<h2 style="color: #1a1a2e; font-size: 16px; margin: 28px 0 12px 0;'
                f' padding-bottom: 8px; border-bottom: 2px solid #0f3460;">{heading}</h2>'
            )

        elif stripped.startswith("- "):
            item_md = stripped[2:]
            # Convert markdown links
            item_html = re.sub(
                r"\[([^\]]+)\]\(([^)]+)\)",
                r'<a href="\2" style="color: #0f3460; text-decoration: underline;">\1</a>',
                item_md,
            )
            # Style "Why it matters:" and "Source:"
            item_html = re.sub(
                r"Why it matters:",
                '<br><br><strong style="color: #e94560;">Why it matters?</strong>',
                item_html,
            )
            item_html = re.sub(
                r"Source:",
                '<br><span style="font-size: 11px; color: #666;">Source:',
                item_html,
            ) + "</span>"
            current_section_items.append(
                f'<p style="margin: 0; line-height: 1.6; font-size: 13px; color: #333;">{item_html}</p>'
            )

        else:
            flush_section_items()
            body_parts.append(
                f'<p style="margin: 12px 0; line-height: 1.6; font-size: 13px; color: #333;">{stripped}</p>'
            )

    flush_section_items()

    date_str = datetime.now(UTC).strftime("%B %d, %Y")
    inner_html = "\n".join(body_parts)

    return f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin: 0; padding: 0; background-color: #f4f4f8; font-family: Georgia, 'Times New Roman', serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color: #f4f4f8;">
<tr><td align="center" style="padding: 20px 10px;">
<table role="presentation" width="700" cellpadding="0" cellspacing="0"
       style="background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
  <!-- Header -->
  <tr><td style="background-color: #1a1a2e; padding: 28px 32px; text-align: center;">
    <h1 style="margin: 0; color: #ffffff; font-size: 20px; letter-spacing: 0.5px;">Daily News Briefing</h1>
    <p style="margin: 6px 0 0 0; color: #a0a0c0; font-size: 12px;">{date_str}</p>
  </td></tr>
  <!-- Body -->
  <tr><td style="padding: 24px 32px;">
    {inner_html}
  </td></tr>
  <!-- Footer -->
  <tr><td style="background-color: #f9f9fb; padding: 20px 32px; text-align: center; border-top: 1px solid #eee;">
    <p style="margin: 0; font-size: 11px; color: #999;">
      Automated Daily News Briefing &mdash; Cornell Tech &amp; NYC CM Virginia Maloney
    </p>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""


def send_email(
    subject: str,
    body: str,
    sender: str,
    recipients: list[str],
    smtp_host: str,
    smtp_port: int,
    username: str,
    password: str,
    use_tls: bool = True,
) -> None:
    """Send an HTML email via SMTP."""
    html_body = markdown_to_html(body)
    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    LOGGER.info("Connecting to %s:%s", smtp_host, smtp_port)
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        if use_tls:
            server.starttls()
        server.login(username, password)
        server.sendmail(sender, recipients, msg.as_string())
    LOGGER.info("Email sent to %s", ", ".join(recipients))


def deliver(reports_dir: Path, run_date: str | None = None) -> dict[str, Any]:
    """Read the briefing and deliver it via email if enabled."""
    email_config = DELIVERY["email"]

    if not email_config.get("enabled"):
        LOGGER.info("Email delivery is disabled; skipping")
        return {"status": "skipped", "reason": "email_delivery_disabled"}

    required_fields = ["smtp_host", "smtp_port", "username", "password", "sender"]
    missing = [f for f in required_fields if not email_config.get(f)]
    if missing:
        raise ValueError(f"Missing email configuration: {', '.join(missing)}")

    recipients = list(email_config.get("recipients") or [])
    if not recipients:
        raise ValueError("No email recipients configured (set EMAIL_RECIPIENTS)")

    briefing_path = resolve_briefing_file(reports_dir, run_date)
    LOGGER.info("Reading briefing from %s", briefing_path)
    body = briefing_path.read_text(encoding="utf-8")

    date_str = run_date or datetime.now(UTC).strftime("%Y-%m-%d")
    subject = f"Daily News Briefing \u2014 {date_str}"

    send_email(
        subject=subject,
        body=body,
        sender=str(email_config["sender"]),
        recipients=recipients,
        smtp_host=str(email_config["smtp_host"]),
        smtp_port=int(email_config["smtp_port"]),
        username=str(email_config["username"]),
        password=str(email_config["password"]),
        use_tls=bool(email_config.get("use_tls", True)),
    )

    return {
        "status": "completed",
        "briefing_file": str(briefing_path),
        "recipients": recipients,
        "subject": subject,
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=DEFAULT_REPORTS_DIR,
        help="Directory containing briefing report files",
    )
    parser.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Date for the briefing file in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> dict[str, Any]:
    """Entry point for CLI and pipeline execution."""
    args = parse_args()
    configure_logging(verbose=args.verbose)
    return deliver(reports_dir=args.reports_dir, run_date=args.run_date)


if __name__ == "__main__":
    result = main()
    LOGGER.info("Delivery result: %s", result)
