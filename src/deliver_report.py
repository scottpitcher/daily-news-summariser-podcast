"""Deliver the daily briefing transcript via email.

This stage reads the latest plain-text briefing from ``outputs/reports/`` and
sends it as an email body using SMTP credentials configured in ``config.py``.
The module uses only the Python standard library (``smtplib``, ``email.mime``).
"""

from __future__ import annotations

import argparse
import logging
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
    """Find the latest plain-text briefing file for today or a given date."""
    if run_date is not None:
        dated_path = reports_dir / f"daily_briefing_{run_date}.txt"
        if dated_path.exists():
            return dated_path
        raise FileNotFoundError(f"Briefing file not found for date: {run_date}")

    txt_files = sorted(reports_dir.glob("daily_briefing_*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No briefing files found in {reports_dir}")

    return txt_files[-1]


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
    """Send a plain-text email via SMTP."""
    msg = MIMEText(body, "plain", "utf-8")
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
