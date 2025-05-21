"""Instagram Handle Scraper
==========================

This script enriches a CSV of athletes with Instagram handles by
querying Google and validating resulting profiles.

Setup:
    pip install -r requirements.txt
    cp .env.example .env  # add API keys

Example usage:
    python ig_scraper.py --in roster.csv --out with_handles.csv --mode enrich
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, quote_plus, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover - optional
    def load_dotenv():
        pass
try:
    from loguru import logger  # type: ignore
except Exception:  # pragma: no cover - optional
    import logging

    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
try:
    from tenacity import retry, stop_after_attempt, wait_fixed  # type: ignore
except Exception:  # pragma: no cover - optional
    def retry(*dargs, **dkwargs):
        def wrapper(func):
            return func
        return wrapper

    def stop_after_attempt(_n):
        return None

    def wait_fixed(_n):
        return None

IG_PROFILE_RE = re.compile(r"https?://(?:www\.)?instagram\.com/([A-Za-z0-9._]+)/?", re.I)
META_PROFILE_TAG = '<meta property="og:type" content="profile"'

REQUEST_DELAY = 1  # seconds

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36 AthScraper/1.0"
    )
}


def load_env() -> None:
    """Load environment variables from .env if present."""
    load_dotenv()



def build_google_query(first: str, last: str, school: str) -> str:
    term = f'"{first} {last}" "{school}" site:instagram.com -site:instagram.com/p'
    return quote_plus(term, safe="")  # encode ALL chars


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def fetch_scrapingbee(url: str) -> requests.Response:
    key = os.getenv("SCRAPINGBEE_KEY")
    if not key:
        raise RuntimeError("SCRAPINGBEE_KEY missing")
    params = {
        "api_key": key,
        "url": url,
        "render_js": "true",
        "country_code": "us",
        "slow_mode": "1",
    }
    return requests.get(
        "https://app.scrapingbee.com/api/v1/",
        params=params,
        headers=HEADERS,
        timeout=30,
    )


def fetch_bright(url: str) -> requests.Response:
    key = os.getenv("BRIGHT_KEY")
    if not key:
        raise RuntimeError("BRIGHT_KEY missing for BrightData fallback")
    proxy = {
        "http": f"http://{key}@zproxy.lum-superproxy.io:22225",
        "https": f"http://{key}@zproxy.lum-superproxy.io:22225",
    }
    return requests.get(url, proxies=proxy, headers=HEADERS, timeout=30)


def fetch_html(url: str) -> Optional[requests.Response]:
    delay = 1
    if os.getenv("SCRAPINGBEE_KEY"):
        for _ in range(2):
            try:
                resp = fetch_scrapingbee(url)
                if resp.status_code == 429:
                    logger.warning("429 from ScrapingBee – pause %.0fs", delay)
                    time.sleep(delay)
                    delay *= 2
                    continue
                if resp.ok:
                    return resp
            except Exception as exc:
                logger.warning("ScrapingBee error: %s", exc)
            time.sleep(REQUEST_DELAY)
    # BrightData fallback even if ScrapingBee key missing
    try:
        resp = fetch_bright(url)
        if resp.ok:
            return resp
        logger.warning("BrightData returned status %s", resp.status_code)
    except Exception as exc:
        logger.error("BrightData request failed: %s", exc)
    return None


def parse_google_results(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a"):
        href = a.get("href", "")
        if href.startswith("/url?"):
            q = parse_qs(urlparse(href).query).get("q", [""])[0]
            if "instagram.com" in q:
                links.append(q)
        if len(links) >= 5:
            break
    return links


def validate_profile(url: str) -> tuple[Optional[str], str, str]:
    resp = fetch_html(url)
    if not resp or resp.status_code != 200:
        return None, "NOT_FOUND", ""
    html_low = resp.text.lower()
    if 'property="og:type"' not in html_low or 'content="profile"' not in html_low:
        return None, "NOT_FOUND", ""
    m = IG_PROFILE_RE.search(resp.url)
    if not m:
        return None, "NOT_FOUND", ""
    handle = m.group(1).lower()
    canonical = f"https://instagram.com/{handle}"
    status = "FOUND_PRIVATE" if "this account is private" in html_low else "FOUND_VALID"
    return handle, status, canonical


@dataclass
class Athlete:
    athlete_id: int
    first_name: str
    last_name: str
    school: str
    claim_score: float

    instagram_handle: str = ""
    profile_url: str = ""
    status: str = ""


class GracefulExit(Exception):
    pass


def sigint_handler(signum, frame):
    raise GracefulExit()


def processed_ids(out_file: Path) -> set[int]:
    if not out_file.exists():
        return set()
    df = pd.read_csv(out_file)
    return set(df["athlete_id"].astype(int))


def write_results(out_file: Path, rows: Iterable[Athlete]):
    header = not out_file.exists()
    with out_file.open("a", newline="") as f:
        writer = csv.writer(f)
        if header:
            writer.writerow([
                "athlete_id",
                "instagram_handle",
                "profile_url",
                "status",
            ])
        for r in rows:
            writer.writerow([r.athlete_id, r.instagram_handle, r.profile_url, r.status])


def upsert_postgres(rows: Iterable[Athlete], dsn: str):
    try:
        import psycopg2
    except Exception:
        logger.warning("psycopg2 not installed; skipping Postgres upsert")
        return

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nil_athletes (
            athlete_id INTEGER PRIMARY KEY,
            instagram_handle TEXT,
            profile_url TEXT,
            status TEXT
        )
        """
    )
    for r in rows:
        cur.execute(
            """
            INSERT INTO nil_athletes (athlete_id, instagram_handle, profile_url, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (athlete_id) DO UPDATE
                SET instagram_handle = EXCLUDED.instagram_handle,
                    profile_url = EXCLUDED.profile_url,
                    status = EXCLUDED.status
            """,
            (r.athlete_id, r.instagram_handle, r.profile_url, r.status),
        )
    conn.commit()
    cur.close()
    conn.close()


def process_row(row) -> Athlete:
    athlete = Athlete(
        athlete_id=int(row["athlete_id"]),
        first_name=str(row["first_name"]),
        last_name=str(row["last_name"]),
        school=str(row["school"]),
        claim_score=row.get("claim_score", 0),
    )
    if row.get("instagram_handle"):
        athlete.instagram_handle = str(row["instagram_handle"]).lower()
        athlete.profile_url = row.get("profile_url", "")
        athlete.status = row.get("status", "FOUND_VALID")
        return athlete

    query = build_google_query(athlete.first_name, athlete.last_name, athlete.school)
    search_url = f"https://www.google.com/search?q={query}&num=5"
    resp = fetch_html(search_url)
    if not resp or resp.status_code != 200:
        athlete.status = "SEARCH_FAIL"
        return athlete

    links = parse_google_results(resp.text)
    for link in links:
        handle, status, canonical = validate_profile(link)
        time.sleep(REQUEST_DELAY)
        if handle:
            athlete.instagram_handle = handle
            athlete.profile_url = canonical
            athlete.status = status
            break
    if not athlete.instagram_handle:
        athlete.status = "NOT_FOUND"
    return athlete


def main(args=None):
    parser = argparse.ArgumentParser(description="Instagram handle scraper")
    parser.add_argument("--in", dest="input", required=True, help="Input CSV")
    parser.add_argument("--out", dest="output", required=True, help="Output CSV")
    parser.add_argument("--mode", default="enrich", choices=["enrich"], help="Mode")
    parser.add_argument("--dsn", help="Postgres DSN for optional upsert")
    opts = parser.parse_args(args)

    load_env()

    input_path = Path(opts.input)
    output_path = Path(opts.output)

    df = pd.read_csv(input_path)
    done = processed_ids(output_path)

    signal.signal(signal.SIGINT, sigint_handler)

    try:
        for _, row in df.iterrows():
            if int(row["athlete_id"]) in done:
                continue
            athlete = process_row(row)
            write_results(output_path, [athlete])
            if opts.dsn:
                upsert_postgres([athlete], opts.dsn)
            time.sleep(REQUEST_DELAY)
    except GracefulExit:
        logger.info("Interrupted, exiting gracefully.")


if __name__ == "__main__":
    main()
