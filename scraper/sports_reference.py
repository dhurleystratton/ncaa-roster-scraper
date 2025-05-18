"""Sports Reference scraping utilities.

This module provides helper functions for scraping roster data from
Sports-Reference.  At the moment it exposes :func:`fetch_rosters` which
downloads raw roster information for a handful of sports and seasons and
consolidates everything into ``data/master_raw.csv``.
"""

from __future__ import annotations

import io
import re
import time
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests import Response
from tqdm import tqdm


def _request_with_retries(
    session: requests.Session, url: str, retries: int = 3, sleep: float = 1.0
) -> Response:
    """Fetch ``url`` with retries and a delay between attempts."""

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # requests.RequestException covers most cases
            if attempt >= retries:
                raise exc
            time.sleep(sleep)


def _season_str(year: int) -> str:
    """Return a season string like ``'2016-17'`` for ``year=2017``."""

    return f"{year - 1}-{str(year)[-2:]}"


def _parse_slugs(html: str, pattern: re.Pattern[str]) -> List[str]:
    """Extract team slugs from ``html`` matching ``pattern``."""

    soup = BeautifulSoup(html, "html.parser")
    slugs = set()
    for a in soup.find_all("a", href=pattern):
        match = pattern.search(a["href"])
        if match:
            slugs.add(match.group(1))
    return sorted(slugs)


def _slugs_for_sport(session: requests.Session, year: int, sport: str) -> List[str]:
    """Fetch the list of team slugs for ``sport`` in ``year``."""

    if sport == "men":
        url = (
            f"https://www.sports-reference.com/cbb/seasons/{year}-school-stats.html"
        )
        pattern = re.compile(rf"/cbb/schools/([^/]+)/{year}\.html")
    elif sport == "women":
        url = (
            f"https://www.sports-reference.com/cbb/seasons/{year}-women-school-stats.html"
        )
        pattern = re.compile(rf"/cbb/schools/([^/]+)/{year}-women\.html")
    elif sport == "football":
        url = f"https://www.sports-reference.com/cfb/years/{year}.html"
        pattern = re.compile(rf"/cfb/schools/([^/]+)/{year}\.html")
    else:
        raise ValueError(f"Unknown sport: {sport}")

    resp = _request_with_retries(session, url)
    return _parse_slugs(resp.text, pattern)


def get_team_slugs(sport: str, year: int) -> list[str]:
    """Return team slugs for ``sport`` in ``year`` using index pages."""

    if sport == "football":
        url = f"https://www.sports-reference.com/cfb/years/{year}-team.html"
    elif sport == "men":
        url = f"https://www.sports-reference.com/cbb/seasons/{year}-school-stats.html"
    elif sport == "women":
        url = f"https://www.sports-reference.com/cbb/seasons/{year}-women-school-stats.html"
    else:
        raise ValueError("sport must be 'men', 'women', or 'football'")

    tables = pd.read_html(url, attrs={"id": "schools"})
    if not tables:
        return []
    df = tables[0]
    links = (
        df.iloc[:, 0]
        .astype(str)
        .str.extract(r'href="/[a-z]+/schools/([^/]+)/')[0]
        .dropna()
        .unique()
    )
    return links.tolist()


def _fetch_roster_csv(
    session: requests.Session, year: int, sport: str, slug: str
) -> Optional[pd.DataFrame]:
    """Retrieve roster CSV for ``slug`` and return a DataFrame."""

    if sport == "men":
        url = (
            f"https://www.sports-reference.com/cbb/schools/{slug}/{year}.html?output=csv"
        )
    elif sport == "women":
        url = (
            f"https://www.sports-reference.com/cbb/schools/{slug}/{year}-women.html?output=csv"
        )
    else:  # football
        url = (
            f"https://www.sports-reference.com/cfb/schools/{slug}/{year}-roster.html?output=csv"
        )

    try:
        resp = _request_with_retries(session, url)
    except Exception:
        return None

    try:
        df = pd.read_csv(io.StringIO(resp.text))
    except Exception:
        return None

    expected_cols = {"player": None, "pos": None, "class": None}
    for col in df.columns:
        lower = col.strip().lower()
        if lower in ("player", "name"):
            expected_cols["player"] = col
        elif lower in ("pos", "position"):
            expected_cols["pos"] = col
        elif lower in ("class", "yr", "year"):
            expected_cols["class"] = col

    if None in expected_cols.values():
        return None

    return df[[expected_cols["player"], expected_cols["pos"], expected_cols["class"]]].rename(
        columns={
            expected_cols["player"]: "player",
            expected_cols["pos"]: "position",
            expected_cols["class"]: "class",
        }
    )


def fetch_rosters(seasons: Optional[Iterable[int]] = None) -> None:
    """Scrape rosters and save them to ``data/master_raw.csv``.

    Parameters
    ----------
    seasons : Iterable[int], optional
        Specific ending years of seasons to scrape. If ``None`` (default),
        seasons 2016-17 through 2020-21 are scraped.
    """

    if seasons is None:
        seasons = list(range(2017, 2022))
    sports = ["men", "women", "football"]

    session = requests.Session()

    frames = []
    for year in seasons:
        season_name = _season_str(year)
        for sport in sports:
            try:
                slugs = _slugs_for_sport(session, year, sport)
            except Exception:
                continue

            for slug in tqdm(slugs, desc=f"{sport}-{year}", unit="team"):
                df = _fetch_roster_csv(session, year, sport, slug)
                time.sleep(1)  # politeness
                if df is None or df.empty:
                    continue

                df.insert(0, "school", slug)
                df.insert(0, "sport", sport)
                df.insert(0, "season", season_name)
                frames.append(df)
    if not frames:
        print("No data scraped – writing empty CSV to avoid downstream errors")
        out = Path("data/master_raw.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            columns=["season", "sport", "school", "player", "position", "class"]
        ).to_csv(out, index=False)
        return

    master = pd.concat(frames, ignore_index=True)

    output = Path("data/master_raw.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(output, index=False)

