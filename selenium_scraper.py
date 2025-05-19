"""Selenium-based roster scraper for Sports-Reference.

This script downloads roster data for men's basketball, women's
basketball and FBS football teams from 2016-2021 using Selenium
WebDriver. It attempts to bypass rate limiting by allowing proxy
rotation and adds delays between requests. Results are appended to
``data/selenium_rosters.csv`` so the process can be resumed.

Team CSV files must be placed in the working directory with the
following names:
    358.csv  - men's basketball teams
    356.csv  - women's basketball teams
    130.csv  - football teams
The files should contain ``school_slug``, ``school_name`` and
``conference`` columns.
"""

from __future__ import annotations

import io
import time
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException


TEAM_FILES = {
    "men": Path("358.csv"),
    "women": Path("356.csv"),
    "football": Path("130.csv"),
}


def build_driver(proxy: Optional[str] = None, *, headless: bool = True) -> webdriver.Chrome:
    """Create and return a Chrome WebDriver instance."""

    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")
    return webdriver.Chrome(options=options)


def parse_roster(csv_text: str) -> pd.DataFrame:
    """Parse CSV text returned by Sports-Reference."""

    return pd.read_csv(io.StringIO(csv_text))


def fetch_roster(driver: webdriver.Chrome, sport: str, slug: str, year: int) -> pd.DataFrame:
    """Download roster CSV for ``slug`` and return a DataFrame."""

    if sport == "men":
        url = f"https://www.sports-reference.com/cbb/schools/{slug}/{year}.html?output=csv"
    elif sport == "women":
        url = f"https://www.sports-reference.com/cbb/schools/{slug}/{year}-women.html?output=csv"
    else:
        url = f"https://www.sports-reference.com/cfb/schools/{slug}/{year}-roster.html?output=csv"

    driver.get(url)
    if "Too Many Requests" in driver.page_source:
        raise RuntimeError("429")
    pre = driver.find_element(By.TAG_NAME, "pre")
    return parse_roster(pre.text)


def append_frames(frames: list[pd.DataFrame], output: Path) -> None:
    """Append list of frames to ``output`` CSV."""

    df = pd.concat(frames, ignore_index=True)
    header = not output.exists()
    df.to_csv(output, mode="a", index=False, header=header)


def main(
    seasons: Iterable[int] = range(2016, 2022),
    delay: float = 3.0,
    proxies: Optional[list[str]] = None,
) -> None:
    """Scrape rosters using Selenium."""

    if proxies is None:
        proxies = [None]
    proxy_idx = 0

    output = Path("data/selenium_rosters.csv")
    output.parent.mkdir(parents=True, exist_ok=True)

    if output.exists():
        done = pd.read_csv(output, usecols=["sport", "season", "school_slug"])
        completed = {
            (r.sport, int(r.season), r.school_slug) for r in done.itertuples(index=False)
        }
    else:
        completed = set()

    driver = build_driver(proxies[proxy_idx])
    frames: list[pd.DataFrame] = []

    try:
        for sport, csv_file in TEAM_FILES.items():
            teams = pd.read_csv(csv_file)
            for year in seasons:
                for slug, school, conf in teams.itertuples(index=False):
                    key = (sport, year, slug)
                    if key in completed:
                        continue
                    tries = 0
                    while tries < 3:
                        try:
                            df = fetch_roster(driver, sport, slug, year)
                            df["season"] = year
                            df["sport"] = sport
                            df["school_slug"] = slug
                            df["school_name"] = school
                            df["conference"] = conf
                            frames.append(df)
                            if len(frames) >= 20:
                                append_frames(frames, output)
                                frames = []
                            break
                        except RuntimeError:
                            tries += 1
                            driver.quit()
                            proxy_idx = (proxy_idx + 1) % len(proxies)
                            driver = build_driver(proxies[proxy_idx])
                            time.sleep(delay * tries)
                        except WebDriverException:
                            tries += 1
                            driver.quit()
                            driver = build_driver(proxies[proxy_idx])
                            time.sleep(delay * tries)
                    time.sleep(delay)
    finally:
        if frames:
            append_frames(frames, output)
        driver.quit()


if __name__ == "__main__":
    main()
