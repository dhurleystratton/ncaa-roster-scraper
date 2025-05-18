import pytest
import time
import urllib.error

from scraper.sports_reference import get_team_slugs


def safe_len(sport: str, yr: int) -> int:
    """Return len(get_team_slugs) with retries on HTTP 429."""
    delay = 1
    for attempt in range(6):
        try:
            return len(get_team_slugs(sport, yr))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if attempt == 5:
                    return 0
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return 0


def test_get_team_slugs_counts():
    assert safe_len("men", 2019) >= 300
    assert safe_len("women", 2019) >= 300
    assert safe_len("football", 2019) >= 120
