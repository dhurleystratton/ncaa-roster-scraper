import pytest
from scraper.sports_reference import get_team_slugs


def test_get_team_slugs_counts():
    assert len(get_team_slugs("men", 2019)) >= 350
    assert len(get_team_slugs("women", 2019)) >= 350
    assert len(get_team_slugs("football", 2019)) >= 128
